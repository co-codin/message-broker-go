package main

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type segment struct {
	base int64
	path string
}

// Record on disk:
//
//	[u32 body_len]
//	[u64 offset]
//	[u16 key_len][key bytes]
//	[u32 payload_len][payload bytes]
//	[u32 crc32]       // IEEE over (offset || key_len || key || payload_len || payload)
//
// body_len = 8 + 2 + key_len + 4 + payload_len + 4
//
// Storing the offset in each record lets compaction remove records while
// preserving the monotonic offset space (compacted segments have gaps).
type Partition struct {
	dir       string
	maxPerSeg int
	retain    int

	mu          sync.Mutex
	segments    []segment
	activeFile  *os.File
	activeCount int
	nextOffset  int64
	notify      chan struct{}
}

func openPartition(dir string, maxPerSeg, retain int) (*Partition, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	p := &Partition{
		dir:       dir,
		maxPerSeg: maxPerSeg,
		retain:    retain,
		notify:    make(chan struct{}),
	}
	if err := p.loadSegments(); err != nil {
		return nil, err
	}
	return p, nil
}

func (p *Partition) loadSegments() error {
	entries, err := os.ReadDir(p.dir)
	if err != nil {
		return err
	}
	var segs []segment
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".log") {
			continue
		}
		base, err := strconv.ParseInt(strings.TrimSuffix(e.Name(), ".log"), 10, 64)
		if err != nil {
			continue
		}
		segs = append(segs, segment{base: base, path: filepath.Join(p.dir, e.Name())})
	}
	sort.Slice(segs, func(i, j int) bool { return segs[i].base < segs[j].base })

	if len(segs) == 0 {
		base := int64(0)
		path := segPath(p.dir, base)
		f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
		if err != nil {
			return err
		}
		p.segments = []segment{{base: base, path: path}}
		p.activeFile = f
		return nil
	}

	last := segs[len(segs)-1]
	f, err := os.OpenFile(last.path, os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	// Scan records to find the max offset (the next to assign is max+1).
	// Compaction may have introduced gaps, so we can't just count.
	count := 0
	var maxOff int64 = last.base - 1
	for {
		off, _, _, err := readRecord(f)
		if err != nil {
			if err == io.EOF {
				break
			}
			f.Close()
			return err
		}
		count++
		if off > maxOff {
			maxOff = off
		}
	}
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		f.Close()
		return err
	}
	p.segments = segs
	p.activeFile = f
	p.activeCount = count
	p.nextOffset = maxOff + 1
	return nil
}

func segPath(dir string, base int64) string {
	return filepath.Join(dir, fmt.Sprintf("%020d.log", base))
}

// Append writes a (key, payload) record and returns its offset. Either may
// be nil; the CRC still covers both.
func (p *Partition) Append(key, payload []byte) (int64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	offset := p.nextOffset
	if err := writeRecord(p.activeFile, offset, key, payload); err != nil {
		return 0, err
	}
	if err := p.activeFile.Sync(); err != nil {
		return 0, err
	}
	p.nextOffset++
	p.activeCount++

	if p.activeCount >= p.maxPerSeg {
		if err := p.activeFile.Close(); err != nil {
			return 0, err
		}
		newBase := p.nextOffset
		newPath := segPath(p.dir, newBase)
		f, err := os.OpenFile(newPath, os.O_CREATE|os.O_RDWR, 0o644)
		if err != nil {
			return 0, err
		}
		p.activeFile = f
		p.activeCount = 0
		p.segments = append(p.segments, segment{base: newBase, path: newPath})
		for len(p.segments) > p.retain {
			old := p.segments[0]
			p.segments = p.segments[1:]
			_ = os.Remove(old.path)
		}
		segmentRolls.Inc()
	}

	close(p.notify)
	p.notify = make(chan struct{})
	return offset, nil
}

func (p *Partition) Head() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.nextOffset
}

// Compact rewrites sealed segments keeping only the latest record per key
// (keyless records are always kept). The active segment is not touched, but
// records in it count toward "latest per key" so a sealed record superseded
// by an active-segment record is still removed.
//
// Returns the number of records dropped.
func (p *Partition) Compact() (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if len(p.segments) <= 1 {
		return 0, nil // nothing sealed to compact
	}

	sealed := p.segments[:len(p.segments)-1]
	activeSeg := p.segments[len(p.segments)-1]

	// Pass 1: build {key -> highest offset} across ALL segments (including active).
	latest := map[string]int64{}
	scan := func(path string) error {
		f, err := os.Open(path)
		if err != nil {
			return err
		}
		defer f.Close()
		for {
			off, key, _, err := readRecord(f)
			if err == io.EOF {
				return nil
			}
			if err != nil {
				return err
			}
			if len(key) == 0 {
				continue
			}
			if cur, ok := latest[string(key)]; !ok || off > cur {
				latest[string(key)] = off
			}
		}
	}
	for _, seg := range sealed {
		if err := scan(seg.path); err != nil {
			return 0, err
		}
	}
	if err := scan(activeSeg.path); err != nil {
		return 0, err
	}

	// Pass 2: collect kept records from sealed segments in offset order.
	type record struct {
		offset       int64
		key, payload []byte
	}
	var kept []record
	dropped := 0
	for _, seg := range sealed {
		f, err := os.Open(seg.path)
		if err != nil {
			return 0, err
		}
		for {
			off, key, payload, err := readRecord(f)
			if err == io.EOF {
				break
			}
			if err != nil {
				f.Close()
				return 0, err
			}
			if len(key) == 0 {
				kept = append(kept, record{off, key, payload})
				continue
			}
			if latest[string(key)] == off {
				kept = append(kept, record{off, key, payload})
			} else {
				dropped++
			}
		}
		f.Close()
	}

	// Write kept records to a new segment named after the oldest sealed segment.
	// Write to a temp file then atomically rename over the original.
	newBase := sealed[0].base
	newPath := segPath(p.dir, newBase)
	tmpPath := newPath + ".compacting"
	nf, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		return 0, err
	}
	for _, r := range kept {
		if err := writeRecord(nf, r.offset, r.key, r.payload); err != nil {
			nf.Close()
			os.Remove(tmpPath)
			return 0, err
		}
	}
	if err := nf.Sync(); err != nil {
		nf.Close()
		os.Remove(tmpPath)
		return 0, err
	}
	nf.Close()

	// Atomically swap in the compacted segment, delete the rest.
	if err := os.Rename(tmpPath, newPath); err != nil {
		return 0, err
	}
	for _, seg := range sealed[1:] {
		if err := os.Remove(seg.path); err != nil {
			return 0, err
		}
	}
	// Rebuild the in-memory segment list: one compacted + active.
	p.segments = []segment{{base: newBase, path: newPath}, activeSeg}

	if dropped > 0 {
		compactionDropped.Add(float64(dropped))
	}
	return dropped, nil
}

// SweepOlderThan removes sealed segments whose file mtime is older than the
// given duration. The active segment is always kept. Returns the number of
// segments deleted. (Background time-based retention.)
func (p *Partition) SweepOlderThan(d time.Duration) (int, error) {
	now := time.Now()
	p.mu.Lock()
	defer p.mu.Unlock()

	removed := 0
	for len(p.segments) > 1 { // never touch the active segment
		oldest := p.segments[0]
		info, err := os.Stat(oldest.path)
		if err != nil {
			return removed, err
		}
		if now.Sub(info.ModTime()) < d {
			break
		}
		if err := os.Remove(oldest.path); err != nil {
			return removed, err
		}
		p.segments = p.segments[1:]
		removed++
	}
	return removed, nil
}

// Iterate streams (key, payload) records from `from` onward. Blocks at the
// active-segment tail until ctx is cancelled or fn returns false. CRC is
// verified on every record — a mismatch ends the iteration with an error.
func (p *Partition) Iterate(ctx context.Context, from int64, fn func(offset int64, key, payload []byte) bool) error {
	cursor := from

	var current *os.File
	var currentBase int64 = -1
	defer func() {
		if current != nil {
			current.Close()
		}
	}()

	for {
		if ctx.Err() != nil {
			return nil
		}

		p.mu.Lock()
		segsCopy := append([]segment(nil), p.segments...)
		nextOff := p.nextOffset
		notify := p.notify
		p.mu.Unlock()

		if len(segsCopy) == 0 {
			select {
			case <-notify:
				continue
			case <-ctx.Done():
				return nil
			}
		}
		if cursor < segsCopy[0].base {
			return fmt.Errorf("offset %d out of range (oldest=%d)", cursor, segsCopy[0].base)
		}
		if cursor >= nextOff {
			select {
			case <-notify:
				continue
			case <-ctx.Done():
				return nil
			}
		}

		segIdx := sort.Search(len(segsCopy), func(i int) bool { return segsCopy[i].base > cursor }) - 1
		if segIdx < 0 {
			segIdx = 0
		}
		seg := segsCopy[segIdx]
		isActive := segIdx == len(segsCopy)-1

		if currentBase != seg.base {
			if current != nil {
				current.Close()
				current = nil
			}
			f, err := os.Open(seg.path)
			if err != nil {
				return err
			}
			current = f
			currentBase = seg.base
		}

		// Walk records; deliver anything with offset >= cursor. Compaction
		// may have produced gaps, so offsets within a segment aren't
		// necessarily contiguous.
		advanced := false
		for {
			if ctx.Err() != nil {
				return nil
			}
			off, key, payload, err := readRecord(current)
			if err == io.EOF {
				break
			}
			if err != nil {
				return err
			}
			if off < cursor {
				continue
			}
			if !fn(off, key, payload) {
				return nil
			}
			cursor = off + 1
			advanced = true
		}
		// If we saw no records at or beyond the cursor in this segment and
		// it's sealed, move to the next by jumping cursor to its successor.
		if !advanced && segIdx+1 < len(segsCopy) {
			cursor = segsCopy[segIdx+1].base
		}

		if isActive {
			select {
			case <-notify:
			case <-ctx.Done():
				return nil
			}
		}
	}
}

// ---- record format helpers -------------------------------------------------

func writeRecord(w io.Writer, offset int64, key, payload []byte) error {
	if len(key) > 65535 {
		return errors.New("record key too large (>65535 bytes)")
	}
	bodyLen := 8 + 2 + len(key) + 4 + len(payload) + 4
	buf := make([]byte, 4+bodyLen)
	binary.BigEndian.PutUint32(buf[0:4], uint32(bodyLen))
	binary.BigEndian.PutUint64(buf[4:12], uint64(offset))
	binary.BigEndian.PutUint16(buf[12:14], uint16(len(key)))
	copy(buf[14:14+len(key)], key)
	off := 14 + len(key)
	binary.BigEndian.PutUint32(buf[off:off+4], uint32(len(payload)))
	off += 4
	copy(buf[off:off+len(payload)], payload)
	off += len(payload)
	// CRC covers the body up to (but not including) the CRC field itself.
	crc := crc32.ChecksumIEEE(buf[4:off])
	binary.BigEndian.PutUint32(buf[off:off+4], crc)
	_, err := w.Write(buf)
	return err
}

func readRecord(r io.Reader) (int64, []byte, []byte, error) {
	var bodyLen uint32
	if err := binary.Read(r, binary.BigEndian, &bodyLen); err != nil {
		return 0, nil, nil, err
	}
	body := make([]byte, bodyLen)
	if _, err := io.ReadFull(r, body); err != nil {
		return 0, nil, nil, err
	}
	// Minimum body: 8 (offset) + 2 (key_len) + 4 (payload_len) + 4 (crc) = 18
	if len(body) < 18 {
		return 0, nil, nil, errors.New("record too short")
	}
	offset := int64(binary.BigEndian.Uint64(body[0:8]))
	keyLen := int(binary.BigEndian.Uint16(body[8:10]))
	if 8+2+keyLen+4+4 > len(body) {
		return 0, nil, nil, errors.New("record truncated")
	}
	keyStart := 10
	payloadLenStart := keyStart + keyLen
	payloadLen := int(binary.BigEndian.Uint32(body[payloadLenStart : payloadLenStart+4]))
	payloadStart := payloadLenStart + 4
	if payloadStart+payloadLen+4 > len(body) {
		return 0, nil, nil, errors.New("record truncated")
	}
	key := append([]byte(nil), body[keyStart:keyStart+keyLen]...)
	payload := append([]byte(nil), body[payloadStart:payloadStart+payloadLen]...)
	crcStart := payloadStart + payloadLen
	storedCRC := binary.BigEndian.Uint32(body[crcStart : crcStart+4])
	computedCRC := crc32.ChecksumIEEE(body[:crcStart])
	if storedCRC != computedCRC {
		return 0, nil, nil, fmt.Errorf("record CRC mismatch: stored=%08x computed=%08x", storedCRC, computedCRC)
	}
	return offset, key, payload, nil
}

func skipRecord(r io.ReadSeeker) error {
	var bodyLen uint32
	if err := binary.Read(r, binary.BigEndian, &bodyLen); err != nil {
		return err
	}
	_, err := r.Seek(int64(bodyLen), io.SeekCurrent)
	return err
}
