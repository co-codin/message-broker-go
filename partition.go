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
//	[u16 key_len][key bytes]
//	[u32 payload_len][payload bytes]
//	[u32 crc32]       // IEEE over (key_len || key || payload_len || payload)
//
// body_len = 2 + key_len + 4 + payload_len + 4
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
	count := 0
	for {
		if err := skipRecord(f); err != nil {
			if err == io.EOF {
				break
			}
			f.Close()
			return err
		}
		count++
	}
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		f.Close()
		return err
	}
	p.segments = segs
	p.activeFile = f
	p.activeCount = count
	p.nextOffset = last.base + int64(count)
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

	if err := writeRecord(p.activeFile, key, payload); err != nil {
		return 0, err
	}
	if err := p.activeFile.Sync(); err != nil {
		return 0, err
	}

	offset := p.nextOffset
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
			skipN := int(cursor - seg.base)
			for i := 0; i < skipN; i++ {
				if err := skipRecord(f); err != nil {
					f.Close()
					return err
				}
			}
			current = f
			currentBase = seg.base
		}

		for {
			if ctx.Err() != nil {
				return nil
			}
			key, payload, err := readRecord(current)
			if err == io.EOF {
				break
			}
			if err != nil {
				return err
			}
			if !fn(cursor, key, payload) {
				return nil
			}
			cursor++
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

func writeRecord(w io.Writer, key, payload []byte) error {
	if len(key) > 65535 {
		return errors.New("record key too large (>65535 bytes)")
	}
	bodyLen := 2 + len(key) + 4 + len(payload) + 4
	buf := make([]byte, 4+bodyLen)
	binary.BigEndian.PutUint32(buf[0:4], uint32(bodyLen))
	binary.BigEndian.PutUint16(buf[4:6], uint16(len(key)))
	copy(buf[6:6+len(key)], key)
	off := 6 + len(key)
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

func readRecord(r io.Reader) ([]byte, []byte, error) {
	var bodyLen uint32
	if err := binary.Read(r, binary.BigEndian, &bodyLen); err != nil {
		return nil, nil, err
	}
	body := make([]byte, bodyLen)
	if _, err := io.ReadFull(r, body); err != nil {
		return nil, nil, err
	}
	// Minimum body: 2 (key_len) + 0 (key) + 4 (payload_len) + 0 (payload) + 4 (crc) = 10
	if len(body) < 10 {
		return nil, nil, errors.New("record too short")
	}
	keyLen := int(binary.BigEndian.Uint16(body[0:2]))
	if 2+keyLen+4+4 > len(body) {
		return nil, nil, errors.New("record truncated")
	}
	keyStart := 2
	payloadLenStart := keyStart + keyLen
	payloadLen := int(binary.BigEndian.Uint32(body[payloadLenStart : payloadLenStart+4]))
	payloadStart := payloadLenStart + 4
	if payloadStart+payloadLen+4 > len(body) {
		return nil, nil, errors.New("record truncated")
	}
	key := append([]byte(nil), body[keyStart:keyStart+keyLen]...)
	payload := append([]byte(nil), body[payloadStart:payloadStart+payloadLen]...)
	crcStart := payloadStart + payloadLen
	storedCRC := binary.BigEndian.Uint32(body[crcStart : crcStart+4])
	computedCRC := crc32.ChecksumIEEE(body[:crcStart])
	if storedCRC != computedCRC {
		return nil, nil, fmt.Errorf("record CRC mismatch: stored=%08x computed=%08x", storedCRC, computedCRC)
	}
	return key, payload, nil
}

func skipRecord(r io.ReadSeeker) error {
	var bodyLen uint32
	if err := binary.Read(r, binary.BigEndian, &bodyLen); err != nil {
		return err
	}
	_, err := r.Seek(int64(bodyLen), io.SeekCurrent)
	return err
}
