package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

type segment struct {
	base int64
	path string
}

// Partition owns the segment files for one partition of one topic.
// Payloads are arbitrary bytes (no newline/space restrictions).
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
		var length uint32
		if err := binary.Read(f, binary.BigEndian, &length); err != nil {
			if err == io.EOF {
				break
			}
			f.Close()
			return err
		}
		if _, err := f.Seek(int64(length), io.SeekCurrent); err != nil {
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

// Append writes a record and returns its offset.
func (p *Partition) Append(payload []byte) (int64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if err := binary.Write(p.activeFile, binary.BigEndian, uint32(len(payload))); err != nil {
		return 0, err
	}
	if _, err := p.activeFile.Write(payload); err != nil {
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

// Iterate streams records from `from` onward through fn. Blocks at the
// active-segment tail until ctx is cancelled or fn returns false.
func (p *Partition) Iterate(ctx context.Context, from int64, fn func(offset int64, payload []byte) bool) error {
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
				var length uint32
				if err := binary.Read(f, binary.BigEndian, &length); err != nil {
					f.Close()
					return err
				}
				if _, err := f.Seek(int64(length), io.SeekCurrent); err != nil {
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
			var length uint32
			err := binary.Read(current, binary.BigEndian, &length)
			if err == io.EOF {
				break
			}
			if err != nil {
				return err
			}
			buf := make([]byte, length)
			if _, err := io.ReadFull(current, buf); err != nil {
				return err
			}
			if !fn(cursor, buf) {
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
