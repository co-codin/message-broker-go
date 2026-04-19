package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestPartitionAppendAssignsSequentialOffsets(t *testing.T) {
	p, err := openPartition(t.TempDir(), 1000, 100)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		off, err := p.Append(nil, fmt.Appendf(nil, "msg-%d", i))
		if err != nil {
			t.Fatal(err)
		}
		if off != int64(i) {
			t.Fatalf("offset[%d]=%d, want %d", i, off, i)
		}
	}
	if p.Head() != 5 {
		t.Fatalf("Head()=%d, want 5", p.Head())
	}
}

func TestPartitionIterateReadsAll(t *testing.T) {
	p, err := openPartition(t.TempDir(), 1000, 100)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		if _, err := p.Append(nil, fmt.Appendf(nil, "msg-%d", i)); err != nil {
			t.Fatal(err)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var (
		mu  sync.Mutex
		got []string
	)
	done := make(chan struct{})
	go func() {
		_ = p.Iterate(ctx, 0, func(offset int64, _, payload []byte) bool {
			mu.Lock()
			got = append(got, string(payload))
			n := len(got)
			mu.Unlock()
			if n == 5 {
				cancel()
				return false
			}
			return true
		})
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("iterate timed out")
	}

	mu.Lock()
	defer mu.Unlock()
	if len(got) != 5 {
		t.Fatalf("got %d records, want 5", len(got))
	}
	for i, s := range got {
		if want := fmt.Sprintf("msg-%d", i); s != want {
			t.Errorf("got[%d]=%q, want %q", i, s, want)
		}
	}
}

func TestPartitionReplayAfterReopen(t *testing.T) {
	dir := t.TempDir()
	p, err := openPartition(dir, 1000, 100)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"alpha", "beta", "gamma"}
	for _, s := range want {
		if _, err := p.Append(nil, []byte(s)); err != nil {
			t.Fatal(err)
		}
	}

	// Reopen with a fresh Partition instance; should rediscover the 3 records.
	p2, err := openPartition(dir, 1000, 100)
	if err != nil {
		t.Fatal(err)
	}
	if p2.Head() != int64(len(want)) {
		t.Fatalf("after reopen: Head=%d, want %d", p2.Head(), len(want))
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var got []string
	done := make(chan struct{})
	go func() {
		_ = p2.Iterate(ctx, 0, func(_ int64, _, payload []byte) bool {
			got = append(got, string(payload))
			if len(got) == len(want) {
				cancel()
				return false
			}
			return true
		})
		close(done)
	}()
	<-done
	for i, s := range got {
		if s != want[i] {
			t.Errorf("got[%d]=%q, want %q", i, s, want[i])
		}
	}
}

func TestPartitionSegmentRollingAndRetention(t *testing.T) {
	dir := t.TempDir()
	// Roll every 3 records, keep only 2 segments on disk.
	p, err := openPartition(dir, 3, 2)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		if _, err := p.Append(nil, fmt.Appendf(nil, "m%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	// Trace: segments are created at bases 0, 3, 6, 9. With retain=2 and
	// retention applied on roll, the oldest gets deleted each time. Final
	// on-disk state should be segment files at bases 6 and 9.
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var bases []int
	for _, e := range entries {
		name := e.Name()
		if !strings.HasSuffix(name, ".log") {
			continue
		}
		n, err := strconv.Atoi(strings.TrimSuffix(name, ".log"))
		if err != nil {
			t.Fatalf("unexpected segment filename %q: %v", name, err)
		}
		bases = append(bases, n)
	}
	sort.Ints(bases)
	if len(bases) != 2 || bases[0] != 6 || bases[1] != 9 {
		t.Fatalf("segments on disk = %v, want [6 9]", bases)
	}
}

func TestPartitionCRCRejectsCorruption(t *testing.T) {
	dir := t.TempDir()
	p, err := openPartition(dir, 1000, 100)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.Append([]byte("k"), []byte("original value")); err != nil {
		t.Fatal(err)
	}

	// Corrupt one byte in the payload region of the segment file.
	segFile := filepath.Join(dir, "00000000000000000000.log")
	data, err := os.ReadFile(segFile)
	if err != nil {
		t.Fatal(err)
	}
	// Layout: [u32 body_len][u16 key_len][k][u32 payload_len][payload][u32 crc]
	// Flip a bit in the payload — guaranteed to change the CRC check.
	data[len(data)-6] ^= 0xff
	if err := os.WriteFile(segFile, data, 0o644); err != nil {
		t.Fatal(err)
	}

	// Reopen and iterate — the CRC check should fire.
	p2, err := openPartition(dir, 1000, 100)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err = p2.Iterate(ctx, 0, func(int64, []byte, []byte) bool { return true })
	if err == nil || !strings.Contains(err.Error(), "CRC mismatch") {
		t.Fatalf("Iterate after corruption: got %v, want CRC mismatch", err)
	}
}

func TestPartitionIterateOutOfRangeReturnsError(t *testing.T) {
	dir := t.TempDir()
	p, err := openPartition(dir, 3, 2)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		if _, err := p.Append(nil, fmt.Appendf(nil, "m%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	// After retention (same trace as above), oldest offset is 6. Trying to
	// iterate from 0 should return an "out of range" error.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err = p.Iterate(ctx, 0, func(int64, []byte, []byte) bool { return true })
	if err == nil || !strings.Contains(err.Error(), "out of range") {
		t.Fatalf("Iterate from 0: got %v, want out-of-range", err)
	}
}
