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

func TestPartitionSweepOlderThan(t *testing.T) {
	dir := t.TempDir()
	// 3 records per segment; no count-based retention pressure.
	p, err := openPartition(dir, 3, 1000)
	if err != nil {
		t.Fatal(err)
	}
	// 10 records -> segments at bases 0, 3, 6, 9.
	for i := 0; i < 10; i++ {
		if _, err := p.Append(nil, fmt.Appendf(nil, "m%d", i)); err != nil {
			t.Fatal(err)
		}
	}

	// Backdate the three sealed segments so they look old on disk.
	old := time.Now().Add(-10 * time.Minute)
	for _, base := range []int64{0, 3, 6} {
		path := filepath.Join(dir, fmt.Sprintf("%020d.log", base))
		if err := os.Chtimes(path, old, old); err != nil {
			t.Fatal(err)
		}
	}

	removed, err := p.SweepOlderThan(time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if removed != 3 {
		t.Errorf("swept %d segments, want 3", removed)
	}

	// Only the active segment (base 9) should be left.
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
		n, _ := strconv.Atoi(strings.TrimSuffix(name, ".log"))
		bases = append(bases, n)
	}
	sort.Ints(bases)
	if len(bases) != 1 || bases[0] != 9 {
		t.Fatalf("remaining segments = %v, want [9]", bases)
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

	// Reopen — the CRC check in loadSegments should fire because loading
	// the active segment now scans records to find the max offset.
	if _, err := openPartition(dir, 1000, 100); err == nil || !strings.Contains(err.Error(), "CRC mismatch") {
		t.Fatalf("openPartition after corruption: got %v, want CRC mismatch", err)
	}
}

func TestPartitionCompaction(t *testing.T) {
	dir := t.TempDir()
	// Roll every 3 records so after 8 appends we have segments at bases 0, 3, 6.
	p, err := openPartition(dir, 3, 100)
	if err != nil {
		t.Fatal(err)
	}
	appends := []struct {
		key, val string
	}{
		{"a", "1"}, {"b", "1"}, {"a", "2"}, // segment 0 (offsets 0,1,2)
		{"c", "1"}, {"a", "3"}, {"b", "2"}, // segment 3 (offsets 3,4,5)
		{"d", "1"}, {"a", "4"}, // segment 6 (offsets 6,7 — still active)
	}
	for _, a := range appends {
		if _, err := p.Append([]byte(a.key), []byte(a.val)); err != nil {
			t.Fatal(err)
		}
	}

	dropped, err := p.Compact()
	if err != nil {
		t.Fatal(err)
	}
	// Sealed records at offsets 0..5. Among those, "a" is superseded by the
	// active-segment record at offset 7 (a=4), so every "a" in sealed
	// (offsets 0, 2, 4) is dropped. "b"=1 at offset 1 is superseded by
	// "b"=2 at offset 5 -> offset 1 dropped. Total = 4 dropped.
	if dropped != 4 {
		t.Errorf("dropped %d records, want 4", dropped)
	}

	// Iterate from 0 should now see: 3 (c=1), 5 (b=2), 6 (d=1), 7 (a=4).
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	type row struct {
		offset   int64
		key, val string
	}
	var got []row
	done := make(chan struct{})
	go func() {
		_ = p.Iterate(ctx, 0, func(off int64, k, v []byte) bool {
			got = append(got, row{off, string(k), string(v)})
			if len(got) == 4 {
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

	want := []row{
		{3, "c", "1"},
		{5, "b", "2"},
		{6, "d", "1"},
		{7, "a", "4"},
	}
	if len(got) != len(want) {
		t.Fatalf("iterate got %d records, want %d: %+v", len(got), len(want), got)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Errorf("got[%d]=%+v, want %+v", i, got[i], want[i])
		}
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
