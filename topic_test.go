package main

import (
	"sync"
	"testing"
)

func TestTopicRoundRobinPartitioning(t *testing.T) {
	topic, err := openTopic(t.TempDir(), 4, 1000, 100)
	if err != nil {
		t.Fatal(err)
	}
	counts := map[int32]int{}
	for i := 0; i < 100; i++ {
		counts[topic.PickPartition(nil)]++
	}
	for p := int32(0); p < 4; p++ {
		if counts[p] != 25 {
			t.Errorf("partition %d got %d messages, want 25", p, counts[p])
		}
	}
}

func TestTopicKeyedPartitioningIsDeterministic(t *testing.T) {
	topic, err := openTopic(t.TempDir(), 4, 1000, 100)
	if err != nil {
		t.Fatal(err)
	}
	first := topic.PickPartition([]byte("foo"))
	for i := 0; i < 100; i++ {
		if got := topic.PickPartition([]byte("foo")); got != first {
			t.Fatalf("same key returned different partitions: %d vs %d", first, got)
		}
	}
}

func TestTopicGroupRebalance(t *testing.T) {
	topic, err := openTopic(t.TempDir(), 4, 1000, 100)
	if err != nil {
		t.Fatal(err)
	}

	var (
		aMu     sync.Mutex
		aAssign []int32
	)
	aID, initial := topic.Join("g", func(p []int32) {
		aMu.Lock()
		aAssign = append([]int32(nil), p...)
		aMu.Unlock()
	})
	if len(initial) != 4 {
		t.Fatalf("first member initial assignment=%v, want 4 partitions", initial)
	}

	var (
		bMu     sync.Mutex
		bAssign []int32
	)
	_, bInitial := topic.Join("g", func(p []int32) {
		bMu.Lock()
		bAssign = append([]int32(nil), p...)
		bMu.Unlock()
	})
	if len(bInitial) != 2 {
		t.Fatalf("second member initial assignment=%v, want 2 partitions", bInitial)
	}

	aMu.Lock()
	aLen := len(aAssign)
	aCopy := append([]int32(nil), aAssign...)
	aMu.Unlock()
	if aLen != 2 {
		t.Errorf("after B joined, A has %d partitions (%v), want 2", aLen, aCopy)
	}

	// Assignments should be disjoint and cover all partitions.
	seen := map[int32]bool{}
	for _, p := range aCopy {
		seen[p] = true
	}
	bMu.Lock()
	for _, p := range bAssign {
		if seen[p] {
			bMu.Unlock()
			t.Fatalf("partition %d assigned to both A and B", p)
		}
		seen[p] = true
	}
	bMu.Unlock()
	if len(seen) != 4 {
		t.Errorf("union of assignments covers %d partitions, want 4", len(seen))
	}

	// A leaves -> B gets everything.
	topic.Leave("g", aID)
	bMu.Lock()
	bFinal := append([]int32(nil), bAssign...)
	bMu.Unlock()
	if len(bFinal) != 4 {
		t.Errorf("after A left, B has %d partitions (%v), want 4", len(bFinal), bFinal)
	}
}

func TestTopicCommitPersistsAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	topic, err := openTopic(dir, 4, 1000, 100)
	if err != nil {
		t.Fatal(err)
	}
	if err := topic.Commit("workers", 2, 42); err != nil {
		t.Fatal(err)
	}
	if err := topic.Commit("workers", 0, 7); err != nil {
		t.Fatal(err)
	}

	// Reopen and verify the committed offsets are still there.
	topic2, err := openTopic(dir, 4, 1000, 100)
	if err != nil {
		t.Fatal(err)
	}
	off, ok := topic2.GroupPartitionOffset("workers", 2)
	if !ok || off != 42 {
		t.Errorf("partition 2 after reopen: off=%d ok=%v, want 42 true", off, ok)
	}
	off, ok = topic2.GroupPartitionOffset("workers", 0)
	if !ok || off != 7 {
		t.Errorf("partition 0 after reopen: off=%d ok=%v, want 7 true", off, ok)
	}
	if _, ok := topic2.GroupPartitionOffset("workers", 3); ok {
		t.Error("partition 3 should not be committed")
	}
	if _, ok := topic2.GroupPartitionOffset("other-group", 0); ok {
		t.Error("unknown group should not be committed")
	}
}
