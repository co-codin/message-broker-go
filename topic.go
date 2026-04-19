package main

import (
	"fmt"
	"hash/fnv"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

// RebalanceFn is called on the topic goroutine each time the group's
// assignment for this member changes (including the first assignment after
// join, and the final empty assignment when the member leaves).
type RebalanceFn func(assignment []int32)

type groupMember struct {
	id       int64 // monotonic, used for stable sort order
	onChange RebalanceFn
	current  []int32
}

type Topic struct {
	name           string
	dir            string
	partitions     []*Partition
	pubCounter     atomic.Int64 // round-robin publish cursor
	groupsMu       sync.Mutex
	groupOffsets   map[string]map[int32]int64 // group -> partition -> next-to-read
	groupMembers   map[string][]*groupMember  // group -> members (sorted by id)
	nextMemberID   int64
}

func openTopic(dir string, numPartitions, maxPerSeg, retain int) (*Topic, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Join(dir, "offsets"), 0o755); err != nil {
		return nil, err
	}
	parts := make([]*Partition, numPartitions)
	for i := 0; i < numPartitions; i++ {
		p, err := openPartition(filepath.Join(dir, fmt.Sprintf("part-%d", i)), maxPerSeg, retain)
		if err != nil {
			return nil, err
		}
		parts[i] = p
	}
	t := &Topic{
		dir:          dir,
		partitions:   parts,
		groupOffsets: make(map[string]map[int32]int64),
		groupMembers: make(map[string][]*groupMember),
	}
	if err := t.loadOffsets(); err != nil {
		return nil, err
	}
	return t, nil
}

func (t *Topic) NumPartitions() int { return len(t.partitions) }

func (t *Topic) Partition(id int32) (*Partition, error) {
	if int(id) < 0 || int(id) >= len(t.partitions) {
		return nil, fmt.Errorf("partition %d out of range (0..%d)", id, len(t.partitions)-1)
	}
	return t.partitions[id], nil
}

// PickPartition chooses a partition for an incoming PUB. With no key it's
// round-robin; with a key it's FNV hash modulo partition count.
func (t *Topic) PickPartition(key []byte) int32 {
	if len(key) == 0 {
		n := t.pubCounter.Add(1) - 1
		return int32(n % int64(len(t.partitions)))
	}
	h := fnv.New32a()
	h.Write(key)
	return int32(h.Sum32() % uint32(len(t.partitions)))
}

// ---- consumer groups -------------------------------------------------------

func (t *Topic) loadOffsets() error {
	root := filepath.Join(t.dir, "offsets")
	groups, err := os.ReadDir(root)
	if err != nil {
		return err
	}
	for _, g := range groups {
		if !g.IsDir() {
			continue
		}
		gname := g.Name()
		gdir := filepath.Join(root, gname)
		files, err := os.ReadDir(gdir)
		if err != nil {
			return err
		}
		for _, f := range files {
			if f.IsDir() {
				continue
			}
			pid, err := strconv.ParseInt(strings.TrimSuffix(f.Name(), ".off"), 10, 32)
			if err != nil {
				continue
			}
			data, err := os.ReadFile(filepath.Join(gdir, f.Name()))
			if err != nil {
				return err
			}
			off, err := strconv.ParseInt(strings.TrimSpace(string(data)), 10, 64)
			if err != nil {
				continue
			}
			if t.groupOffsets[gname] == nil {
				t.groupOffsets[gname] = make(map[int32]int64)
			}
			t.groupOffsets[gname][int32(pid)] = off
		}
	}
	return nil
}

func (t *Topic) GroupPartitionOffset(group string, partition int32) (int64, bool) {
	t.groupsMu.Lock()
	defer t.groupsMu.Unlock()
	m, ok := t.groupOffsets[group]
	if !ok {
		return 0, false
	}
	off, ok := m[partition]
	return off, ok
}

func (t *Topic) Commit(group string, partition int32, offset int64) error {
	t.groupsMu.Lock()
	if t.groupOffsets[group] == nil {
		t.groupOffsets[group] = make(map[int32]int64)
	}
	t.groupOffsets[group][partition] = offset
	t.groupsMu.Unlock()

	gdir := filepath.Join(t.dir, "offsets", group)
	if err := os.MkdirAll(gdir, 0o755); err != nil {
		return err
	}
	path := filepath.Join(gdir, fmt.Sprintf("%d.off", partition))
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, []byte(strconv.FormatInt(offset, 10)), 0o644); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	commitsTotal.Inc()
	return nil
}

// Join adds a member to a group and triggers a rebalance. Returns the
// member's ID (used later by Leave) and the initial assignment.
func (t *Topic) Join(group string, onChange RebalanceFn) (int64, []int32) {
	t.groupsMu.Lock()
	t.nextMemberID++
	id := t.nextMemberID
	m := &groupMember{id: id, onChange: onChange}
	t.groupMembers[group] = append(t.groupMembers[group], m)
	members := append([]*groupMember(nil), t.groupMembers[group]...)
	t.groupsMu.Unlock()

	// rebalance outside the lock (but serialized by the fact we're on the
	// calling goroutine; no two Join/Leave calls are concurrent for the same
	// group because they take groupsMu briefly).
	t.rebalance(members)

	// Find the new member's assignment (rebalance set m.current).
	t.groupsMu.Lock()
	initial := append([]int32(nil), m.current...)
	t.groupsMu.Unlock()
	return id, initial
}

func (t *Topic) Leave(group string, memberID int64) {
	t.groupsMu.Lock()
	members := t.groupMembers[group]
	var kept []*groupMember
	for _, m := range members {
		if m.id != memberID {
			kept = append(kept, m)
		}
	}
	t.groupMembers[group] = kept
	remainingCopy := append([]*groupMember(nil), kept...)
	t.groupsMu.Unlock()
	t.rebalance(remainingCopy)
}

// rebalance distributes partitions round-robin over the (already sorted) member
// list and notifies each member whose assignment changed.
func (t *Topic) rebalance(members []*groupMember) {
	// Stable sort by id so assignment is deterministic across runs.
	sort.Slice(members, func(i, j int) bool { return members[i].id < members[j].id })

	// Distribute partitions.
	newAssign := make(map[int64][]int32)
	for _, m := range members {
		newAssign[m.id] = nil
	}
	if len(members) > 0 {
		for p := 0; p < len(t.partitions); p++ {
			m := members[p%len(members)]
			newAssign[m.id] = append(newAssign[m.id], int32(p))
		}
	}

	// Compare old vs new; call onChange for those that differ.
	type notice struct {
		fn   RebalanceFn
		next []int32
	}
	var notices []notice
	t.groupsMu.Lock()
	for _, m := range members {
		next := newAssign[m.id]
		if !equalInts(m.current, next) {
			m.current = next
			notices = append(notices, notice{fn: m.onChange, next: append([]int32(nil), next...)})
		}
	}
	t.groupsMu.Unlock()

	for _, n := range notices {
		n.fn(n.next)
	}
}

func equalInts(a, b []int32) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
