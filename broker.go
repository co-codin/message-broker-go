package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"sync"
	"time"
)

var validTopic = regexp.MustCompile(`^[a-zA-Z0-9._-]+$`)

type Broker struct {
	dir         string
	numParts    int
	maxPerSeg   int
	retain      int
	retainFor   time.Duration // 0 disables time-based sweeping
	sweepEvery  time.Duration
	compactEvery time.Duration // 0 disables compaction

	mu     sync.Mutex
	topics map[string]*Topic

	stopCh   chan struct{}
	stopOnce sync.Once
}

func NewBroker(dir string, numParts, maxPerSeg, retain int) (*Broker, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	if numParts < 1 {
		numParts = 1
	}
	if maxPerSeg < 1 {
		maxPerSeg = 1000
	}
	if retain < 1 {
		retain = 100
	}
	return &Broker{
		dir:       dir,
		numParts:  numParts,
		maxPerSeg: maxPerSeg,
		retain:    retain,
		topics:    make(map[string]*Topic),
		stopCh:    make(chan struct{}),
	}, nil
}

// SetTimeRetention enables a background sweep that deletes sealed segments
// older than `age`. Sweep fires every `every`; pass zero values to disable.
func (b *Broker) SetTimeRetention(age, every time.Duration) {
	b.retainFor = age
	b.sweepEvery = every
}

// SetCompaction enables periodic log compaction. Every `every`, each
// partition of every topic rewrites its sealed segments keeping only the
// latest record per key. Pass zero to disable.
func (b *Broker) SetCompaction(every time.Duration) {
	b.compactEvery = every
}

// Run starts background jobs (retention sweep, compaction). Call once after
// NewBroker. Returns immediately; jobs stop when Stop is called.
func (b *Broker) Run() {
	if b.retainFor > 0 && b.sweepEvery > 0 {
		go b.sweepLoop()
	}
	if b.compactEvery > 0 {
		go b.compactLoop()
	}
}

func (b *Broker) Stop() {
	b.stopOnce.Do(func() { close(b.stopCh) })
}

func (b *Broker) sweepLoop() {
	t := time.NewTicker(b.sweepEvery)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			b.sweepOnce()
		case <-b.stopCh:
			return
		}
	}
}

func (b *Broker) compactLoop() {
	t := time.NewTicker(b.compactEvery)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			b.compactOnce()
		case <-b.stopCh:
			return
		}
	}
}

func (b *Broker) compactOnce() {
	b.mu.Lock()
	topics := make([]*Topic, 0, len(b.topics))
	for _, t := range b.topics {
		topics = append(topics, t)
	}
	b.mu.Unlock()

	total := 0
	for _, t := range topics {
		for _, p := range t.partitions {
			n, err := p.Compact()
			if err != nil {
				log.Printf("compaction error on %s: %v", t.name, err)
				continue
			}
			total += n
		}
	}
	if total > 0 {
		log.Printf("compaction dropped %d superseded records", total)
	}
}

func (b *Broker) sweepOnce() {
	b.mu.Lock()
	topics := make([]*Topic, 0, len(b.topics))
	for _, t := range b.topics {
		topics = append(topics, t)
	}
	b.mu.Unlock()

	total := 0
	for _, t := range topics {
		for _, p := range t.partitions {
			n, err := p.SweepOlderThan(b.retainFor)
			if err != nil {
				log.Printf("sweep error on %s: %v", t.name, err)
				continue
			}
			total += n
		}
	}
	if total > 0 {
		log.Printf("time-based retention removed %d segments", total)
	}
}

func (b *Broker) Topic(name string) (*Topic, error) {
	if !validTopic.MatchString(name) {
		return nil, fmt.Errorf("invalid topic: %q", name)
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if t, ok := b.topics[name]; ok {
		return t, nil
	}
	t, err := openTopic(filepath.Join(b.dir, name), b.numParts, b.maxPerSeg, b.retain)
	if err != nil {
		return nil, err
	}
	t.name = name
	b.topics[name] = t
	return t, nil
}

// Publish writes to the partition chosen by PickPartition(key). Pass key=nil
// for round-robin distribution.
func (b *Broker) Publish(topicName string, key, payload []byte) (int32, int64, error) {
	t, err := b.Topic(topicName)
	if err != nil {
		return 0, 0, err
	}
	pid := t.PickPartition(key)
	part, err := t.Partition(pid)
	if err != nil {
		return 0, 0, err
	}
	offset, err := part.Append(key, payload)
	if err == nil {
		publishesTotal.Inc()
	}
	return pid, offset, err
}

// PartitionHead returns the next-to-assign offset for a partition.
func (b *Broker) PartitionHead(topicName string, partition int32) (int64, error) {
	t, err := b.Topic(topicName)
	if err != nil {
		return 0, err
	}
	p, err := t.Partition(partition)
	if err != nil {
		return 0, err
	}
	return p.Head(), nil
}

func (b *Broker) Iterate(ctx context.Context, topicName string, partition int32, from int64, fn func(offset int64, key, payload []byte) bool) error {
	t, err := b.Topic(topicName)
	if err != nil {
		return err
	}
	p, err := t.Partition(partition)
	if err != nil {
		return err
	}
	return p.Iterate(ctx, from, fn)
}
