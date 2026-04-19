package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sync"
)

var validTopic = regexp.MustCompile(`^[a-zA-Z0-9._-]+$`)

type Broker struct {
	dir        string
	numParts   int
	maxPerSeg  int
	retain     int

	mu     sync.Mutex
	topics map[string]*Topic
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
	}, nil
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
