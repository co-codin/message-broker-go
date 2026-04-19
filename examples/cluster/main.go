package main

// Cluster demo: hand the client all 3 node addresses and let it figure out
// which one is the leader. The client auto-rotates on "not leader" errors.
//
// Usage: run examples/cluster/run-cluster.sh — it starts 3 nodes and
// invokes `go run ./examples/cluster`.

import (
	"fmt"
	"log"
	"sync/atomic"
	"time"

	"minibroker/client"
)

var nodes = []string{"localhost:4221", "localhost:4222", "localhost:4223"}

func main() {
	// Producer with all three addresses — writes auto-redirect to the leader.
	producer, err := client.Dial(nodes...)
	if err != nil {
		log.Fatal(err)
	}
	defer producer.Close()

	for i := 0; i < 5; i++ {
		pid, off, err := producer.Publish("events", []byte("k"), fmt.Appendf(nil, "msg-%d", i))
		if err != nil {
			log.Fatalf("publish #%d: %v", i, err)
		}
		fmt.Printf("  PUB -> partition=%d offset=%d\n", pid, off)
	}

	// Give replication a moment to land on followers.
	time.Sleep(500 * time.Millisecond)

	// Consumer pinned to node 2 (could be a follower) — reads still work.
	consumer, err := client.Dial(nodes[1])
	if err != nil {
		log.Fatal(err)
	}
	defer consumer.Close()

	var got int32
	done := make(chan struct{})
	err = consumer.Subscribe("events", 0, 0,
		func(topic string, pid int32, offset int64, key, payload []byte) {
			fmt.Printf("  %s recv: [%s/%d@%d] %s\n", nodes[1], topic, pid, offset, payload)
			if atomic.AddInt32(&got, 1) == 5 {
				close(done)
			}
		})
	if err != nil {
		log.Fatal(err)
	}
	select {
	case <-done:
		fmt.Println("-- replication OK: 5 messages visible on the other node")
	case <-time.After(3 * time.Second):
		log.Fatalf("timeout: only saw %d / 5", atomic.LoadInt32(&got))
	}
}
