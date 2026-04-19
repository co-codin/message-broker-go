package main

// Cluster demo: probes the 3 broker ports, finds the Raft leader (the one
// that accepts a PUB), publishes 5 messages there, then reads them back from
// a FOLLOWER to prove the data was replicated.
//
// Usage: run examples/cluster/run-cluster.sh — it starts 3 nodes and
// invokes `go run ./examples/cluster`.

import (
	"fmt"
	"log"
	"strings"
	"sync/atomic"
	"time"

	"minibroker/client"
)

var nodes = []string{"localhost:4221", "localhost:4222", "localhost:4223"}

func main() {
	// 1. Find the leader: whichever node accepts a test publish.
	var leaderIdx = -1
	var leader *client.Client
	for i, addr := range nodes {
		c, err := client.Dial(addr)
		if err != nil {
			fmt.Printf("  %s: dial failed: %v\n", addr, err)
			continue
		}
		_, _, err = c.Publish("probe", nil, []byte("hello"))
		if err == nil {
			leaderIdx = i
			leader = c
			fmt.Printf("  leader: %s\n", addr)
			break
		}
		msg := err.Error()
		if strings.Contains(msg, "not leader") {
			fmt.Printf("  %s: follower (%s)\n", addr, msg)
		} else {
			fmt.Printf("  %s: other err: %v\n", addr, msg)
		}
		c.Close()
	}
	if leader == nil {
		log.Fatal("no leader found")
	}
	defer leader.Close()

	// 2. Publish 5 messages on the leader.
	for i := 0; i < 5; i++ {
		pid, off, err := leader.Publish("events", []byte("k"), fmt.Appendf(nil, "msg-%d", i))
		if err != nil {
			log.Fatalf("publish #%d: %v", i, err)
		}
		fmt.Printf("  PUB -> partition=%d offset=%d\n", pid, off)
	}

	// Give replication a moment to land on followers.
	time.Sleep(500 * time.Millisecond)

	// 3. Subscribe from a follower (pick any other node). The follower
	// should have received the same records via Raft replication.
	var followerAddr string
	for i, addr := range nodes {
		if i != leaderIdx {
			followerAddr = addr
			break
		}
	}
	follower, err := client.Dial(followerAddr)
	if err != nil {
		log.Fatal(err)
	}
	defer follower.Close()

	var got int32
	done := make(chan struct{})
	err = follower.Subscribe("events", 0, 0,
		func(topic string, pid int32, offset int64, key, payload []byte) {
			fmt.Printf("  follower %s recv: [%s/%d@%d] %s\n", followerAddr, topic, pid, offset, payload)
			if atomic.AddInt32(&got, 1) == 5 {
				close(done)
			}
		})
	if err != nil {
		log.Fatal(err)
	}
	select {
	case <-done:
		fmt.Println("-- replication OK: all 5 messages visible on follower")
	case <-time.After(3 * time.Second):
		log.Fatalf("timeout: follower only saw %d / 5", atomic.LoadInt32(&got))
	}
}
