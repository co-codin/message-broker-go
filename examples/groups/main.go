package main

import (
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"minibroker/client"
)

// Single-member consumer-group demo (partition-aware).
// Phase 1: publish 3 messages (round-robin across partitions).
// Phase 2: session A joins group=worker, commits next-offset for each.
// Phase 3: publish 2 more.
// Phase 4: session B joins same group, should resume and receive only 2.
//
// Run broker with `-partitions 1` for deterministic offsets in the log below.
func main() {
	pub, err := client.Dial("localhost:4222")
	if err != nil {
		log.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if _, _, err := pub.Publish("jobs", []byte("k"), fmt.Appendf(nil, "job-%d", i)); err != nil {
			log.Fatal(err)
		}
	}

	fmt.Println("session A: joining group=worker")
	sessionA, err := client.Dial("localhost:4222")
	if err != nil {
		log.Fatal(err)
	}
	var aCount int32
	aDone := make(chan struct{})
	var aMu sync.Mutex
	var aReceived []int64
	err = sessionA.SubscribeGroup("jobs", "worker", nil,
		func(topic string, pid int32, offset int64, payload []byte) {
			aMu.Lock()
			aReceived = append(aReceived, offset)
			aMu.Unlock()
			fmt.Printf("  A recv: [%s/%d@%d] %s\n", topic, pid, offset, payload)
			if err := sessionA.Commit(topic, "worker", pid, offset+1); err != nil {
				log.Printf("commit err: %v", err)
			}
			if atomic.AddInt32(&aCount, 1) == 3 {
				close(aDone)
			}
		})
	if err != nil {
		log.Fatal(err)
	}
	select {
	case <-aDone:
	case <-time.After(2 * time.Second):
		log.Fatalf("A timeout: got %v", aReceived)
	}
	sessionA.Close()
	fmt.Println("session A: closed (committed next=3)")
	time.Sleep(150 * time.Millisecond)

	for i := 3; i < 5; i++ {
		if _, _, err := pub.Publish("jobs", []byte("k"), fmt.Appendf(nil, "job-%d", i)); err != nil {
			log.Fatal(err)
		}
	}
	pub.Close()

	fmt.Println("session B: rejoining group=worker")
	sessionB, err := client.Dial("localhost:4222")
	if err != nil {
		log.Fatal(err)
	}
	defer sessionB.Close()

	var bCount int32
	bDone := make(chan struct{})
	var bMu sync.Mutex
	var bReceived []int64
	err = sessionB.SubscribeGroup("jobs", "worker", nil,
		func(topic string, pid int32, offset int64, payload []byte) {
			bMu.Lock()
			bReceived = append(bReceived, offset)
			bMu.Unlock()
			fmt.Printf("  B recv: [%s/%d@%d] %s\n", topic, pid, offset, payload)
			if atomic.AddInt32(&bCount, 1) == 2 {
				close(bDone)
			}
		})
	if err != nil {
		log.Fatal(err)
	}
	select {
	case <-bDone:
		bMu.Lock()
		got := append([]int64(nil), bReceived...)
		bMu.Unlock()
		if len(got) == 2 && got[0] == 3 && got[1] == 4 {
			fmt.Println("-- group resumed correctly; got [3, 4]")
		} else {
			log.Fatalf("unexpected offsets: %v", got)
		}
	case <-time.After(2 * time.Second):
		log.Fatalf("B timeout: got %v", bReceived)
	}
}
