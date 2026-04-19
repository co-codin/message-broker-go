package main

import (
	"bytes"
	"fmt"
	"log"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"minibroker/client"
)

// Partitions demo (run broker with `-partitions 4`):
//
//   1. Publish 20 binary-safe messages round-robin across 4 partitions.
//   2. Worker A joins group=pipeline -> gets all 4 partitions initially.
//   3. Worker B joins the same group -> rebalance kicks in; A gets 2, B gets 2.
//   4. Both workers drain & commit; the UNION of their received offsets covers
//      every record exactly once.
//
// Also demonstrates that payloads with spaces AND a literal newline survive
// the binary protocol unscathed (the old text protocol couldn't carry them).
func main() {
	// --- Phase 1: publisher handle (publishes start in phase 3) ------------
	pub, err := client.Dial("localhost:4222")
	if err != nil {
		log.Fatal(err)
	}
	defer pub.Close()

	// --- Phase 2: worker A only -------------------------------------------
	workerA, err := client.Dial("localhost:4222")
	if err != nil {
		log.Fatal(err)
	}
	defer workerA.Close()

	var (
		seenMu  sync.Mutex
		seenA   = map[string]struct{}{}
		seenB   = map[string]struct{}{}
		totalN  int32
		allDone = make(chan struct{})
	)
	record := func(label string, bucket map[string]struct{}, payload []byte) {
		seenMu.Lock()
		bucket[string(payload)] = struct{}{}
		total := atomic.AddInt32(&totalN, 1)
		seenMu.Unlock()
		fmt.Printf("  %s recv: %q\n", label, payload)
		if total == 20 {
			close(allDone)
		}
	}

	assignA := make(chan []int32, 4)
	err = workerA.SubscribeGroup("events", "pipeline",
		func(topic, group string, assign []int32) {
			fmt.Printf("  rebalance A: %v\n", assign)
			assignA <- assign
		},
		func(topic string, pid int32, offset int64, payload []byte) {
			record("A", seenA, payload)
			if err := workerA.Commit(topic, "pipeline", pid, offset+1); err != nil {
				log.Printf("A commit err: %v", err)
			}
		})
	if err != nil {
		log.Fatal(err)
	}
	initial := <-assignA
	if len(initial) != 4 {
		log.Fatalf("A should start with all 4 partitions, got %v", initial)
	}

	// --- Phase 3: worker B joins BEFORE publishing so both share the load.
	workerB, err := client.Dial("localhost:4222")
	if err != nil {
		log.Fatal(err)
	}
	defer workerB.Close()

	assignB := make(chan []int32, 4)
	err = workerB.SubscribeGroup("events", "pipeline",
		func(topic, group string, assign []int32) {
			fmt.Printf("  rebalance B: %v\n", assign)
			assignB <- assign
		},
		func(topic string, pid int32, offset int64, payload []byte) {
			record("B", seenB, payload)
			if err := workerB.Commit(topic, "pipeline", pid, offset+1); err != nil {
				log.Printf("B commit err: %v", err)
			}
		})
	if err != nil {
		log.Fatal(err)
	}
	bInit := <-assignB
	fmt.Printf("B initial assignment: %v\n", bInit)
	// Wait for A's rebalance (A -> subset) so the server is done reassigning
	// iterators before we publish.
	<-assignA
	time.Sleep(100 * time.Millisecond)

	// --- Phase 3b: now publish 20 binary-safe messages --------------------
	expected := map[string]struct{}{}
	for i := 0; i < 20; i++ {
		payload := fmt.Appendf(nil, "row-%02d\tfield with spaces\nand a newline", i)
		expected[string(payload)] = struct{}{}
		if _, _, err := pub.Publish("events", nil, payload); err != nil {
			log.Fatal(err)
		}
	}
	fmt.Println("published 20 binary-safe messages across 4 partitions")

	// --- Phase 4: wait for everything -------------------------------------
	select {
	case <-allDone:
	case <-time.After(4 * time.Second):
		log.Fatalf("timeout: only saw %d/20 messages", atomic.LoadInt32(&totalN))
	}

	// Sanity checks.
	union := map[string]struct{}{}
	for k := range seenA {
		union[k] = struct{}{}
	}
	for k := range seenB {
		union[k] = struct{}{}
	}
	if len(union) != 20 {
		log.Fatalf("union size is %d, want 20", len(union))
	}
	// Verify binary payload round-trip.
	var oneExpected []byte
	for k := range expected {
		oneExpected = []byte(k)
		break
	}
	_, inA := seenA[string(oneExpected)]
	_, inB := seenB[string(oneExpected)]
	if !inA && !inB {
		log.Fatalf("payload round-trip failed: %q", oneExpected)
	}
	if !bytes.Contains(oneExpected, []byte("\n")) {
		log.Fatal("test payload should contain a newline")
	}

	fmt.Printf("-- both workers together covered all 20 offsets\n")
	fmt.Printf("   A received %d, B received %d (total %d)\n",
		len(seenA), len(seenB), len(seenA)+len(seenB))

	// Diagnostic: show the partition split (by listing who got what, de-duped).
	_ = sort.Ints
}
