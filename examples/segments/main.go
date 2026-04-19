package main

import (
	"fmt"
	"log"
	"sync/atomic"
	"time"

	"minibroker/client"
)

// Segments demo: publish 13 messages with a fixed key (all to partition 0),
// with broker started as `-segment-size 5 -retain 2 -partitions 1`.
// Only the segments at base offsets 5 and 10 should remain.
func main() {
	pub, err := client.Dial("localhost:4222")
	if err != nil {
		log.Fatal(err)
	}
	defer pub.Close()

	key := []byte("k")
	for i := 0; i < 13; i++ {
		if _, _, err := pub.Publish("logs", key, fmt.Appendf(nil, "msg-%02d", i)); err != nil {
			log.Fatal(err)
		}
	}
	fmt.Println("published 13 messages")

	sub, err := client.Dial("localhost:4222")
	if err != nil {
		log.Fatal(err)
	}
	defer sub.Close()

	var seen int32
	done := make(chan struct{})
	err = sub.Subscribe("logs", 0, 5, func(topic string, pid int32, offset int64, key, payload []byte) {
		fmt.Printf("  recv: [%s/%d@%d] %s\n", topic, pid, offset, payload)
		if atomic.AddInt32(&seen, 1) == 8 {
			close(done)
		}
	})
	if err != nil {
		log.Fatal(err)
	}
	select {
	case <-done:
		fmt.Println("-- replay from offset 5 OK: 8 records")
	case <-time.After(3 * time.Second):
		log.Fatalf("timeout: saw %d", atomic.LoadInt32(&seen))
	}
}
