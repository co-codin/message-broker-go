package main

import (
	"fmt"
	"log"
	"sync/atomic"
	"time"

	"minibroker/client"
)

// Basic pub/sub demo: publish 3 messages to partition 0 BEFORE subscribing,
// then subscribe to partition 0 from offset 0 to verify replay, then
// unsubscribe and confirm no further delivery.
func main() {
	pub, err := client.Dial("localhost:4222")
	if err != nil {
		log.Fatal(err)
	}
	defer pub.Close()

	// Key "news" hashes to a deterministic partition; publish 3 messages there.
	var partition int32
	for _, m := range []string{"one", "two", "three"} {
		pid, _, err := pub.Publish("news", []byte("news"), []byte(m))
		if err != nil {
			log.Fatal(err)
		}
		partition = pid
	}
	fmt.Printf("published 3 messages to partition %d\n", partition)

	sub, err := client.Dial("localhost:4222")
	if err != nil {
		log.Fatal(err)
	}
	defer sub.Close()

	var count int32
	done := make(chan struct{})
	err = sub.Subscribe("news", partition, 0, func(topic string, pid int32, offset int64, payload []byte) {
		fmt.Printf("recv: [%s/%d@%d] %s\n", topic, pid, offset, payload)
		if atomic.AddInt32(&count, 1) == 3 {
			close(done)
		}
	})
	if err != nil {
		log.Fatal(err)
	}
	select {
	case <-done:
		fmt.Println("-- replay OK")
	case <-time.After(2 * time.Second):
		log.Fatalf("timeout: got %d", atomic.LoadInt32(&count))
	}

	if err := sub.Unsubscribe("news"); err != nil {
		log.Fatal(err)
	}
	before := atomic.LoadInt32(&count)
	if _, _, err := pub.Publish("news", []byte("news"), []byte("after-unsub")); err != nil {
		log.Fatal(err)
	}
	time.Sleep(200 * time.Millisecond)
	if got := atomic.LoadInt32(&count); got != before {
		log.Fatalf("unsubscribe failed: %d extra", got-before)
	}
	fmt.Println("-- unsubscribe OK")
}
