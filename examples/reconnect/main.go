package main

import (
	"fmt"
	"log"
	"sync/atomic"
	"time"

	"minibroker/client"
)

func main() {
	sub, err := client.Dial("localhost:4222")
	if err != nil {
		log.Fatal(err)
	}
	defer sub.Close()

	var count int32
	done := make(chan struct{})
	err = sub.Subscribe("events", 0, 0, func(topic string, pid int32, offset int64, payload []byte) {
		fmt.Printf("recv: [%s/%d@%d] %s\n", topic, pid, offset, payload)
		if atomic.AddInt32(&count, 1) == 4 {
			close(done)
		}
	})
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("subscriber running — broker will be restarted by the test harness")
	select {
	case <-done:
		fmt.Println("-- received all 4 messages (across a broker restart)")
	case <-time.After(60 * time.Second):
		log.Fatalf("timeout — only received %d", atomic.LoadInt32(&count))
	}
}
