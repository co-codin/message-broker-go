package main

import (
	"flag"
	"log"
	"time"
)

func main() {
	addr := flag.String("addr", ":4222", "TCP address to listen on")
	dir := flag.String("dir", "./data", "directory for topic logs")
	partitions := flag.Int("partitions", 4, "partitions per topic")
	segSize := flag.Int("segment-size", 1000, "records per segment before rolling")
	retain := flag.Int("retain", 100, "segments kept per partition (sealed + active)")
	retainFor := flag.Duration("retain-for", 0,
		"time-based retention: drop sealed segments older than this duration (0 disables)")
	sweepEvery := flag.Duration("sweep-every", 30*time.Second,
		"how often to run the time-retention sweep when -retain-for is set")
	heartbeatTimeout := flag.Duration("heartbeat-timeout", 15*time.Second,
		"group members without a heartbeat within this window get kicked (0 disables)")
	flag.Parse()

	broker, err := NewBroker(*dir, *partitions, *segSize, *retain)
	if err != nil {
		log.Fatal(err)
	}
	broker.SetTimeRetention(*retainFor, *sweepEvery)
	broker.Run()
	defer broker.Stop()

	server := NewServer(broker)
	server.SetHeartbeatTimeout(*heartbeatTimeout)
	defer server.Stop()

	if err := server.Listen(*addr); err != nil {
		log.Fatal(err)
	}
}
