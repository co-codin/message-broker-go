package main

import (
	"flag"
	"log"
)

func main() {
	addr := flag.String("addr", ":4222", "TCP address to listen on")
	dir := flag.String("dir", "./data", "directory for topic logs")
	partitions := flag.Int("partitions", 4, "partitions per topic")
	segSize := flag.Int("segment-size", 1000, "records per segment before rolling")
	retain := flag.Int("retain", 100, "segments kept per partition (sealed + active)")
	flag.Parse()

	broker, err := NewBroker(*dir, *partitions, *segSize, *retain)
	if err != nil {
		log.Fatal(err)
	}
	server := NewServer(broker)
	if err := server.Listen(*addr); err != nil {
		log.Fatal(err)
	}
}
