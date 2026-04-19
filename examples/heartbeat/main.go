package main

// Demo of the heartbeat/eviction path.
//
// Start the broker with a tiny timeout:   ./minibroker -heartbeat-timeout 2s
//
// Normal client A sends heartbeats (done automatically by SubscribeGroup),
// so its membership persists. A second "stuck" connection is simulated by
// opening a raw TCP connection, sending a SUB group= frame, and then never
// sending any heartbeats — the server should close that connection within
// one eviction-sweep cycle.
import (
	"encoding/binary"
	"fmt"
	"log"
	"net"
	"time"

	"minibroker/client"
	"minibroker/proto"
)

func main() {
	// Heartbeat every 500ms so we beat a 2s server timeout comfortably.
	client.SetHeartbeatInterval(500 * time.Millisecond)

	// 1. Normal consumer that DOES send heartbeats.
	good, err := client.Dial("localhost:4222")
	if err != nil {
		log.Fatal(err)
	}
	defer good.Close()
	if err := good.SubscribeGroup("events", "g", nil,
		func(topic string, pid int32, off int64, key, payload []byte) {}); err != nil {
		log.Fatal(err)
	}
	fmt.Println("good: joined, heartbeating automatically")

	// 2. Stuck consumer: opens a raw socket, sends SUB group=g, then nothing.
	raw, err := net.Dial("tcp", "localhost:4222")
	if err != nil {
		log.Fatal(err)
	}
	body := proto.NewBuilder().String("events").Byte(proto.SubGroup).String("g").Build()
	if err := proto.WriteFrame(raw, proto.OpSub, body); err != nil {
		log.Fatal(err)
	}
	// Read the initial OK frame so the server sees a happy subscriber.
	var header [4]byte
	if _, err := raw.Read(header[:]); err != nil {
		log.Fatal(err)
	}
	total := binary.BigEndian.Uint32(header[:])
	skip := make([]byte, total)
	if _, err := raw.Read(skip); err != nil {
		log.Fatal(err)
	}
	fmt.Println("stuck: joined with no heartbeats")

	start := time.Now()
	// Wait for the server to close our connection — a successful eviction.
	buf := make([]byte, 256)
	for {
		_, err := raw.Read(buf)
		if err != nil {
			elapsed := time.Since(start)
			fmt.Printf("stuck: connection closed by server after %s (eviction OK)\n", elapsed.Round(100*time.Millisecond))
			return
		}
	}
}
