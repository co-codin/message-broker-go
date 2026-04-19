// Benchmark harness comparing minibroker to NATS (JetStream), RabbitMQ, and
// Redis Streams — all in durable-publish mode with per-message acknowledgement
// so the numbers are apples-to-apples.
//
//   go run ./bench -target minibroker -n 20000 -size 100
//   go run ./bench -target nats-js    -n 20000 -size 100
//   go run ./bench -target rabbit     -n 20000 -size 100
//   go run ./bench -target redis      -n 20000 -size 100
//
// Assumes the respective brokers are reachable (see docker-compose.bench.yml).
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	"minibroker/client"

	"github.com/nats-io/nats.go"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/redis/go-redis/v9"
)

func main() {
	target := flag.String("target", "minibroker", "minibroker | nats-js | rabbit | redis")
	n := flag.Int("n", 20000, "number of messages to publish")
	size := flag.Int("size", 100, "message payload size in bytes")
	addr := flag.String("addr", "", "broker address (target-specific default if empty)")
	flag.Parse()

	payload := make([]byte, *size)
	for i := range payload {
		payload[i] = byte(i % 256)
	}

	switch *target {
	case "minibroker":
		benchMinibroker(*addr, *n, payload)
	case "nats-js":
		benchNATS(*addr, *n, payload)
	case "rabbit":
		benchRabbit(*addr, *n, payload)
	case "redis":
		benchRedis(*addr, *n, payload)
	default:
		log.Fatalf("unknown target %q", *target)
	}
}

func report(name string, n, size int, elapsed time.Duration) {
	rate := float64(n) / elapsed.Seconds()
	mib := rate * float64(size) / (1024 * 1024)
	fmt.Printf("%-14s  n=%-7d  size=%-5d  %7.2fs  %10.0f msg/s  %6.2f MiB/s\n",
		name, n, size, elapsed.Seconds(), rate, mib)
}

// --- minibroker (durable: fsync on every Append) -----------------------------
func benchMinibroker(addr string, n int, payload []byte) {
	if addr == "" {
		addr = "localhost:4222"
	}
	c, err := client.Dial(addr)
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	start := time.Now()
	for i := 0; i < n; i++ {
		if _, _, err := c.Publish("bench", nil, payload); err != nil {
			log.Fatal(err)
		}
	}
	report("minibroker", n, len(payload), time.Since(start))
}

// --- NATS JetStream (durable: file storage, sync publish) --------------------
func benchNATS(addr string, n int, payload []byte) {
	if addr == "" {
		addr = "nats://localhost:4223"
	}
	nc, err := nats.Connect(addr, nats.Timeout(5*time.Second))
	if err != nil {
		log.Fatal(err)
	}
	defer nc.Close()

	js, err := nc.JetStream()
	if err != nil {
		log.Fatal(err)
	}

	streamName := "BENCH"
	_ = js.DeleteStream(streamName) // clean slate
	if _, err := js.AddStream(&nats.StreamConfig{
		Name:     streamName,
		Subjects: []string{"bench"},
		Storage:  nats.FileStorage,
	}); err != nil {
		log.Fatal(err)
	}

	start := time.Now()
	for i := 0; i < n; i++ {
		if _, err := js.Publish("bench", payload); err != nil {
			log.Fatal(err)
		}
	}
	report("nats-js", n, len(payload), time.Since(start))
}

// --- RabbitMQ (durable queue, persistent messages, publisher confirms) -------
func benchRabbit(addr string, n int, payload []byte) {
	if addr == "" {
		addr = "amqp://guest:guest@localhost:5672/"
	}
	conn, err := amqp.Dial(addr)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()
	ch, err := conn.Channel()
	if err != nil {
		log.Fatal(err)
	}
	defer ch.Close()

	if _, err := ch.QueueDelete("bench", false, false, false); err != nil {
		log.Fatal(err)
	}
	if _, err := ch.QueueDeclare("bench", true, false, false, false, nil); err != nil {
		log.Fatal(err)
	}
	if err := ch.Confirm(false); err != nil {
		log.Fatal(err)
	}
	confirms := ch.NotifyPublish(make(chan amqp.Confirmation, 1))

	ctx := context.Background()
	start := time.Now()
	for i := 0; i < n; i++ {
		if err := ch.PublishWithContext(ctx, "", "bench", false, false, amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			Body:         payload,
		}); err != nil {
			log.Fatal(err)
		}
		// Wait for the confirm before publishing the next one so the
		// comparison with the other (synchronous-per-message) brokers
		// stays apples-to-apples.
		if c := <-confirms; !c.Ack {
			log.Fatalf("nack on msg %d", i)
		}
	}
	report("rabbitmq", n, len(payload), time.Since(start))
}

// --- Redis Streams (XADD; AOF appendfsync=always via docker-compose) ---------
func benchRedis(addr string, n int, payload []byte) {
	if addr == "" {
		addr = "localhost:6379"
	}
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	defer rdb.Close()

	ctx := context.Background()
	if err := rdb.Del(ctx, "bench").Err(); err != nil {
		log.Fatal(err)
	}

	start := time.Now()
	for i := 0; i < n; i++ {
		if err := rdb.XAdd(ctx, &redis.XAddArgs{
			Stream: "bench",
			Values: map[string]any{"d": payload},
		}).Err(); err != nil {
			log.Fatal(err)
		}
	}
	report("redis-streams", n, len(payload), time.Since(start))
}
