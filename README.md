# minibroker

A small, learning-oriented message broker in Go — inspired by NATS, Kafka,
Redis Streams, and RabbitMQ — built to understand how such systems work
under the hood. **Not production-ready**; it's a teaching toy.

## What it does

- **Persistent, append-only log** per partition on disk, with **CRC32** on
  every record
- **Segmentation** — segment files roll after N records
- **Retention** — segment-count based (`-retain`) and time based
  (`-retain-for`); a background sweeper trims old sealed segments
- **Partitions per topic** with keyed or round-robin publish routing
- **Consumer groups** with server-tracked, per-partition committed offsets
- **Automatic rebalance** — when group members join or leave, partitions are
  redistributed and affected clients receive `REBALANCE` notifications
- **Heartbeats + session timeouts** — stuck group members get evicted after
  `-heartbeat-timeout`
- **Log compaction by key** — `-compact-every` rewrites sealed segments
  keeping only the latest record per key (preserving the offset space)
- **Raft replication across N nodes** (via `hashicorp/raft`) — writes go
  through a leader; followers serve reads from the replicated local state
- **Binary wire protocol** — payloads can contain arbitrary bytes
- **Go client library** with auto-reconnect, heartbeats, and per-subscription
  delivery goroutines

## Wire protocol

Every frame:

```
[u32 total-len][u8 op][op-specific body]
```

Strings are encoded as `[u16 len][bytes]`, byte slices as `[u32 len][bytes]`,
offsets are `u64`, partition ids are `u32`. All integers big-endian.

| op        | direction | purpose                                                      |
|-----------|-----------|--------------------------------------------------------------|
| PUB       | c→s       | publish to a topic (routed by key or round-robin)            |
| SUB       | c→s       | subscribe to a partition (head/offset) or join a group       |
| UNSUB     | c→s       | leave a subscription                                         |
| COMMIT    | c→s       | commit an offset for (topic, group, partition)               |
| QUIT      | c→s       | end session                                                  |
| HEARTBEAT | c→s       | group-member liveness ping                                   |
| OK        | s→c       | ack for a request                                            |
| ERR       | s→c       | error reply                                                  |
| MSG       | s→c       | server push: a record for a subscribed topic/partition       |
| REBALANCE | s→c       | group's partition assignment changed                         |

Record format on disk:

```
[u32 body_len]
[u64 offset]                   # stored so compaction can preserve offsets
[u16 key_len][key bytes]
[u32 payload_len][payload bytes]
[u32 crc32]                    # IEEE over offset || key_len || key || payload_len || payload
```

## Disk layout

```
data/
  <topic>/
    part-0/
      00000000000000000000.log     # records: [u32 len][payload]
      00000000000000001000.log
    part-1/
      ...
    offsets/
      <group>/
        0.off                      # committed offset for (group, partition 0)
        1.off
```

## Running

```sh
make build
./minibroker -partitions 4
```

Flags:

| flag                  | default | meaning                                                              |
|-----------------------|---------|----------------------------------------------------------------------|
| `-addr`               | `:4222` | TCP listen address                                                   |
| `-dir`                | `./data`| data directory                                                       |
| `-partitions`         | `4`     | partitions per topic (global for this broker)                        |
| `-segment-size`       | `1000`  | records per segment before rolling                                   |
| `-retain`             | `100`   | total segments kept per partition (count-based retention)            |
| `-retain-for`         | `0`     | time-based retention: drop sealed segments older than this (0 off)   |
| `-sweep-every`        | `30s`   | how often the time-retention sweep runs                              |
| `-heartbeat-timeout`  | `15s`   | evict a group member that hasn't heartbeat'd in this window          |
| `-compact-every`      | `0`     | run log compaction at this interval (0 disables)                     |
| `-cluster-id`         | ``      | this node's id (enables Raft replication when set)                   |
| `-cluster-addr`       | ``      | this node's Raft RPC address, e.g. `:8001`                           |
| `-cluster-peers`      | ``      | comma-separated peer list `id@addr`                                  |
| `-cluster-bootstrap`  | `false` | true on exactly one node the first time the cluster is created       |

### Running a 3-node cluster

```sh
./examples/cluster/run-cluster.sh
```

This starts three `minibroker` processes on ports 4221/4222/4223 (broker) and
8001/8002/8003 (Raft), waits for an election, publishes through the leader,
and confirms a follower received the replicated messages.

## Docker

Multi-stage Dockerfile (distroless runtime, ~12 MB final image) and two
compose files — one for single-node, one for the 3-node cluster.

| target                    | what it does                                                  |
|---------------------------|---------------------------------------------------------------|
| `make docker-build`       | build the local `minibroker:latest` image                     |
| `make docker-up`          | single broker on `localhost:4222`, named volume `broker-data` |
| `make docker-down`        | stop the single broker (keeps the data volume)                |
| `make docker-cluster-up`  | 3-node Raft cluster, brokers on :4221 / :4222 / :4223         |
| `make docker-cluster-down`| stop the cluster AND wipe volumes (fresh bootstrap next time) |
| `make docker-clean`       | remove both compose stacks + the image                        |

Once the cluster is up, `go run ./examples/cluster` probes the three ports,
finds the leader, publishes through it, and reads the replicated records
back from a follower.

## Using the Go client

```go
package main

import (
	"fmt"
	"log"

	"minibroker/client"
)

func main() {
	c, err := client.Dial("localhost:4222")
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	// Publish — nil key means round-robin across partitions.
	partition, offset, err := c.Publish("events", nil, []byte("hello"))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("wrote partition=%d offset=%d\n", partition, offset)

	// Join a consumer group — the broker picks our partitions and sends
	// us a REBALANCE whenever the assignment changes.
	err = c.SubscribeGroup("events", "workers",
		func(topic, group string, assignment []int32) {
			fmt.Println("my partitions:", assignment)
		},
		func(topic string, pid int32, offset int64, payload []byte) {
			fmt.Printf("[%s/%d@%d] %s\n", topic, pid, offset, payload)
			// Commit next-offset-to-read (Kafka convention).
			_ = c.Commit(topic, "workers", pid, offset+1)
		})
	if err != nil {
		log.Fatal(err)
	}

	select {} // run until killed
}
```

For a single-partition, ephemeral subscription (no group), use
`c.Subscribe(topic, partition, fromOffset, handler)` — pass `fromOffset=-1`
to start at the partition's current head, or `0` to replay from the
beginning.

## Demos

Each demo expects a broker listening on `localhost:4222`. Start one in a
second terminal with the hint next to each target.

| target                  | what it shows                                                        | broker hint                          |
|-------------------------|----------------------------------------------------------------------|--------------------------------------|
| `make demo`             | basic pub/sub, replay from offset, unsubscribe                       | `make run`                           |
| `make demo-reconnect`   | client survives a broker restart                                     | `make run` (then kill + restart)     |
| `make demo-segments`    | segmentation and retention                                           | `make run-segments`                  |
| `make demo-groups`      | consumer group resumes from a committed offset across sessions       | `make run`                           |
| `make demo-partitions`  | two workers in a group split four partitions; binary-safe payloads   | `make run-partitions`                |
| `make demo-heartbeat`   | well-behaved client persists; stuck raw socket gets evicted          | `make run-heartbeat`                 |
| `make demo-cluster`     | spins up 3 nodes, publishes via leader, reads from follower          | (script starts the cluster itself)   |

## Project layout

```
minibroker/
├── proto/            binary framing + op codes
├── partition.go      single-partition segmented log
├── topic.go          N partitions + consumer-group state + rebalance
├── broker.go         topic registry + publish routing
├── server.go         TCP server; handles all ops; manages group iterators
├── main.go           flags and entrypoint
├── client/           Go client library (Dial, Publish, Subscribe, ...)
└── examples/         five runnable demos
```

## Things this doesn't do (on purpose, for now)

- No TLS, no auth, no ACLs.
- No per-topic configuration (partition count is a global broker flag).
- Size-based retention (byte budget) — only count and time based.
- Publishers do not batch; each PUB is one fsync (and one Raft round-trip
  when clustered).
- Raft log / stable store lives in memory; only snapshots hit disk. A node
  restart currently replays from snapshot + in-memory log and expects peers
  to fill gaps. Swap in `raftboltdb` for durable Raft state.
- No consumer-group session IDs beyond the in-server memberID; if a client
  reconnects quickly enough, its previous slot may still linger until the
  heartbeat timeout fires.
