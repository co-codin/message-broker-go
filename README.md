# minibroker

A small, learning-oriented message broker in Go — inspired by NATS, Kafka,
Redis Streams, and RabbitMQ — built to understand how such systems work
under the hood. **Not production-ready**; it's a teaching toy.

## What it does

- **Persistent, append-only log** per partition on disk
- **Segmentation + retention** — segment files roll after N records; oldest
  segments are deleted
- **Partitions per topic** with keyed or round-robin publish routing
- **Consumer groups** with server-tracked, per-partition committed offsets
- **Automatic rebalance** — when group members join or leave, partitions are
  redistributed and affected clients receive `REBALANCE` notifications
- **Binary wire protocol** — payloads can contain arbitrary bytes (newlines,
  spaces, NULs)
- **Go client library** with auto-reconnect and per-subscription delivery
  goroutines

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
| OK        | s→c       | ack for a request                                            |
| ERR       | s→c       | error reply                                                  |
| MSG       | s→c       | server push: a record for a subscribed topic/partition       |
| REBALANCE | s→c       | group's partition assignment changed                         |

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

| flag           | default | meaning                                        |
|----------------|---------|------------------------------------------------|
| `-addr`        | `:4222` | TCP listen address                             |
| `-dir`         | `./data`| data directory                                 |
| `-partitions`  | `4`     | partitions per topic (global for this broker)  |
| `-segment-size`| `1000`  | records per segment before rolling             |
| `-retain`      | `100`   | total segments kept per partition              |

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

- No replication. A single broker is the single point of failure.
- No heartbeats or session timeouts. A slow group member keeps its partitions
  until the TCP connection breaks.
- No TLS, no auth, no ACLs.
- No per-topic configuration (partition count is a global broker flag).
- Retention is segment-count based, not time or size.
- Publishers do not batch; each PUB is one fsync.
