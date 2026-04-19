# minibroker Go client

The Go client library lives in [`client/`](client/) and is imported as
`minibroker/client`. It speaks the binary wire protocol over TCP, survives
broker restarts and leader changes, and exposes a small, composable API.

## Install

```go
import "minibroker/client"
```

(The module is `minibroker`; if you're using this outside the repo, vendor
or rewrite imports accordingly.)

## At a glance

```go
c, err := client.Dial("localhost:4222")
if err != nil { log.Fatal(err) }
defer c.Close()

partition, offset, _ := c.Publish("events", nil, []byte("hello"))
_ = c.Subscribe("events", partition, 0,
    func(topic string, pid int32, offset int64, key, payload []byte) {
        log.Printf("[%s/%d@%d] %s", topic, pid, offset, payload)
    })
```

## Connecting

```go
// Single broker
c, err := client.Dial("localhost:4222")

// Multiple nodes: the client rotates through them on "not leader"
// errors. Subscribe and reads stay on whichever node answered first.
c, err := client.Dial("n1:4222", "n2:4222", "n3:4222")
```

`Dial` establishes a TCP connection, starts a background read loop, and
spawns a supervisor goroutine that reconnects with linear backoff if the
connection drops.

## Publishing

```go
partition, offset, err := c.Publish(topic string, key, payload []byte)
```

- `topic` — must match `^[a-zA-Z0-9._-]+$`.
- `key` — used for deterministic partition routing (FNV-1a hash mod
  `-partitions`). Pass `nil` for round-robin.
- `payload` — arbitrary bytes.

Returns the partition chosen and the record's offset.

If the broker is a Raft follower, the client transparently rotates to the
next address in its `Dial` list and retries (up to `len(addrs) + 1` times).
A bare single-address client will just return the `"not leader; leader=X"`
error after exhausting retries.

## Subscribing (ephemeral)

```go
err := c.Subscribe(topic string, partition int32, from int64, handler)
```

- `partition` — which partition to read from. Topics currently have a
  broker-wide number of partitions set by `-partitions`.
- `from`:
  - `-1` — start at the partition's current head (new messages only)
  - `0`  — replay from the earliest retained record
  - `N>=0` — start at offset N (errors with `"out of range"` if retention
    has dropped that offset)

```go
handler := func(topic string, pid int32, offset int64, key, payload []byte) {
    fmt.Printf("%s/%d@%d: %s=%s\n", topic, pid, offset, key, payload)
}
```

The handler runs on a dedicated per-subscription goroutine with a 256-msg
buffer in front of it. Slow handlers apply backpressure through the buffer;
they do *not* block other subscriptions.

## Subscribing as a consumer group

Consumer groups load-balance partitions across members. When a member joins
or leaves, the broker redistributes partitions and notifies every remaining
member through the optional rebalance callback.

```go
onRebalance := func(topic, group string, assignment []int32) {
    fmt.Printf("my partitions now: %v\n", assignment)
}

handler := func(topic string, pid int32, offset int64, key, payload []byte) {
    process(payload)
    // Commit the NEXT offset to read — Kafka convention.
    _ = c.Commit(topic, "workers", pid, offset+1)
}

err := c.SubscribeGroup("events", "workers", onRebalance, handler)
```

`onRebalance` fires:
- Once synchronously during `SubscribeGroup` with the initial assignment.
- Again asynchronously via the server's `REBALANCE` frame whenever the set
  changes (another member joined, one got evicted, one unsubscribed).

Pass `nil` for `onRebalance` if you don't care.

### Heartbeats

`SubscribeGroup` spawns a background heartbeat goroutine that sends a
`HEARTBEAT` every 5 seconds by default. The server (default timeout 15s)
evicts members that haven't ping'd in that window by closing their TCP
connection; the supervisor then reconnects and the client re-joins the
group automatically.

Tune the interval before joining:

```go
client.SetHeartbeatInterval(2 * time.Second)
```

## Committing offsets

```go
err := c.Commit(topic, group string, partition int32, offset int64)
```

The convention is that `offset` is the **next** offset to consume —
typically `receivedOffset + 1`. When another member later rejoins the same
group, the broker serves it from the committed offset.

Commits land in `<datadir>/<topic>/offsets/<group>/<partition>.off`
atomically (write-temp + rename). In cluster mode they go through Raft.

## Unsubscribing

```go
err := c.Unsubscribe(topic string)
```

Cancels the iterator for that topic on the server, deletes the
subscription locally, and shuts down the handler goroutine. Group members
also leave their group (triggering a rebalance for the remaining
members).

## Closing

```go
err := c.Close()
```

Closes the TCP connection, stops the supervisor, and cancels every
subscription. Safe to call multiple times.

## Handler signature and ordering

```go
type Handler func(topic string, partition int32, offset int64, key, payload []byte)
```

- `topic` — echoed back for convenience (useful if one handler is shared
  across topics).
- `partition` — the partition this record came from.
- `offset` — the record's assigned offset. For compacted topics, offsets
  can have gaps (compaction removes superseded records but keeps their
  offsets intact).
- `key` — may be `nil` if the publisher didn't set one.
- `payload` — arbitrary bytes.

**Ordering guarantee:** records within a single partition arrive in
offset order. Across partitions, there is no global ordering — that's the
trade-off for partition parallelism.

## Error handling

- Most methods return errors as plain `error`. Check them.
- `"already subscribed to <topic>"` — you called `Subscribe` twice for the
  same topic on one client; unsubscribe first.
- `"offset N out of range (oldest=M)"` — retention dropped the requested
  offset before you could read it. Resubscribe with `from >= M` or
  accept the data loss.
- `"not leader; leader=X"` — only surfaced after the client has exhausted
  its address list. If you passed multiple nodes to `Dial`, the client
  already retried them all.
- `"evicted: heartbeat timeout"` — the server kicked this connection for
  missing heartbeats. The supervisor will reconnect and re-subscribe.

## Full example

```go
package main

import (
    "fmt"
    "log"
    "time"

    "minibroker/client"
)

func main() {
    c, err := client.Dial("n1:4222", "n2:4222", "n3:4222")
    if err != nil {
        log.Fatal(err)
    }
    defer c.Close()

    // Publish 10 records with keys so compaction can work later.
    for i := 0; i < 10; i++ {
        key := fmt.Appendf(nil, "user-%d", i%3)   // three keys
        val := fmt.Appendf(nil, "event-%d", i)
        _, _, err := c.Publish("events", key, val)
        if err != nil {
            log.Fatal(err)
        }
    }

    // Join a group. Commit each offset as we process it.
    done := make(chan struct{})
    seen := 0
    err = c.SubscribeGroup("events", "workers",
        func(topic, group string, assignment []int32) {
            log.Printf("assigned partitions: %v", assignment)
        },
        func(topic string, pid int32, off int64, key, payload []byte) {
            fmt.Printf("[%s/%d@%d] %s=%s\n", topic, pid, off, key, payload)
            _ = c.Commit(topic, "workers", pid, off+1)
            seen++
            if seen == 10 {
                close(done)
            }
        })
    if err != nil {
        log.Fatal(err)
    }

    select {
    case <-done:
    case <-time.After(5 * time.Second):
        log.Fatal("timeout")
    }
}
```

## Limitations to know

- The client keeps a single connection open at a time. Subscribe-heavy
  workloads should use one client per subscription if you want isolation
  between slow handlers.
- No batched publish — each `Publish` is a round-trip.
- No QoS / delivery guarantees: messages are at-least-once within a
  group (re-delivery is possible if you don't commit in time); within a
  single connection, the server delivers every MSG frame it has written,
  and TCP handles in-order delivery.
- Group members compete for partitions — if two clients in the same
  group subscribe to a 1-partition topic, only one reads at a time.
