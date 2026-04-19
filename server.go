package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"minibroker/proto"
)

type Server struct {
	broker  *Broker
	cluster *Cluster // optional; when non-nil writes go through Raft

	hbTimeout time.Duration

	liveMu sync.Mutex
	live   map[*connSub]struct{}

	listenerMu sync.Mutex
	listener   net.Listener

	stopCh   chan struct{}
	stopOnce sync.Once
}

func NewServer(b *Broker) *Server {
	return &Server{
		broker: b,
		live:   make(map[*connSub]struct{}),
		stopCh: make(chan struct{}),
	}
}

// AttachCluster enables replicated writes. All PUB and COMMIT operations
// are routed through Raft. Must be called before Listen.
func (s *Server) AttachCluster(c *Cluster) { s.cluster = c }

// SetHeartbeatTimeout enables the eviction sweeper. Members that don't send
// a heartbeat within `timeout` get their connection closed.
func (s *Server) SetHeartbeatTimeout(timeout time.Duration) {
	s.hbTimeout = timeout
}

func (s *Server) Stop() {
	s.stopOnce.Do(func() { close(s.stopCh) })
	s.listenerMu.Lock()
	ln := s.listener
	s.listenerMu.Unlock()
	if ln != nil {
		_ = ln.Close()
	}
}

func (s *Server) Listen(addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	s.listenerMu.Lock()
	s.listener = ln
	s.listenerMu.Unlock()

	log.Printf("minibroker listening on %s", addr)
	if s.hbTimeout > 0 {
		go s.evictLoop()
	}
	for {
		conn, err := ln.Accept()
		if err != nil {
			select {
			case <-s.stopCh:
				return nil // normal shutdown
			default:
			}
			log.Printf("accept error: %v", err)
			continue
		}
		go s.handle(conn)
	}
}

func (s *Server) evictLoop() {
	t := time.NewTicker(s.hbTimeout / 3)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			s.evictStale()
		case <-s.stopCh:
			return
		}
	}
}

func (s *Server) evictStale() {
	cutoff := time.Now().Add(-s.hbTimeout).UnixNano()
	s.liveMu.Lock()
	var victims []*connSub
	for sub := range s.live {
		if sub.lastSeen.Load() < cutoff {
			victims = append(victims, sub)
		}
	}
	s.liveMu.Unlock()
	for _, v := range victims {
		log.Printf("evicting stale member: topic=%s group=%s", v.topic, v.group)
		body := proto.NewBuilder().String("evicted: heartbeat timeout").Build()
		_ = v.owner.writeFrame(proto.OpErr, body)
		_ = v.owner.raw.Close()
	}
}

// Per-connection subscription state. A single key "topic" maps to either an
// ephemeral single-partition iterator, or to a group membership with one
// iterator per currently-assigned partition.
type connSub struct {
	topic    string
	group    string // empty if ephemeral
	iters    map[int32]context.CancelFunc
	member   int64
	owner    *conn
	lastSeen atomic.Int64 // unix nanos of the last heartbeat
}

type conn struct {
	raw     net.Conn
	br      *bufio.Reader
	bw      *bufio.Writer
	writeMu sync.Mutex
}

func (c *conn) writeFrame(op proto.Op, body []byte) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if err := proto.WriteFrame(c.bw, op, body); err != nil {
		return err
	}
	return c.bw.Flush()
}

func (s *Server) handle(raw net.Conn) {
	defer raw.Close()
	log.Printf("client connected: %s", raw.RemoteAddr())

	c := &conn{raw: raw, br: bufio.NewReader(raw), bw: bufio.NewWriter(raw)}

	subs := map[string]*connSub{}
	var subsMu sync.Mutex

	defer func() {
		subsMu.Lock()
		for _, sub := range subs {
			for _, cancel := range sub.iters {
				cancel()
			}
			if sub.group != "" {
				s.liveMu.Lock()
				delete(s.live, sub)
				s.liveMu.Unlock()
				if t, err := s.broker.Topic(sub.topic); err == nil {
					t.Leave(sub.group, sub.member)
				}
			}
			subscribersActive.Dec()
		}
		subsMu.Unlock()
		log.Printf("client disconnected: %s", raw.RemoteAddr())
	}()

	for {
		op, body, err := proto.ReadFrame(c.br)
		if err != nil {
			return
		}

		switch op {
		case proto.OpPub:
			s.handlePub(c, body)
		case proto.OpSub:
			s.handleSub(c, body, subs, &subsMu)
		case proto.OpUnsub:
			s.handleUnsub(c, body, subs, &subsMu)
		case proto.OpCommit:
			s.handleCommit(c, body)
		case proto.OpHeartbeat:
			s.handleHeartbeat(c, body, subs, &subsMu)
		case proto.OpQuit:
			_ = c.writeFrame(proto.OpOk, nil)
			return
		default:
			s.writeErr(c, fmt.Sprintf("unknown op 0x%02x", byte(op)))
		}
	}
}

func (s *Server) writeErr(c *conn, reason string) {
	body := proto.NewBuilder().String(reason).Build()
	_ = c.writeFrame(proto.OpErr, body)
}

func (s *Server) handlePub(c *conn, body []byte) {
	p := proto.NewParser(body)
	topic, err := p.String()
	if err != nil {
		s.writeErr(c, "pub: bad topic")
		return
	}
	key, err := p.Bytes()
	if err != nil {
		s.writeErr(c, "pub: bad key")
		return
	}
	payload, err := p.Bytes()
	if err != nil {
		s.writeErr(c, "pub: bad payload")
		return
	}
	var pid int32
	var offset int64
	if s.cluster != nil {
		pid, offset, err = s.cluster.Publish(topic, key, payload)
	} else {
		pid, offset, err = s.broker.Publish(topic, key, payload)
	}
	if err != nil {
		s.writeErr(c, err.Error())
		return
	}
	reply := proto.NewBuilder().U32(uint32(pid)).U64(uint64(offset)).Build()
	_ = c.writeFrame(proto.OpOk, reply)
}

func (s *Server) handleSub(c *conn, body []byte, subs map[string]*connSub, subsMu *sync.Mutex) {
	p := proto.NewParser(body)
	topic, err := p.String()
	if err != nil {
		s.writeErr(c, "sub: bad topic")
		return
	}
	mode, err := p.Byte()
	if err != nil {
		s.writeErr(c, "sub: bad mode")
		return
	}

	subsMu.Lock()
	if _, already := subs[topic]; already {
		subsMu.Unlock()
		s.writeErr(c, "already subscribed to "+topic)
		return
	}
	subsMu.Unlock()

	t, err := s.broker.Topic(topic)
	if err != nil {
		s.writeErr(c, err.Error())
		return
	}

	switch mode {
	case proto.SubHead, proto.SubOffset:
		pid32, err := p.U32()
		if err != nil {
			s.writeErr(c, "sub: missing partition")
			return
		}
		pid := int32(pid32)
		part, err := t.Partition(pid)
		if err != nil {
			s.writeErr(c, err.Error())
			return
		}
		var from int64
		if mode == proto.SubOffset {
			off, err := p.U64()
			if err != nil {
				s.writeErr(c, "sub: missing offset")
				return
			}
			from = int64(off)
		} else {
			from = part.Head()
		}

		ctx, cancel := context.WithCancel(context.Background())
		sub := &connSub{
			topic: topic,
			iters: map[int32]context.CancelFunc{pid: cancel},
		}
		subsMu.Lock()
		subs[topic] = sub
		subsMu.Unlock()
		subscribersActive.Inc()

		reply := proto.NewBuilder().Partitions([]int32{pid}).Build()
		_ = c.writeFrame(proto.OpOk, reply)

		go s.streamPartition(c, ctx, topic, pid, from)

	case proto.SubGroup:
		group, err := p.String()
		if err != nil {
			s.writeErr(c, "sub: missing group")
			return
		}
		sub := &connSub{
			topic: topic,
			group: group,
			iters: make(map[int32]context.CancelFunc),
			owner: c,
		}
		sub.lastSeen.Store(time.Now().UnixNano())

		// Join the group with a callback that receives rebalance notifications.
		// The callback serializes its work through subsMu so it's safe vs.
		// concurrent UNSUB or disconnect cleanup.
		var initialSet bool
		onChange := func(assignment []int32) {
			subsMu.Lock()
			if _, stillMember := subs[topic]; !stillMember && initialSet {
				// we've been removed; ignore late notifications
				subsMu.Unlock()
				return
			}
			// Stop iterators for partitions no longer assigned.
			want := map[int32]bool{}
			for _, p := range assignment {
				want[p] = true
			}
			for pid, cancel := range sub.iters {
				if !want[pid] {
					cancel()
					delete(sub.iters, pid)
				}
			}
			// Start iterators for newly-assigned partitions.
			for _, pid := range assignment {
				if _, running := sub.iters[pid]; running {
					continue
				}
				ctx, cancel := context.WithCancel(context.Background())
				sub.iters[pid] = cancel
				from := resolveGroupStart(t, pid, group)
				go s.streamPartition(c, ctx, topic, pid, from)
			}
			subsMu.Unlock()

			// Notify the client of its new assignment (not for the very first
			// call because we bundle that into the SUB OK reply below).
			if initialSet {
				body := proto.NewBuilder().String(topic).String(group).Partitions(assignment).Build()
				_ = c.writeFrame(proto.OpRebalance, body)
			}
		}

		memberID, initial := t.Join(group, onChange)
		sub.member = memberID

		// Register the subscription BEFORE the initial iterator-start so
		// subsequent onChange callbacks see us as still a member.
		subsMu.Lock()
		subs[topic] = sub
		subsMu.Unlock()

		s.liveMu.Lock()
		s.live[sub] = struct{}{}
		s.liveMu.Unlock()
		subscribersActive.Inc()

		// Perform the "initial" assignment work that the first Join-triggered
		// onChange already did (it ran with initialSet=false) — but it did
		// start the iterators. Mark initialSet so future calls push REBALANCE.
		initialSet = true

		reply := proto.NewBuilder().String(group).Partitions(initial).Build()
		_ = c.writeFrame(proto.OpOk, reply)

	default:
		s.writeErr(c, fmt.Sprintf("sub: unknown mode %d", mode))
	}
}

func resolveGroupStart(t *Topic, pid int32, group string) int64 {
	if off, ok := t.GroupPartitionOffset(group, pid); ok {
		return off
	}
	return 0 // new groups start at earliest
}

func (s *Server) streamPartition(c *conn, ctx context.Context, topic string, pid int32, from int64) {
	err := s.broker.Iterate(ctx, topic, pid, from, func(offset int64, key, payload []byte) bool {
		body := proto.NewBuilder().
			String(topic).
			U32(uint32(pid)).
			U64(uint64(offset)).
			Bytes(key).
			Bytes(payload).
			Build()
		if werr := c.writeFrame(proto.OpMsg, body); werr != nil {
			return false
		}
		return true
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		s.writeErr(c, fmt.Sprintf("iterate %s/%d: %s", topic, pid, err.Error()))
	}
}

func (s *Server) handleUnsub(c *conn, body []byte, subs map[string]*connSub, subsMu *sync.Mutex) {
	p := proto.NewParser(body)
	topic, err := p.String()
	if err != nil {
		s.writeErr(c, "unsub: bad topic")
		return
	}
	subsMu.Lock()
	sub, ok := subs[topic]
	if !ok {
		subsMu.Unlock()
		s.writeErr(c, "not-subscribed: "+topic)
		return
	}
	delete(subs, topic)
	for _, cancel := range sub.iters {
		cancel()
	}
	subsMu.Unlock()

	if sub.group != "" {
		s.liveMu.Lock()
		delete(s.live, sub)
		s.liveMu.Unlock()
		if t, err := s.broker.Topic(topic); err == nil {
			t.Leave(sub.group, sub.member)
		}
	}
	subscribersActive.Dec()

	_ = c.writeFrame(proto.OpOk, nil)
}

func (s *Server) handleCommit(c *conn, body []byte) {
	p := proto.NewParser(body)
	topic, err := p.String()
	if err != nil {
		s.writeErr(c, "commit: bad topic")
		return
	}
	group, err := p.String()
	if err != nil {
		s.writeErr(c, "commit: bad group")
		return
	}
	pid32, err := p.U32()
	if err != nil {
		s.writeErr(c, "commit: bad partition")
		return
	}
	offset64, err := p.U64()
	if err != nil {
		s.writeErr(c, "commit: bad offset")
		return
	}
	if s.cluster != nil {
		if err := s.cluster.CommitGroup(topic, group, int32(pid32), int64(offset64)); err != nil {
			s.writeErr(c, err.Error())
			return
		}
	} else {
		t, err := s.broker.Topic(topic)
		if err != nil {
			s.writeErr(c, err.Error())
			return
		}
		if err := t.Commit(group, int32(pid32), int64(offset64)); err != nil {
			s.writeErr(c, err.Error())
			return
		}
	}
	reply := proto.NewBuilder().U64(offset64).Build()
	_ = c.writeFrame(proto.OpOk, reply)
}

func (s *Server) handleHeartbeat(c *conn, body []byte, subs map[string]*connSub, subsMu *sync.Mutex) {
	p := proto.NewParser(body)
	topic, err := p.String()
	if err != nil {
		s.writeErr(c, "heartbeat: bad topic")
		return
	}
	group, err := p.String()
	if err != nil {
		s.writeErr(c, "heartbeat: bad group")
		return
	}
	subsMu.Lock()
	sub, ok := subs[topic]
	subsMu.Unlock()
	if !ok || sub.group != group {
		s.writeErr(c, "heartbeat: not-subscribed: "+topic)
		return
	}
	sub.lastSeen.Store(time.Now().UnixNano())
	_ = c.writeFrame(proto.OpOk, nil)
}

// Unused helper referenced via io package to avoid unused-import errors when
// we iterate manually elsewhere. (kept so build stays stable across refactors)
var _ = io.EOF
