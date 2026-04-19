package client

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"minibroker/proto"
)

// Handler receives a single message.
type Handler func(topic string, partition int32, offset int64, payload []byte)

// RebalanceHandler is invoked when a group's partition assignment changes.
// It's optional — pass nil if you don't need it.
type RebalanceHandler func(topic, group string, assignment []int32)

type msgEvent struct {
	partition int32
	offset    int64
	payload   []byte
}

type subscription struct {
	handler    Handler
	onRebal    RebalanceHandler
	group      string   // empty for ephemeral
	partitions []int32  // ephemeral: the one partition; group: current assignment
	// ephemeral subs track lastOffset per partition for reconnect
	from       int64 // originally requested (-1 = head)
	lastOffset map[int32]int64
	fresh      bool
	ch         chan msgEvent
	cancel     chan struct{}
}

func newSubscription(h Handler, onRebal RebalanceHandler, group string, from int64) *subscription {
	return &subscription{
		handler:    h,
		onRebal:    onRebal,
		group:      group,
		from:       from,
		fresh:      true,
		lastOffset: make(map[int32]int64),
		ch:         make(chan msgEvent, 256),
		cancel:     make(chan struct{}),
	}
}

func (s *subscription) run(topic string) {
	for {
		select {
		case evt := <-s.ch:
			s.handler(topic, evt.partition, evt.offset, evt.payload)
		case <-s.cancel:
			return
		}
	}
}

type Client struct {
	addr string

	cmdMu sync.Mutex // serializes request/response pairs

	connMu sync.Mutex
	conn   net.Conn
	bw     *bufio.Writer
	reply  chan replyFrame
	done   chan struct{}

	subMu sync.Mutex
	subs  map[string]*subscription

	closeOnce sync.Once
	closed    chan struct{}
}

type replyFrame struct {
	op   proto.Op
	body []byte
}

func Dial(addr string) (*Client, error) {
	c := &Client{
		addr:   addr,
		subs:   make(map[string]*subscription),
		closed: make(chan struct{}),
	}
	if err := c.connect(); err != nil {
		return nil, err
	}
	go c.supervise()
	return c, nil
}

func (c *Client) connect() error {
	conn, err := net.Dial("tcp", c.addr)
	if err != nil {
		return err
	}
	c.connMu.Lock()
	c.conn = conn
	c.bw = bufio.NewWriter(conn)
	c.reply = make(chan replyFrame, 1)
	c.done = make(chan struct{})
	doneCh := c.done
	replyCh := c.reply
	c.connMu.Unlock()
	go c.readLoop(conn, doneCh, replyCh)
	return nil
}

func (c *Client) readLoop(conn net.Conn, done chan struct{}, reply chan replyFrame) {
	defer close(done)
	br := bufio.NewReader(conn)
	for {
		op, body, err := proto.ReadFrame(br)
		if err != nil {
			return
		}
		switch op {
		case proto.OpMsg:
			c.dispatchMsg(body)
		case proto.OpRebalance:
			c.dispatchRebalance(body)
		default:
			select {
			case reply <- replyFrame{op: op, body: body}:
			default:
			}
		}
	}
}

func (c *Client) dispatchMsg(body []byte) {
	p := proto.NewParser(body)
	topic, err := p.String()
	if err != nil {
		return
	}
	pid32, err := p.U32()
	if err != nil {
		return
	}
	pid := int32(pid32)
	off, err := p.U64()
	if err != nil {
		return
	}
	payload, err := p.Bytes()
	if err != nil {
		return
	}
	c.subMu.Lock()
	sub := c.subs[topic]
	if sub != nil {
		sub.lastOffset[pid] = int64(off)
		sub.fresh = false
	}
	c.subMu.Unlock()
	if sub != nil {
		select {
		case sub.ch <- msgEvent{partition: pid, offset: int64(off), payload: payload}:
		case <-sub.cancel:
		}
	}
}

func (c *Client) dispatchRebalance(body []byte) {
	p := proto.NewParser(body)
	topic, err := p.String()
	if err != nil {
		return
	}
	group, err := p.String()
	if err != nil {
		return
	}
	parts, err := p.Partitions()
	if err != nil {
		return
	}
	c.subMu.Lock()
	sub := c.subs[topic]
	if sub != nil {
		sub.partitions = parts
	}
	c.subMu.Unlock()
	if sub != nil && sub.onRebal != nil {
		sub.onRebal(topic, group, parts)
	}
}

func (c *Client) supervise() {
	for {
		c.connMu.Lock()
		done := c.done
		c.connMu.Unlock()
		select {
		case <-done:
		case <-c.closed:
			return
		}
		select {
		case <-c.closed:
			return
		default:
		}
		for {
			select {
			case <-c.closed:
				return
			default:
			}
			if err := c.connect(); err == nil {
				break
			}
			time.Sleep(500 * time.Millisecond)
		}
		c.resubscribe()
	}
}

func (c *Client) resubscribe() {
	c.subMu.Lock()
	type todo struct {
		topic     string
		group     string
		partition int32
		from      int64
	}
	var list []todo
	for topic, sub := range c.subs {
		if sub.group != "" {
			list = append(list, todo{topic: topic, group: sub.group})
		} else {
			// ephemeral: single partition
			var pid int32
			if len(sub.partitions) > 0 {
				pid = sub.partitions[0]
			}
			from := sub.from
			if !sub.fresh {
				if last, ok := sub.lastOffset[pid]; ok {
					from = last + 1
				}
			}
			list = append(list, todo{topic: topic, partition: pid, from: from})
		}
	}
	c.subMu.Unlock()

	for _, it := range list {
		var body []byte
		if it.group != "" {
			body = proto.NewBuilder().String(it.topic).Byte(proto.SubGroup).String(it.group).Build()
		} else if it.from < 0 {
			body = proto.NewBuilder().String(it.topic).Byte(proto.SubHead).U32(uint32(it.partition)).Build()
		} else {
			body = proto.NewBuilder().String(it.topic).Byte(proto.SubOffset).U32(uint32(it.partition)).U64(uint64(it.from)).Build()
		}
		if _, err := c.request(proto.OpSub, body); err != nil {
			return
		}
	}
}

func (c *Client) request(op proto.Op, body []byte) (replyFrame, error) {
	c.cmdMu.Lock()
	defer c.cmdMu.Unlock()

	c.connMu.Lock()
	bw := c.bw
	reply := c.reply
	done := c.done
	c.connMu.Unlock()
	if bw == nil {
		return replyFrame{}, errors.New("not connected")
	}

	if err := proto.WriteFrame(bw, op, body); err != nil {
		return replyFrame{}, err
	}
	if err := bw.Flush(); err != nil {
		return replyFrame{}, err
	}
	select {
	case r := <-reply:
		return r, nil
	case <-done:
		return replyFrame{}, errors.New("connection closed")
	case <-c.closed:
		return replyFrame{}, errors.New("client closed")
	}
}

func (c *Client) expectOk(r replyFrame) error {
	switch r.op {
	case proto.OpOk:
		return nil
	case proto.OpErr:
		p := proto.NewParser(r.body)
		s, _ := p.String()
		return errors.New(s)
	default:
		return fmt.Errorf("unexpected op 0x%02x", byte(r.op))
	}
}

// Publish routes the message to a partition using the given key (nil for
// round-robin) and returns the assigned partition and offset.
func (c *Client) Publish(topic string, key, payload []byte) (int32, int64, error) {
	body := proto.NewBuilder().String(topic).Bytes(key).Bytes(payload).Build()
	r, err := c.request(proto.OpPub, body)
	if err != nil {
		return 0, 0, err
	}
	if err := c.expectOk(r); err != nil {
		return 0, 0, err
	}
	p := proto.NewParser(r.body)
	pid, _ := p.U32()
	off, _ := p.U64()
	return int32(pid), int64(off), nil
}

// Subscribe attaches to a single partition of a topic. from=-1 starts at the
// partition's head; from>=0 starts from the given offset.
func (c *Client) Subscribe(topic string, partition int32, from int64, handler Handler) error {
	c.subMu.Lock()
	if _, exists := c.subs[topic]; exists {
		c.subMu.Unlock()
		return fmt.Errorf("already subscribed to %s", topic)
	}
	sub := newSubscription(handler, nil, "", from)
	sub.partitions = []int32{partition}
	c.subs[topic] = sub
	c.subMu.Unlock()
	go sub.run(topic)

	var body []byte
	if from < 0 {
		body = proto.NewBuilder().String(topic).Byte(proto.SubHead).U32(uint32(partition)).Build()
	} else {
		body = proto.NewBuilder().String(topic).Byte(proto.SubOffset).U32(uint32(partition)).U64(uint64(from)).Build()
	}
	r, err := c.request(proto.OpSub, body)
	if err != nil {
		c.removeSub(topic)
		return err
	}
	if err := c.expectOk(r); err != nil {
		c.removeSub(topic)
		return err
	}
	return nil
}

// SubscribeGroup joins a consumer group on the topic. The server chooses the
// partitions assigned to this member. If onRebal is non-nil, it's invoked
// both on initial assignment and on every rebalance.
func (c *Client) SubscribeGroup(topic, group string, onRebal RebalanceHandler, handler Handler) error {
	if group == "" {
		return errors.New("group name cannot be empty")
	}
	c.subMu.Lock()
	if _, exists := c.subs[topic]; exists {
		c.subMu.Unlock()
		return fmt.Errorf("already subscribed to %s", topic)
	}
	sub := newSubscription(handler, onRebal, group, -1)
	c.subs[topic] = sub
	c.subMu.Unlock()
	go sub.run(topic)

	body := proto.NewBuilder().String(topic).Byte(proto.SubGroup).String(group).Build()
	r, err := c.request(proto.OpSub, body)
	if err != nil {
		c.removeSub(topic)
		return err
	}
	if err := c.expectOk(r); err != nil {
		c.removeSub(topic)
		return err
	}
	// The SUB OK for groups carries: [string group][u32 count][u32 p0]...
	p := proto.NewParser(r.body)
	_, _ = p.String()
	parts, _ := p.Partitions()
	c.subMu.Lock()
	sub.partitions = parts
	c.subMu.Unlock()
	if onRebal != nil {
		onRebal(topic, group, parts)
	}
	return nil
}

func (c *Client) Commit(topic, group string, partition int32, offset int64) error {
	body := proto.NewBuilder().
		String(topic).
		String(group).
		U32(uint32(partition)).
		U64(uint64(offset)).
		Build()
	r, err := c.request(proto.OpCommit, body)
	if err != nil {
		return err
	}
	return c.expectOk(r)
}

func (c *Client) Unsubscribe(topic string) error {
	body := proto.NewBuilder().String(topic).Build()
	r, err := c.request(proto.OpUnsub, body)
	if err != nil {
		return err
	}
	if err := c.expectOk(r); err != nil {
		return err
	}
	c.removeSub(topic)
	return nil
}

func (c *Client) removeSub(topic string) {
	c.subMu.Lock()
	sub, ok := c.subs[topic]
	if ok {
		delete(c.subs, topic)
	}
	c.subMu.Unlock()
	if ok {
		select {
		case <-sub.cancel:
		default:
			close(sub.cancel)
		}
	}
}

func (c *Client) Close() error {
	c.closeOnce.Do(func() { close(c.closed) })

	c.subMu.Lock()
	for _, sub := range c.subs {
		select {
		case <-sub.cancel:
		default:
			close(sub.cancel)
		}
	}
	c.subs = make(map[string]*subscription)
	c.subMu.Unlock()

	c.connMu.Lock()
	conn := c.conn
	c.connMu.Unlock()
	if conn != nil {
		return conn.Close()
	}
	return nil
}
