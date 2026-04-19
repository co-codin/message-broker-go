package main

import (
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"

	"minibroker/proto"
)

// Cluster wraps a hashicorp/raft node and exposes just enough surface for
// the broker's write path (Publish, Commit) to go through consensus when
// clustering is enabled.
type Cluster struct {
	raft *raft.Raft
	fsm  *brokerFSM
}

type ClusterConfig struct {
	NodeID    string   // unique id for this node (also used as Raft ServerID)
	BindAddr  string   // this node's Raft TCP transport address (host:port)
	Peers     []string // full member list at bootstrap time (host:port each)
	Bootstrap bool     // only one node in the initial cluster should set this
	DataDir   string   // where raft snapshots live
}

// NewCluster creates a Raft node bound to `broker` as its state machine.
// Logs and stable state are kept in-memory for this learning implementation;
// snapshots are on disk. Good enough to demonstrate replication + election;
// a production deploy would use raftboltdb for durability.
func NewCluster(broker *Broker, cfg ClusterConfig) (*Cluster, error) {
	if cfg.NodeID == "" {
		return nil, errors.New("cluster: NodeID required")
	}
	if cfg.BindAddr == "" {
		return nil, errors.New("cluster: BindAddr required")
	}
	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		return nil, err
	}

	raftCfg := raft.DefaultConfig()
	raftCfg.LocalID = raft.ServerID(cfg.NodeID)

	tcpAddr, err := net.ResolveTCPAddr("tcp", cfg.BindAddr)
	if err != nil {
		return nil, fmt.Errorf("resolve raft addr: %w", err)
	}
	transport, err := raft.NewTCPTransport(cfg.BindAddr, tcpAddr, 3, 5*time.Second, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("tcp transport: %w", err)
	}

	// Durable Raft log + stable state in BoltDB so a node restart can rejoin
	// without losing its in-flight log entries or vote state.
	boltPath := filepath.Join(cfg.DataDir, "raft.bolt")
	boltStore, err := raftboltdb.NewBoltStore(boltPath)
	if err != nil {
		return nil, fmt.Errorf("bolt store: %w", err)
	}
	logs := raft.LogStore(boltStore)
	stable := raft.StableStore(boltStore)
	snaps, err := raft.NewFileSnapshotStore(cfg.DataDir, 2, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("snapshot store: %w", err)
	}

	fsm := &brokerFSM{broker: broker}
	r, err := raft.NewRaft(raftCfg, fsm, logs, stable, snaps, transport)
	if err != nil {
		return nil, fmt.Errorf("raft.NewRaft: %w", err)
	}

	if cfg.Bootstrap {
		servers := make([]raft.Server, 0, len(cfg.Peers))
		for _, p := range cfg.Peers {
			// peer spec: "id@addr" — id becomes the ServerID.
			id, addr, _ := strings.Cut(p, "@")
			if addr == "" {
				addr = id
			}
			servers = append(servers, raft.Server{
				ID:      raft.ServerID(id),
				Address: raft.ServerAddress(addr),
			})
		}
		if err := r.BootstrapCluster(raft.Configuration{Servers: servers}).Error(); err != nil &&
			!errors.Is(err, raft.ErrCantBootstrap) {
			return nil, fmt.Errorf("bootstrap: %w", err)
		}
	}
	return &Cluster{raft: r, fsm: fsm}, nil
}

func (c *Cluster) IsLeader() bool          { return c.raft.State() == raft.Leader }
func (c *Cluster) LeaderAddress() string   { return string(c.raft.Leader()) }
func (c *Cluster) State() raft.RaftState   { return c.raft.State() }

// Shutdown stops the Raft node and flushes any in-flight writes.
func (c *Cluster) Shutdown() error { return c.raft.Shutdown().Error() }

// Command types stored in the leading byte of each Raft log entry body.
const (
	cmdPub    byte = 0x01
	cmdCommit byte = 0x02
)

// pubResult is what brokerFSM.Apply returns for a PUB command — the
// leader's client handler pulls this back out to send to its caller.
type pubResult struct {
	partition int32
	offset    int64
}

// Publish submits a PUB command via Raft. Only the leader succeeds; on a
// follower it returns an error whose message carries the current leader's
// address so the caller can retry there.
func (c *Cluster) Publish(topic string, key, payload []byte) (int32, int64, error) {
	if c.raft.State() != raft.Leader {
		return 0, 0, fmt.Errorf("not leader; leader=%s", c.LeaderAddress())
	}
	body := proto.NewBuilder().
		Byte(cmdPub).
		String(topic).
		Bytes(key).
		Bytes(payload).
		Build()
	f := c.raft.Apply(body, 2*time.Second)
	if err := f.Error(); err != nil {
		return 0, 0, err
	}
	resp := f.Response()
	if err, ok := resp.(error); ok {
		return 0, 0, err
	}
	if pr, ok := resp.(pubResult); ok {
		return pr.partition, pr.offset, nil
	}
	return 0, 0, errors.New("unexpected FSM response")
}

// CommitGroup submits a COMMIT command via Raft.
func (c *Cluster) CommitGroup(topic, group string, partition int32, offset int64) error {
	if c.raft.State() != raft.Leader {
		return fmt.Errorf("not leader; leader=%s", c.LeaderAddress())
	}
	body := proto.NewBuilder().
		Byte(cmdCommit).
		String(topic).
		String(group).
		U32(uint32(partition)).
		U64(uint64(offset)).
		Build()
	f := c.raft.Apply(body, 2*time.Second)
	if err := f.Error(); err != nil {
		return err
	}
	if err, ok := f.Response().(error); ok {
		return err
	}
	return nil
}

// --- FSM ---------------------------------------------------------------------

type brokerFSM struct {
	broker *Broker
}

func (f *brokerFSM) Apply(l *raft.Log) any {
	if len(l.Data) < 1 {
		return errors.New("empty log entry")
	}
	cmd := l.Data[0]
	p := proto.NewParser(l.Data[1:])
	switch cmd {
	case cmdPub:
		topic, err := p.String()
		if err != nil {
			return err
		}
		key, err := p.Bytes()
		if err != nil {
			return err
		}
		payload, err := p.Bytes()
		if err != nil {
			return err
		}
		pid, off, err := f.broker.Publish(topic, key, payload)
		if err != nil {
			return err
		}
		return pubResult{partition: pid, offset: off}

	case cmdCommit:
		topic, err := p.String()
		if err != nil {
			return err
		}
		group, err := p.String()
		if err != nil {
			return err
		}
		pid32, err := p.U32()
		if err != nil {
			return err
		}
		off64, err := p.U64()
		if err != nil {
			return err
		}
		t, err := f.broker.Topic(topic)
		if err != nil {
			return err
		}
		return t.Commit(group, int32(pid32), int64(off64))

	default:
		return fmt.Errorf("unknown command byte 0x%02x", cmd)
	}
}

// Snapshot returns a no-op snapshot. The broker keeps its state in segment
// files on disk already; replaying the Raft log on startup re-applies any
// writes that hadn't been materialized yet. A real implementation would
// checkpoint the data directory here so the log can be truncated.
func (f *brokerFSM) Snapshot() (raft.FSMSnapshot, error) { return &noopSnapshot{}, nil }

func (f *brokerFSM) Restore(r io.ReadCloser) error { defer r.Close(); return nil }

type noopSnapshot struct{}

func (n *noopSnapshot) Persist(sink raft.SnapshotSink) error { return sink.Close() }
func (n *noopSnapshot) Release()                             {}
