package main

import (
	"fmt"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
)

// TestClusterReplication stands up a 3-node Raft cluster using the library's
// in-memory transport (raft.InmemTransport) and FSM, publishes a record
// through the leader, and verifies every node materialises the write locally.
func TestClusterReplication(t *testing.T) {
	type node struct {
		id        raft.ServerID
		broker    *Broker
		fsm       *brokerFSM
		transport *raft.InmemTransport
		raft      *raft.Raft
	}

	const nNodes = 3
	nodes := make([]*node, nNodes)

	for i := range nodes {
		b, err := NewBroker(t.TempDir(), 1, 1000, 100)
		if err != nil {
			t.Fatalf("node %d: NewBroker: %v", i, err)
		}
		id := raft.ServerID(fmt.Sprintf("n%d", i))
		_, transport := raft.NewInmemTransport(raft.ServerAddress(id))
		nodes[i] = &node{
			id:        id,
			broker:    b,
			fsm:       &brokerFSM{broker: b},
			transport: transport,
		}
	}

	// Fully mesh the in-memory transports.
	for i := range nodes {
		for j := range nodes {
			if i == j {
				continue
			}
			nodes[i].transport.Connect(nodes[j].transport.LocalAddr(), nodes[j].transport)
		}
	}

	servers := make([]raft.Server, nNodes)
	for i, n := range nodes {
		servers[i] = raft.Server{ID: n.id, Address: n.transport.LocalAddr()}
	}

	for i, n := range nodes {
		cfg := raft.DefaultConfig()
		cfg.LocalID = n.id
		cfg.Logger = hclog.NewNullLogger()
		// Tight timeouts so the test doesn't spend a second on election.
		cfg.HeartbeatTimeout = 50 * time.Millisecond
		cfg.ElectionTimeout = 50 * time.Millisecond
		cfg.LeaderLeaseTimeout = 50 * time.Millisecond
		cfg.CommitTimeout = 5 * time.Millisecond

		logs := raft.NewInmemStore()
		stable := raft.NewInmemStore()
		snaps := raft.NewInmemSnapshotStore()

		r, err := raft.NewRaft(cfg, n.fsm, logs, stable, snaps, n.transport)
		if err != nil {
			t.Fatalf("node %d: NewRaft: %v", i, err)
		}
		n.raft = r
		if i == 0 {
			if err := r.BootstrapCluster(raft.Configuration{Servers: servers}).Error(); err != nil {
				t.Fatalf("bootstrap: %v", err)
			}
		}
	}
	t.Cleanup(func() {
		for _, n := range nodes {
			_ = n.raft.Shutdown().Error()
		}
	})

	// Wait for a leader to appear.
	var leader *node
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		for _, n := range nodes {
			if n.raft.State() == raft.Leader {
				leader = n
				break
			}
		}
		if leader != nil {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	if leader == nil {
		t.Fatal("no leader elected within 3s")
	}

	// Publish through the leader's Cluster wrapper.
	c := &Cluster{raft: leader.raft, fsm: leader.fsm}
	pid, off, err := c.Publish("t", []byte("k"), []byte("hello"))
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	if pid != 0 || off != 0 {
		t.Fatalf("got partition=%d offset=%d, want 0/0", pid, off)
	}

	// Wait for the write to land on all nodes.
	for retry := 0; retry < 40; retry++ {
		allGood := true
		for _, n := range nodes {
			head, err := n.broker.PartitionHead("t", 0)
			if err != nil || head != 1 {
				allGood = false
				break
			}
		}
		if allGood {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	for i, n := range nodes {
		head, _ := n.broker.PartitionHead("t", 0)
		t.Errorf("node %d (id=%s): head=%d, want 1", i, n.id, head)
	}
}
