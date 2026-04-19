package main

import (
	"context"
	"flag"
	"log"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

func main() {
	addr := flag.String("addr", ":4222", "TCP address to listen on")
	dir := flag.String("dir", "./data", "directory for topic logs")
	partitions := flag.Int("partitions", 4, "partitions per topic")
	segSize := flag.Int("segment-size", 1000, "records per segment before rolling")
	retain := flag.Int("retain", 100, "segments kept per partition (sealed + active)")
	retainFor := flag.Duration("retain-for", 0,
		"time-based retention: drop sealed segments older than this duration (0 disables)")
	sweepEvery := flag.Duration("sweep-every", 30*time.Second,
		"how often to run the time-retention sweep when -retain-for is set")
	heartbeatTimeout := flag.Duration("heartbeat-timeout", 15*time.Second,
		"group members without a heartbeat within this window get kicked (0 disables)")
	compactEvery := flag.Duration("compact-every", 0,
		"run log compaction at this interval (0 disables); keeps the latest record per key")

	// --- Cluster / replication flags --------------------------------------
	clusterID := flag.String("cluster-id", "",
		"this node's id (also used as raft ServerID); enables replication when set")
	clusterAddr := flag.String("cluster-addr", "",
		"this node's raft RPC address, e.g. :8001")
	clusterPeers := flag.String("cluster-peers", "",
		"comma-separated peer list `id@addr`, e.g. n1@:8001,n2@:8002,n3@:8003")
	clusterBootstrap := flag.Bool("cluster-bootstrap", false,
		"bootstrap a brand-new cluster from this node (exactly one node, first run only)")

	metricsAddr := flag.String("metrics-addr", "",
		"serve Prometheus metrics on /metrics at this address (empty = disabled)")

	healthcheck := flag.Bool("healthcheck", false,
		"dial the broker's -addr and exit 0 if reachable, 1 otherwise (for docker HEALTHCHECK)")

	flag.Parse()

	if *healthcheck {
		a := *addr
		if strings.HasPrefix(a, ":") {
			a = "127.0.0.1" + a
		}
		conn, err := net.DialTimeout("tcp", a, 2*time.Second)
		if err != nil {
			os.Exit(1)
		}
		conn.Close()
		os.Exit(0)
	}

	broker, err := NewBroker(*dir, *partitions, *segSize, *retain)
	if err != nil {
		log.Fatal(err)
	}
	broker.SetTimeRetention(*retainFor, *sweepEvery)
	broker.SetCompaction(*compactEvery)
	broker.Run()
	defer broker.Stop()

	server := NewServer(broker)
	server.SetHeartbeatTimeout(*heartbeatTimeout)
	defer server.Stop()

	if *metricsAddr != "" {
		srv := startMetricsServer(*metricsAddr)
		defer func() {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			_ = srv.Shutdown(ctx)
		}()
	}

	if *clusterID != "" {
		peers := strings.Split(*clusterPeers, ",")
		cluster, err := NewCluster(broker, ClusterConfig{
			NodeID:    *clusterID,
			BindAddr:  *clusterAddr,
			Peers:     peers,
			Bootstrap: *clusterBootstrap,
			DataDir:   filepath.Join(*dir, "raft"),
		})
		if err != nil {
			log.Fatal(err)
		}
		server.AttachCluster(cluster)
		defer cluster.Shutdown()
		log.Printf("cluster mode: id=%s addr=%s peers=%v bootstrap=%v",
			*clusterID, *clusterAddr, peers, *clusterBootstrap)
	}

	// Start the accept loop in a goroutine so we can react to signals.
	listenErr := make(chan error, 1)
	go func() {
		listenErr <- server.Listen(*addr)
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)

	select {
	case err := <-listenErr:
		if err != nil {
			log.Fatal(err)
		}
	case s := <-sig:
		log.Printf("received %s, shutting down...", s)
		// Defers (broker.Stop, server.Stop, cluster.Shutdown) run when main
		// returns; server.Stop closes the listener so Listen returns.
	}
}
