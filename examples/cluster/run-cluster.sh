#!/bin/bash
# Spin up a 3-node minibroker cluster on localhost and run the cluster demo.
#
# Layout:
#   node n1: broker :4221, raft :8001, data ./data-n1
#   node n2: broker :4222, raft :8002, data ./data-n2
#   node n3: broker :4223, raft :8003, data ./data-n3
#
# n1 bootstraps, n2/n3 join after.
set -u

PEERS="n1@127.0.0.1:8001,n2@127.0.0.1:8002,n3@127.0.0.1:8003"

# Clean previous state
pkill -f 'minibroker -addr' 2>/dev/null || true
sleep 0.3
rm -rf data-n1 data-n2 data-n3

# Start the three nodes. n1 with -cluster-bootstrap.
./minibroker -addr :4221 -dir ./data-n1 -partitions 1 \
    -cluster-id n1 -cluster-addr 127.0.0.1:8001 -cluster-peers "$PEERS" \
    -cluster-bootstrap > /tmp/n1.log 2>&1 &
N1=$!
./minibroker -addr :4222 -dir ./data-n2 -partitions 1 \
    -cluster-id n2 -cluster-addr 127.0.0.1:8002 -cluster-peers "$PEERS" \
    > /tmp/n2.log 2>&1 &
N2=$!
./minibroker -addr :4223 -dir ./data-n3 -partitions 1 \
    -cluster-id n3 -cluster-addr 127.0.0.1:8003 -cluster-peers "$PEERS" \
    > /tmp/n3.log 2>&1 &
N3=$!

echo "started nodes: n1=$N1 n2=$N2 n3=$N3"
echo "waiting for leader election..."
sleep 3

# Run the Go demo program that publishes through the leader and reads it back
# from a follower. The demo code probes each node to find the leader.
go run ./examples/cluster
DEMO_RC=$?

# Tear down
kill $N1 $N2 $N3 2>/dev/null
wait $N1 $N2 $N3 2>/dev/null
rm -rf data-n1 data-n2 data-n3

exit $DEMO_RC
