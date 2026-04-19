BINARY := minibroker

.PHONY: all build run run-partitions run-segments run-heartbeat fmt tidy vet clean test \
        demo demo-reconnect demo-segments demo-groups demo-partitions demo-heartbeat demo-cluster \
        docker-build docker-up docker-down docker-cluster-up docker-cluster-down docker-clean

all: build

build:
	go build -o $(BINARY) .

## run broker with defaults (4 partitions, segment-size 1000, retain 100)
run: build
	./$(BINARY)

## run broker with 4 partitions for the partitions demo
run-partitions: build
	./$(BINARY) -partitions 4

## run broker with tiny segments for the segments demo
run-segments: build
	./$(BINARY) -partitions 1 -segment-size 5 -retain 2

## run broker with a short heartbeat timeout for the heartbeat demo
run-heartbeat: build
	./$(BINARY) -heartbeat-timeout 2s

demo:
	go run ./examples/demo

demo-reconnect:
	go run ./examples/reconnect

demo-segments:
	go run ./examples/segments

demo-groups:
	go run ./examples/groups

demo-partitions:
	go run ./examples/partitions

demo-heartbeat:
	go run ./examples/heartbeat

demo-cluster: build
	./examples/cluster/run-cluster.sh

test:
	go test ./...

fmt:
	gofmt -w .

vet:
	go vet ./...

tidy:
	go mod tidy

clean:
	rm -f $(BINARY)
	rm -rf data data-n1 data-n2 data-n3

## --- Docker --------------------------------------------------------------
## Build the local image.
docker-build:
	docker build -t minibroker:latest .

## Single-node broker in docker-compose.
docker-up:
	docker compose up -d --build

docker-down:
	docker compose down

## Replicated 3-node cluster (docker-compose.cluster.yml).
docker-cluster-up:
	docker compose -f docker-compose.cluster.yml up -d --build

docker-cluster-down:
	docker compose -f docker-compose.cluster.yml down -v

## Remove local image + compose volumes.
docker-clean:
	docker compose down -v 2>/dev/null || true
	docker compose -f docker-compose.cluster.yml down -v 2>/dev/null || true
	docker compose -f docker-compose.bench.yml down -v 2>/dev/null || true
	docker rmi minibroker:latest 2>/dev/null || true

## --- Benchmarks ----------------------------------------------------------
## Start all four brokers (minibroker, nats, rabbit, redis) in docker.
bench-up:
	docker compose -f docker-compose.bench.yml up -d --build

bench-down:
	docker compose -f docker-compose.bench.yml down -v

## Run a full pass against every target (assumes bench-up already ran).
bench:
	@echo "--- 100 B payloads, n=20000 ---"
	go run ./bench -target minibroker  -n 20000 -size 100
	go run ./bench -target nats-js     -n 20000 -size 100
	go run ./bench -target rabbit      -n 20000 -size 100
	go run ./bench -target redis -addr localhost:6380 -n 20000 -size 100
	@echo "--- 1 KiB payloads, n=10000 ---"
	go run ./bench -target minibroker  -n 10000 -size 1024
	go run ./bench -target nats-js     -n 10000 -size 1024
	go run ./bench -target rabbit      -n 10000 -size 1024
	go run ./bench -target redis -addr localhost:6380 -n 10000 -size 1024
