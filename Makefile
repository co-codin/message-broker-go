BINARY := minibroker

.PHONY: all build run run-partitions run-segments run-heartbeat fmt tidy vet clean test \
        demo demo-reconnect demo-segments demo-groups demo-partitions demo-heartbeat demo-cluster

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
