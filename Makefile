BINARY := minibroker

.PHONY: all build run run-partitions run-segments fmt tidy vet clean \
        demo demo-reconnect demo-segments demo-groups demo-partitions

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

fmt:
	gofmt -w .

vet:
	go vet ./...

tidy:
	go mod tidy

clean:
	rm -f $(BINARY)
	rm -rf data
