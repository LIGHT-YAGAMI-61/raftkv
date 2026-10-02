NODES3 := node1:5001,node2:5001,node3:5001

.PHONY: build test up down bench

build:
	go build -o bin/node ./cmd/node
	go build -o bin/client ./cmd/client
	go build -o bin/bench ./cmd/bench
	go build -o bin/chaos ./cmd/chaos

test:
	go test -race -count=1 ./...

up:
	docker compose up --build -d

down:
	docker compose down -v

bench:
	docker compose run --rm bench -nodes $(NODES3) -duration 10s -concurrency 50 -read-ratio 0
