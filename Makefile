.PHONY: build test fmt lint

build:
	go build -o bin/goqd ./cmd/goqd
	go build -o bin/goq-cli ./cmd/goq-cli

test:
	go test ./...

fmt:
	gofmt -l -w .

lint:
	golangci-lint run
