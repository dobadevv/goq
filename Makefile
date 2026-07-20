.PHONY: build test fmt lint docker-build docker-publish

IMAGE := dobadevv/goq
VERSION := $(shell git describe --tags --always --dirty)

build:
	go build -o bin/goqd ./cmd/goqd
	go build -o bin/goq-cli ./cmd/goq-cli

test:
	go test ./...

fmt:
	gofmt -l -w .

lint:
	golangci-lint run

docker-build:
	docker build -t $(IMAGE):$(VERSION) -t $(IMAGE):latest .

docker-publish:
	./scripts/publish-docker.sh
