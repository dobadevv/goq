# goq

A standalone RabbitMQ-style message broker in Go. Single static binary, TCP
server, SQLite persistence, Observer-pattern dispatch. No external services.

## Build

```bash
go build ./cmd/goqd      # broker server
go build ./cmd/goq-cli   # test client
```

## Run

```bash
./goqd --host 127.0.0.1 --port 7711 --db-path goq.db
```

## Try it

```bash
# Declare a topic (broadcast or roundrobin)
./goq-cli declare --addr 127.0.0.1:7711 --topic emails --mode roundrobin

# Subscribe (prints "id  topic  payload" per message, auto-acks)
./goq-cli subscribe --addr 127.0.0.1:7711 --topic emails --client-id c1

# Publish (in another terminal)
./goq-cli publish --addr 127.0.0.1:7711 --topic emails --payload "hello"
```

## Protocol

Length-prefixed JSON frames (4-byte big-endian length + JSON body, max 1 MiB).
Commands: `CONNECT`, `DECLARE`, `PUBLISH`, `SUBSCRIBE`, `ACK`; server replies
`OK`, `ERROR`, and pushes `MESSAGE`. See
`docs/superpowers/specs/2026-07-14-goq-mvp-design.md` for the full design.

## Test

```bash
go test ./...
```
