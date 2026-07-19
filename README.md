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
GOQD_HOST=127.0.0.1 GOQD_PORT=7711 GOQD_DB_PATH=goq.db ./goqd
```

Config is read from environment variables (all optional):

| Var | Default | Description |
|---|---|---|
| `GOQD_HOST` | `127.0.0.1` | host/interface to bind |
| `GOQD_PORT` | `7711` | TCP port to listen on |
| `GOQD_DB_PATH` | `goq.db` | path to the SQLite database file |
| `GOQD_SLOW_CONSUMER_TIMEOUT` | `5s` | disconnect a consumer whose send queue stays full this long |

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
