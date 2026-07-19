# goq

A standalone, RabbitMQ-style message broker in Go: single static binary, TCP
server, SQLite persistence, Observer-pattern dispatch. No external services —
no separate broker process to install, no cluster to run, no cgo dependency
(pure-Go SQLite driver). Build it, run it, point clients at a host:port.

## Why

Most message-broker options force a tradeoff: run a heavyweight external
service (RabbitMQ, Kafka) for a workload that doesn't need its durability
guarantees or clustering, or hand-roll an in-process pub/sub that loses
everything on restart. goq targets the middle: a real broker — persisted
messages, at-least-once delivery via acks, slow-consumer isolation — that
ships as one binary with an embedded SQLite file for storage.

A producer declares a topic once with a fixed dispatch mode:

- **broadcast** — every subscriber gets every message.
- **roundrobin** — each message goes to exactly one subscriber, rotating.

Every published message is durably persisted before the producer's publish is
acknowledged, so a publish is never lost even if no consumer is currently
connected. Consumers ack messages after processing; a consumer whose outbound
queue backs up is disconnected rather than allowed to stall the whole broker.

## Server

Run the broker itself. There's no published binary yet, so build it from
source.

### Clone and build

```bash
git clone https://github.com/dobadevv/goq.git
cd goq
make build
```

### Run

```bash
./bin/goqd
```

The server logs `goqd listening` on startup and shuts down gracefully on
`SIGINT`/`SIGTERM`, closing the listener and the database cleanly.

### Configure

`goqd` reads its configuration entirely from environment variables — no
flags:

| Var | Default | Description |
|---|---|---|
| `GOQD_HOST` | `127.0.0.1` | host/interface to bind |
| `GOQD_PORT` | `7711` | TCP port to listen on |
| `GOQD_DB_PATH` | `goq.db` | path to the SQLite database file |
| `GOQD_SLOW_CONSUMER_TIMEOUT` | `5s` | disconnect a consumer whose send queue stays full this long |

All variables are optional; unset ones fall back to the defaults above.

```bash
GOQD_HOST=127.0.0.1 GOQD_PORT=7711 GOQD_DB_PATH=goq.db ./goqd
```

### Run the tests

```bash
go test ./...
```

## Client

`client` wraps the wire protocol behind a small Go API — connect, declare a
topic, publish, and subscribe — so your code doesn't need to speak the raw
protocol directly.

### Install

```bash
go get -u github.com/dobadevv/goq/client
```

### Connect and declare a topic

```go
import (
	"context"
	"log"

	"github.com/dobadevv/goq/client"
)

c := client.New("127.0.0.1:7711", client.WithClientID("svc-a"))
if err := c.Connect(context.Background()); err != nil {
	log.Fatal(err)
}
defer c.Close()

if err := c.Declare(context.Background(), "emails", client.ModeBroadcast); err != nil {
	log.Fatal(err)
}
```

### Publish

```go
if err := c.Publish(context.Background(), "emails", []byte("hello")); err != nil {
	log.Fatal(err)
}
```

### Consume

`Subscribe` takes over the connection's read loop and blocks, so give it its
own `Client` and run it in a goroutine. The handler's return value controls
acking: `nil` acks the message and continues; a returned error stops
`Subscribe` and is returned from the call.

```go
consumer := client.New("127.0.0.1:7711", client.WithClientID("worker-1"))
if err := consumer.Connect(context.Background()); err != nil {
	log.Fatal(err)
}
defer consumer.Close()

go func() {
	err := consumer.Subscribe(context.Background(), "emails", func(m client.Message) error {
		handle(m.Payload) // your processing logic
		return nil        // nil acks and continues; a returned error stops Subscribe
	})
	if err != nil {
		log.Println("subscribe stopped:", err)
	}
}()
```

Cancel the `context.Context` passed to `Subscribe` (or close the `Client`)
to stop the goroutine.

Broker-side `ERROR` replies surface as `*client.ServerError`, so callers can
`errors.As` to inspect the reason instead of string-matching `Error()`.

A `Client` is safe for concurrent `Declare`/`Publish` calls, but `Subscribe`
owns the connection's reads for as long as it runs — don't call
`Declare`/`Publish` on a `Client` that's actively subscribed; use a second
`Client` instead, as in the producer/consumer split above.

## CLI

`goq-cli` is a thin client good enough for hand-testing and as living
protocol documentation.

### Install

```bash
go install github.com/dobadevv/goq/cmd/goq-cli@latest
```

### Declare a topic

Do this once, before publishing or subscribing:

```bash
goq-cli declare --addr 127.0.0.1:7711 --topic emails --mode roundrobin
```

### Publish

```bash
goq-cli publish --addr 127.0.0.1:7711 --topic emails --payload "hello"
```

### Consume

Blocks, printing `id  topic  payload` per message, auto-acking each one:

```bash
goq-cli subscribe --addr 127.0.0.1:7711 --topic emails --client-id c1
```

## Protocol

For other languages, or for lower-level control, the wire protocol is
length-prefixed JSON frames (4-byte big-endian length + JSON body, max 1
MiB) — see `internal/protocol`. Any client, in any language, just needs to
speak that framing over TCP.

| Command | Direction | Payload | Meaning |
|---|---|---|---|
| `CONNECT` | client → server | `{role, client_id}` | identify the connection as `producer` or `consumer` |
| `DECLARE` | client → server | `{topic, mode}` | create a topic with a fixed `broadcast`\|`roundrobin` mode |
| `PUBLISH` | client → server | `{topic, payload}` | publish a message; `OK` means durably persisted |
| `SUBSCRIBE` | client → server | `{topic}` | register as an observer for a topic |
| `ACK` | client → server | `{message_id}` | acknowledge processing of a delivered message |
| `MESSAGE` | server → client | `{id, topic, payload}` | pushed to a subscriber |
| `OK` | server → client | `{}` | command succeeded |
| `ERROR` | server → client | `{reason}` | command failed |

A topic must be `DECLARE`d before it can be `PUBLISH`ed to or `SUBSCRIBE`d to.
See `docs/superpowers/specs/2026-07-14-goq-mvp-design.md` for the full design,
including delivery tracking and slow-consumer handling.
