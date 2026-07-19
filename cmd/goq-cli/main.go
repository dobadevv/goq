// Command goq-cli is a thin client for exercising a goq broker by hand.
//
// Usage:
//
//	goq-cli declare   --addr host:port --topic NAME --mode broadcast|roundrobin [--client-id ID]
//	goq-cli publish   --addr host:port --topic NAME --payload TEXT             [--client-id ID]
//	goq-cli subscribe --addr host:port --topic NAME                            [--client-id ID]
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/dobadevv/goq/client"
)

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	switch os.Args[1] {
	case "declare":
		addr, id, topic, mode := declareFlags(os.Args[2:])
		fail(runDeclare(addr, id, topic, mode))
	case "publish":
		addr, id, topic, payload := publishFlags(os.Args[2:])
		fail(runPublish(addr, id, topic, []byte(payload)))
	case "subscribe":
		addr, id, topic := subscribeFlags(os.Args[2:])
		fail(runSubscribe(addr, id, topic, os.Stdout, nil))
	default:
		usage()
	}
}

func runDeclare(addr, clientID, topic, mode string) error {
	c := client.New(addr, client.WithClientID(clientID))
	if err := c.Connect(context.Background()); err != nil {
		return err
	}
	defer c.Close()
	return c.Declare(context.Background(), topic, mode)
}

func runPublish(addr, clientID, topic string, payload []byte) error {
	c := client.New(addr, client.WithClientID(clientID))
	if err := c.Connect(context.Background()); err != nil {
		return err
	}
	defer c.Close()
	return c.Publish(context.Background(), topic, payload)
}

// runSubscribe connects, subscribes to topic, and blocks printing
// "id  topic  payload" per message until stop closes (if non-nil) or the
// connection ends.
func runSubscribe(addr, clientID, topic string, out io.Writer, stop <-chan struct{}) error {
	c := client.New(addr, client.WithClientID(clientID))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := c.Connect(ctx); err != nil {
		return err
	}
	defer c.Close()
	if stop != nil {
		go func() {
			select {
			case <-stop:
				cancel()
			case <-ctx.Done():
			}
		}()
	}
	return c.Subscribe(ctx, topic, func(m client.Message) error {
		fmt.Fprintf(out, "%s\t%s\t%s\n", m.ID, m.Topic, m.Payload)
		return nil
	})
}

func fail(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: goq-cli <declare|publish|subscribe> --addr host:port --topic NAME [flags]")
	os.Exit(2)
}

func declareFlags(args []string) (addr, id, topic, mode string) {
	fs := flag.NewFlagSet("declare", flag.ExitOnError)
	a := fs.String("addr", "127.0.0.1:7711", "broker address")
	i := fs.String("client-id", "cli-declare", "client id")
	tp := fs.String("topic", "", "topic name")
	md := fs.String("mode", "broadcast", "broadcast|roundrobin")
	_ = fs.Parse(args)
	return *a, *i, *tp, *md
}

func publishFlags(args []string) (addr, id, topic, payload string) {
	fs := flag.NewFlagSet("publish", flag.ExitOnError)
	a := fs.String("addr", "127.0.0.1:7711", "broker address")
	i := fs.String("client-id", "cli-publish", "client id")
	tp := fs.String("topic", "", "topic name")
	pl := fs.String("payload", "", "message payload")
	_ = fs.Parse(args)
	return *a, *i, *tp, *pl
}

func subscribeFlags(args []string) (addr, id, topic string) {
	fs := flag.NewFlagSet("subscribe", flag.ExitOnError)
	a := fs.String("addr", "127.0.0.1:7711", "broker address")
	i := fs.String("client-id", "cli-subscribe", "client id")
	tp := fs.String("topic", "", "topic name")
	_ = fs.Parse(args)
	return *a, *i, *tp
}
