// Command goq-cli is a thin client for exercising a goq broker by hand.
//
// Usage:
//
//	goq-cli declare   --addr host:port --topic NAME --mode broadcast|roundrobin [--client-id ID]
//	goq-cli publish   --addr host:port --topic NAME --payload TEXT             [--client-id ID]
//	goq-cli subscribe --addr host:port --topic NAME                            [--client-id ID]
package main

import (
	"flag"
	"fmt"
	"io"
	"net"
	"os"

	"goq/internal/protocol"
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

// connect dials addr and performs the CONNECT handshake.
func connect(addr, role, clientID string) (net.Conn, error) {
	c, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	if err := writeCmd(c, protocol.TypeConnect, protocol.Connect{Role: role, ClientID: clientID}); err != nil {
		_ = c.Close()
		return nil, err
	}
	if err := expectOK(c); err != nil {
		_ = c.Close()
		return nil, err
	}
	return c, nil
}

func runDeclare(addr, clientID, topic, mode string) error {
	c, err := connect(addr, "producer", clientID)
	if err != nil {
		return err
	}
	defer c.Close()
	if err := writeCmd(c, protocol.TypeDeclare, protocol.Declare{Topic: topic, Mode: mode}); err != nil {
		return err
	}
	return expectOK(c)
}

func runPublish(addr, clientID, topic string, payload []byte) error {
	c, err := connect(addr, "producer", clientID)
	if err != nil {
		return err
	}
	defer c.Close()
	if err := writeCmd(c, protocol.TypePublish, protocol.Publish{Topic: topic, Payload: payload}); err != nil {
		return err
	}
	return expectOK(c)
}

func runSubscribe(addr, clientID, topic string, out io.Writer, stop <-chan struct{}) error {
	c, err := connect(addr, "consumer", clientID)
	if err != nil {
		return err
	}
	defer c.Close()
	if err := writeCmd(c, protocol.TypeSubscribe, protocol.Subscribe{Topic: topic}); err != nil {
		return err
	}
	if err := expectOK(c); err != nil {
		return err
	}
	if stop != nil {
		go func() { <-stop; _ = c.Close() }()
	}
	for {
		env, err := protocol.ReadFrame(c)
		if err != nil {
			return nil // connection closed or stopped
		}
		if env.Type != protocol.TypeMessage {
			continue
		}
		var m protocol.Message
		if err := env.Decode(&m); err != nil {
			return err
		}
		fmt.Fprintf(out, "%s\t%s\t%s\n", m.ID, m.Topic, m.Payload)
		_ = writeCmd(c, protocol.TypeAck, protocol.Ack{MessageID: m.ID})
	}
}

func writeCmd(c net.Conn, cmdType string, payload any) error {
	env, err := protocol.Encode(cmdType, payload)
	if err != nil {
		return err
	}
	return protocol.WriteFrame(c, env)
}

func expectOK(c net.Conn) error {
	env, err := protocol.ReadFrame(c)
	if err != nil {
		return err
	}
	if env.Type == protocol.TypeError {
		var e protocol.Error
		_ = env.Decode(&e)
		return fmt.Errorf("server error: %s", e.Reason)
	}
	if env.Type != protocol.TypeOK {
		return fmt.Errorf("unexpected reply: %s", env.Type)
	}
	return nil
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
