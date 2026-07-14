package server

import (
	"crypto/rand"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"

	"goq/internal/broker"
	"goq/internal/protocol"
)

var (
	errSlowConsumer = errors.New("server: consumer too slow")
	errConnClosed   = errors.New("server: connection closed")
)

// outboundItem is a frame queued for the writer goroutine. onSent, if set, runs
// after the frame is written to the socket.
type outboundItem struct {
	env    protocol.Envelope
	onSent func()
}

// connection is one client connection. It implements broker.Observer so a
// subscribed consumer can be attached to topics directly.
type connection struct {
	srv       *Server
	netConn   net.Conn
	outbound  chan outboundItem
	done      chan struct{}
	closeOnce sync.Once

	authenticated bool
	clientID      string
	role          string

	subsMu sync.Mutex
	subs   map[string]*broker.Topic // topics this connection is attached to
}

func newConnection(srv *Server, netConn net.Conn) *connection {
	return &connection{
		srv:      srv,
		netConn:  netConn,
		outbound: make(chan outboundItem, srv.cfg.OutboundCapacity),
		done:     make(chan struct{}),
		subs:     make(map[string]*broker.Topic),
	}
}

func (c *connection) ID() string { return c.clientID }

// writeLoop serializes all socket writes for this connection.
func (c *connection) writeLoop() {
	for {
		select {
		case item := <-c.outbound:
			if err := protocol.WriteFrame(c.netConn, item.env); err != nil {
				c.close()
				return
			}
			if item.onSent != nil {
				item.onSent()
			}
		case <-c.done:
			return
		}
	}
}

// readLoop reads and dispatches frames until the connection closes.
func (c *connection) readLoop() {
	defer c.close()
	for {
		env, err := protocol.ReadFrame(c.netConn)
		if err != nil {
			return
		}
		c.handle(env)
	}
}

// reply enqueues a control frame (OK/ERROR) to this connection.
func (c *connection) reply(env protocol.Envelope) {
	select {
	case c.outbound <- outboundItem{env: env}:
	case <-c.done:
	}
}

func (c *connection) replyOK() {
	env, _ := protocol.Encode(protocol.TypeOK, nil)
	c.reply(env)
}

func (c *connection) replyError(reason string) {
	env, _ := protocol.Encode(protocol.TypeError, protocol.Error{Reason: reason})
	c.reply(env)
}

// replySync enqueues env and blocks until the writer has actually written it
// to the socket (or the connection starts closing for some other reason). A
// plain reply() only guarantees the frame reaches the outbound queue, not the
// socket, so a reply() immediately followed by close() can have close() win
// the race and drop the frame; replySync closes that gap.
func (c *connection) replySync(env protocol.Envelope) {
	sent := make(chan struct{})
	item := outboundItem{env: env, onSent: func() { close(sent) }}
	select {
	case c.outbound <- item:
	case <-c.done:
		return
	}
	select {
	case <-sent:
	case <-c.done:
	}
}

// replyErrorAndClose sends a terminal ERROR reply and only closes the
// connection once that reply has actually been written, so the client is
// guaranteed to see the error instead of a bare EOF.
func (c *connection) replyErrorAndClose(reason string) {
	env, _ := protocol.Encode(protocol.TypeError, protocol.Error{Reason: reason})
	c.replySync(env)
	c.close()
}

// handle routes a single command. The first frame must be CONNECT.
func (c *connection) handle(env protocol.Envelope) {
	if !c.authenticated {
		if env.Type != protocol.TypeConnect {
			c.replyErrorAndClose("expected CONNECT")
			return
		}
		c.handleConnect(env)
		return
	}
	switch env.Type {
	case protocol.TypeConnect:
		c.replyError("already connected")
	case protocol.TypeDeclare:
		c.handleDeclare(env)
	case protocol.TypeSubscribe:
		c.handleSubscribe(env)
	case protocol.TypePublish:
		c.handlePublish(env)
	case protocol.TypeAck:
		c.handleAck(env)
	default:
		c.replyError("unknown command: " + env.Type)
	}
}

func (c *connection) handleConnect(env protocol.Envelope) {
	var p protocol.Connect
	if err := env.Decode(&p); err != nil {
		c.replyErrorAndClose("bad CONNECT payload")
		return
	}
	if p.ClientID == "" {
		c.replyErrorAndClose("client_id required")
		return
	}
	if !c.srv.clients.add(p.ClientID) {
		c.replyErrorAndClose("client_id already in use")
		return
	}
	c.clientID = p.ClientID
	c.role = p.Role
	c.authenticated = true
	slog.Info("client connected", "client_id", p.ClientID, "role", p.Role)
	c.replyOK()
}

// close is idempotent: it detaches from topics, frees the client ID, and closes
// the socket.
func (c *connection) close() {
	c.closeOnce.Do(func() {
		close(c.done)
		c.subsMu.Lock()
		for _, top := range c.subs {
			top.Detach(c.clientID)
		}
		c.subs = map[string]*broker.Topic{}
		c.subsMu.Unlock()
		if c.clientID != "" {
			c.srv.clients.remove(c.clientID)
		}
		_ = c.netConn.Close()
		c.srv.removeConn(c)
		if c.clientID != "" {
			slog.Info("client disconnected", "client_id", c.clientID)
		}
	})
}

// notifyTimeout is the slow-consumer disconnect threshold.
func (c *connection) notifyTimeout() time.Duration { return c.srv.cfg.SlowConsumerTimeout }

func (c *connection) handleDeclare(env protocol.Envelope) {
	var p protocol.Declare
	if err := env.Decode(&p); err != nil {
		c.replyError("bad DECLARE payload")
		return
	}
	if p.Mode != protocol.ModeBroadcast && p.Mode != protocol.ModeRoundRobin {
		c.replyError("invalid mode: " + p.Mode)
		return
	}
	switch err := c.srv.broker.Declare(p.Topic, p.Mode); {
	case err == nil:
		slog.Info("topic declared", "topic", p.Topic, "mode", p.Mode)
		c.replyOK()
	case errors.Is(err, broker.ErrModeConflict):
		c.replyError("topic already declared with a different mode")
	default:
		c.replyError("declare failed")
	}
}

func (c *connection) handleSubscribe(env protocol.Envelope) {
	var p protocol.Subscribe
	if err := env.Decode(&p); err != nil {
		c.replyError("bad SUBSCRIBE payload")
		return
	}
	top, ok := c.srv.broker.Topic(p.Topic)
	if !ok {
		c.replyError("topic not declared: " + p.Topic)
		return
	}
	top.Attach(c)
	c.subsMu.Lock()
	c.subs[p.Topic] = top
	c.subsMu.Unlock()
	slog.Info("subscribed", "client_id", c.clientID, "topic", p.Topic)
	c.replyOK()
}

func (c *connection) handlePublish(env protocol.Envelope) {
	var p protocol.Publish
	if err := env.Decode(&p); err != nil {
		c.replyError("bad PUBLISH payload")
		return
	}
	top, ok := c.srv.broker.Topic(p.Topic)
	if !ok {
		c.replyError("topic not declared: " + p.Topic)
		return
	}
	id := newMessageID()
	if err := c.srv.store.InsertMessage(id, p.Topic, p.Payload); err != nil {
		c.replyError("persist failed")
		return
	}
	c.replyOK() // durability ack: persisted, will not be lost
	notified := top.Publish(broker.Message{ID: id, Topic: p.Topic, Payload: p.Payload})
	slog.Info("published", "topic", p.Topic, "message_id", id, "notified", len(notified))
}

func (c *connection) handleAck(env protocol.Envelope) {
	var p protocol.Ack
	if err := env.Decode(&p); err != nil {
		c.replyError("bad ACK payload")
		return
	}
	if err := c.srv.store.MarkAcked(p.MessageID, c.clientID); err != nil {
		c.replyError("ack failed")
		return
	}
	c.replyOK()
}

// Notify implements broker.Observer. It records the queued delivery BEFORE
// enqueueing the frame (same goroutine), so the writer's delivered-mark can
// never run before the row exists. A queue that stays full past the slow-
// consumer timeout causes this consumer to be disconnected.
func (c *connection) Notify(msg broker.Message) error {
	if err := c.srv.store.InsertDelivery(msg.ID, c.clientID); err != nil {
		return err
	}
	env, err := protocol.Encode(protocol.TypeMessage, protocol.Message{
		ID: msg.ID, Topic: msg.Topic, Payload: msg.Payload,
	})
	if err != nil {
		return err
	}
	item := outboundItem{
		env: env,
		onSent: func() {
			_ = c.srv.store.MarkDelivered(msg.ID, c.clientID)
		},
	}
	select {
	case c.outbound <- item:
		return nil
	case <-time.After(c.notifyTimeout()):
		slog.Warn("disconnecting slow consumer", "client_id", c.clientID)
		c.close()
		return errSlowConsumer
	case <-c.done:
		return errConnClosed
	}
}

// newMessageID returns a random UUIDv4 string, using only crypto/rand.
func newMessageID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic("crypto/rand failed: " + err.Error())
	}
	b[6] = (b[6] & 0x0f) | 0x40 // version 4
	b[8] = (b[8] & 0x3f) | 0x80 // variant 10
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}
