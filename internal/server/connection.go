package server

import (
	"log/slog"
	"net"
	"sync"
	"time"

	"goq/internal/broker"
	"goq/internal/protocol"
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
	default:
		// DECLARE/PUBLISH/SUBSCRIBE/ACK are added in the next task.
		c.replyError("unsupported command: " + env.Type)
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
