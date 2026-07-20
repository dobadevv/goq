// Package client provides a Go client for the goq wire protocol: connect to
// a broker, declare a topic, publish messages, and subscribe to a topic.
package client

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"net"
	"sync"

	"github.com/dobadevv/goq/internal/protocol"
)

// Dispatch modes a topic may be declared with, aliasing internal/protocol's
// constants so callers don't need to import that package directly.
const (
	ModeBroadcast  = protocol.ModeBroadcast
	ModeRoundRobin = protocol.ModeRoundRobin
)

// role identifies every Client connection to the broker. The server does not
// enforce this value; it is only recorded in server-side logs.
const role = "client"

// ErrNotConnected is returned by Declare/Publish/Subscribe when called
// before a successful Connect.
var ErrNotConnected = errors.New("goq: not connected")

// Message is a message delivered to a Subscribe handler.
type Message struct {
	ID      string
	Topic   string
	Payload []byte
}

// ServerError wraps a broker-side ERROR reply, letting callers distinguish a
// server rejection from a network or decode error.
type ServerError struct {
	Reason string
}

func (e *ServerError) Error() string {
	return fmt.Sprintf("goq: server error: %s", e.Reason)
}

// Client is a persistent connection to a goq broker. Declare and Publish are
// safe to call concurrently on the same Client. Subscribe takes over the
// connection's read loop for as long as it runs; do not call Declare or
// Publish concurrently with an active Subscribe on the same Client — create
// a second Client instead.
type Client struct {
	addr     string
	clientID string
	username string
	password string

	mu   sync.Mutex
	conn net.Conn
}

// Option configures a Client at construction time.
type Option func(*Client)

// WithClientID sets the client_id sent on CONNECT. If unset, New generates a
// random one.
func WithClientID(id string) Option {
	return func(c *Client) { c.clientID = id }
}

// WithCredentials sets the username/password sent on CONNECT. Required by
// any broker enforcing authentication (goqd always does).
func WithCredentials(username, password string) Option {
	return func(c *Client) {
		c.username = username
		c.password = password
	}
}

// New constructs a Client for the broker at addr. Call Connect before using
// it.
func New(addr string, opts ...Option) *Client {
	c := &Client{addr: addr}
	for _, opt := range opts {
		opt(c)
	}
	if c.clientID == "" {
		c.clientID = randomClientID()
	}
	return c
}

// Connect dials the broker and performs the CONNECT handshake. ctx bounds
// both the dial and the handshake round trip; cancelling it aborts the
// connection.
func (c *Client) Connect(ctx context.Context) error {
	var d net.Dialer
	conn, err := d.DialContext(ctx, "tcp", c.addr)
	if err != nil {
		return fmt.Errorf("goq: dial %s: %w", c.addr, err)
	}

	c.mu.Lock()
	c.conn = conn
	c.mu.Unlock()

	if err := c.request(ctx, protocol.TypeConnect, protocol.Connect{
		Role:     role,
		ClientID: c.clientID,
		Username: c.username,
		Password: c.password,
	}); err != nil {
		_ = conn.Close()
		return err
	}
	return nil
}

// Close closes the underlying connection.
func (c *Client) Close() error {
	c.mu.Lock()
	conn := c.conn
	c.mu.Unlock()
	if conn == nil {
		return nil
	}
	return conn.Close()
}

// request writes cmdType/payload and waits for a single OK/ERROR reply,
// serialized against other calls on this Client and aborted if ctx is
// cancelled before the reply arrives.
func (c *Client) request(ctx context.Context, cmdType string, payload any) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn == nil {
		return ErrNotConnected
	}
	stop := watchContext(ctx, c.conn)
	defer stop()
	if err := writeCmd(c.conn, cmdType, payload); err != nil {
		return err
	}
	return expectOK(c.conn)
}

// watchContext closes conn if ctx is cancelled before stop is called. This
// aborts any in-flight read or write with an error instead of leaving it
// blocked forever.
func watchContext(ctx context.Context, conn net.Conn) (stop func()) {
	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			_ = conn.Close()
		case <-done:
		}
	}()
	return func() { close(done) }
}

func writeCmd(conn net.Conn, cmdType string, payload any) error {
	env, err := protocol.Encode(cmdType, payload)
	if err != nil {
		return fmt.Errorf("goq: encode %s: %w", cmdType, err)
	}
	return protocol.WriteFrame(conn, env)
}

func expectOK(conn net.Conn) error {
	env, err := protocol.ReadFrame(conn)
	if err != nil {
		return fmt.Errorf("goq: read reply: %w", err)
	}
	if env.Type == protocol.TypeError {
		var e protocol.Error
		_ = env.Decode(&e)
		return &ServerError{Reason: e.Reason}
	}
	if env.Type != protocol.TypeOK {
		return fmt.Errorf("goq: unexpected reply: %s", env.Type)
	}
	return nil
}

func randomClientID() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic("goq: crypto/rand failed: " + err.Error())
	}
	return fmt.Sprintf("client-%x", b)
}
