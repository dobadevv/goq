package client

import (
	"context"
	"fmt"

	"goq/internal/protocol"
)

// Declare creates a topic with the given dispatch mode (ModeBroadcast or
// ModeRoundRobin). Declaring the same topic twice with the same mode is a
// no-op; declaring it with a conflicting mode returns a *ServerError.
func (c *Client) Declare(ctx context.Context, topic, mode string) error {
	return c.request(ctx, protocol.TypeDeclare, protocol.Declare{Topic: topic, Mode: mode})
}

// Publish persists payload to topic. A nil error means the broker durably
// persisted the message before replying. Publishing to a topic that hasn't
// been declared returns a *ServerError.
func (c *Client) Publish(ctx context.Context, topic string, payload []byte) error {
	return c.request(ctx, protocol.TypePublish, protocol.Publish{Topic: topic, Payload: payload})
}

// Subscribe registers as a consumer of topic and blocks, invoking handler
// for every message delivered. It takes over reading the Client's
// connection for the duration of the call — see the Client doc comment for
// the concurrency constraint this implies.
//
// handler returning nil acks the message and continues the loop. handler
// returning a non-nil error stops the loop, and Subscribe returns that
// error wrapped with context, without acking the message that caused it.
// Subscribe also returns nil cleanly when the connection closes or ctx is
// cancelled.
func (c *Client) Subscribe(ctx context.Context, topic string, handler func(Message) error) error {
	c.mu.Lock()
	conn := c.conn
	c.mu.Unlock()
	if conn == nil {
		return errNotConnected
	}

	stop := watchContext(ctx, conn)
	defer stop()

	if err := writeCmd(conn, protocol.TypeSubscribe, protocol.Subscribe{Topic: topic}); err != nil {
		return err
	}
	if err := expectOK(conn); err != nil {
		return err
	}

	for {
		env, err := protocol.ReadFrame(conn)
		if err != nil {
			return nil // connection closed or ctx cancelled
		}
		if env.Type != protocol.TypeMessage {
			continue
		}
		var m protocol.Message
		if err := env.Decode(&m); err != nil {
			return fmt.Errorf("goq: decode message: %w", err)
		}
		if err := handler(Message{ID: m.ID, Topic: m.Topic, Payload: m.Payload}); err != nil {
			return fmt.Errorf("goq: subscribe handler: %w", err)
		}
		if err := writeCmd(conn, protocol.TypeAck, protocol.Ack{MessageID: m.ID}); err != nil {
			return err
		}
	}
}
