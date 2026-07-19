package client

import (
	"context"

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
