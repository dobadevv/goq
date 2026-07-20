// Package protocol defines the goq wire protocol: length-prefixed JSON frames
// and the command/payload types exchanged between clients and the broker.
package protocol

import "encoding/json"

// Command type discriminators carried in Envelope.Type.
const (
	TypeConnect   = "CONNECT"
	TypeDeclare   = "DECLARE"
	TypePublish   = "PUBLISH"
	TypeSubscribe = "SUBSCRIBE"
	TypeAck       = "ACK"
	TypeMessage   = "MESSAGE"
	TypeOK        = "OK"
	TypeError     = "ERROR"
)

// Dispatch modes a topic may be declared with.
const (
	ModeBroadcast  = "broadcast"
	ModeRoundRobin = "roundrobin"
)

// Envelope is the outer frame: a command type plus its JSON payload.
type Envelope struct {
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

type Connect struct {
	Role     string `json:"role"`
	ClientID string `json:"client_id"`
	Username string `json:"username"`
	Password string `json:"password"`
}

type Declare struct {
	Topic string `json:"topic"`
	Mode  string `json:"mode"`
}

type Publish struct {
	Topic   string `json:"topic"`
	Payload []byte `json:"payload"`
}

type Subscribe struct {
	Topic string `json:"topic"`
}

type Ack struct {
	MessageID string `json:"message_id"`
}

type Message struct {
	ID      string `json:"id"`
	Topic   string `json:"topic"`
	Payload []byte `json:"payload"`
}

type Error struct {
	Reason string `json:"reason"`
}

// Encode wraps a payload struct into an Envelope. A nil payload yields an
// Envelope with only its Type set (used for OK).
func Encode(cmdType string, payload any) (Envelope, error) {
	if payload == nil {
		return Envelope{Type: cmdType}, nil
	}
	b, err := json.Marshal(payload)
	if err != nil {
		return Envelope{}, err
	}
	return Envelope{Type: cmdType, Payload: b}, nil
}

// Decode unmarshals the envelope payload into v.
func (e Envelope) Decode(v any) error {
	return json.Unmarshal(e.Payload, v)
}
