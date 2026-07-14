// Package broker implements goq's in-memory topic registry and Observer-pattern
// message dispatch. It depends only on a small TopicStore interface for
// durability of topic declarations.
package broker

// Message is a unit of work dispatched to observers.
type Message struct {
	ID      string
	Topic   string
	Payload []byte
}

// Observer is a subscriber. Notify must not block indefinitely; it returns an
// error when the observer is dead or too slow to accept the message.
type Observer interface {
	ID() string
	Notify(msg Message) error
}
