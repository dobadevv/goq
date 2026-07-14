package broker

import (
	"errors"
	"sync"
)

// ErrModeConflict is returned when redeclaring an existing topic with a
// different dispatch mode.
var ErrModeConflict = errors.New("broker: topic already declared with a different mode")

// TopicStore persists topic declarations so they survive a restart.
type TopicStore interface {
	DeclareTopic(name, mode string) error
	LoadTopics() (map[string]string, error)
}

// Broker owns the set of declared topics and gates their creation.
type Broker struct {
	mu     sync.Mutex
	topics map[string]*Topic
	store  TopicStore
}

func NewBroker(store TopicStore) *Broker {
	return &Broker{topics: make(map[string]*Topic), store: store}
}

// Load restores declared topics from the store into memory.
func (b *Broker) Load() error {
	modes, err := b.store.LoadTopics()
	if err != nil {
		return err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	for name, mode := range modes {
		b.topics[name] = NewTopic(name, mode)
	}
	return nil
}

// Declare creates and persists a new topic. Redeclaring with the same mode is a
// no-op; redeclaring with a different mode returns ErrModeConflict.
func (b *Broker) Declare(name, mode string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if existing, ok := b.topics[name]; ok {
		if existing.Mode() != mode {
			return ErrModeConflict
		}
		return nil
	}
	if err := b.store.DeclareTopic(name, mode); err != nil {
		return err
	}
	b.topics[name] = NewTopic(name, mode)
	return nil
}

// Topic returns the topic by name, if declared.
func (b *Broker) Topic(name string) (*Topic, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	t, ok := b.topics[name]
	return t, ok
}
