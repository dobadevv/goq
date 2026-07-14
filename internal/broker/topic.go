package broker

import "sync"

// Topic is the Observer-pattern Subject. Its dispatch mode is fixed at creation.
type Topic struct {
	mu        sync.Mutex
	name      string
	mode      string
	observers []Observer
	rrIndex   int
}

// NewTopic creates a topic with a fixed dispatch mode ("broadcast" or
// "roundrobin").
func NewTopic(name, mode string) *Topic {
	return &Topic{name: name, mode: mode}
}

func (t *Topic) Mode() string { return t.mode }

// Attach registers an observer.
func (t *Topic) Attach(o Observer) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.observers = append(t.observers, o)
}

// Detach removes the observer with the given id, if present.
func (t *Topic) Detach(id string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for i, o := range t.observers {
		if o.ID() == id {
			t.observers = append(t.observers[:i], t.observers[i+1:]...)
			return
		}
	}
}

// Publish snapshots the target observers under the lock, then notifies them
// outside the lock so a slow observer cannot stall dispatch or rotation. It
// returns the observers whose Notify succeeded.
func (t *Topic) Publish(msg Message) []Observer {
	targets := t.selectTargets()
	var notified []Observer
	for _, o := range targets {
		if err := o.Notify(msg); err == nil {
			notified = append(notified, o)
		}
	}
	return notified
}

func (t *Topic) selectTargets() []Observer {
	t.mu.Lock()
	defer t.mu.Unlock()
	if len(t.observers) == 0 {
		return nil
	}
	if t.mode == "roundrobin" {
		o := t.observers[t.rrIndex%len(t.observers)]
		t.rrIndex = (t.rrIndex + 1) % len(t.observers)
		return []Observer{o}
	}
	// broadcast: copy so callers never touch the live slice.
	snapshot := make([]Observer, len(t.observers))
	copy(snapshot, t.observers)
	return snapshot
}
