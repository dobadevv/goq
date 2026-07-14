package broker

import (
	"sync"
	"testing"
)

// recordingObserver records the messages it receives; failing observers return
// an error from Notify.
type recordingObserver struct {
	mu   sync.Mutex
	id   string
	got  []string
	fail bool
}

func (o *recordingObserver) ID() string { return o.id }
func (o *recordingObserver) Notify(m Message) error {
	if o.fail {
		return errTest
	}
	o.mu.Lock()
	o.got = append(o.got, m.ID)
	o.mu.Unlock()
	return nil
}

var errTest = &testError{}

type testError struct{}

func (*testError) Error() string { return "notify failed" }

func TestBroadcastNotifiesAll(t *testing.T) {
	top := NewTopic("events", "broadcast")
	a := &recordingObserver{id: "a"}
	b := &recordingObserver{id: "b"}
	top.Attach(a)
	top.Attach(b)

	notified := top.Publish(Message{ID: "m1"})
	if len(notified) != 2 {
		t.Fatalf("notified %d, want 2", len(notified))
	}
	if len(a.got) != 1 || len(b.got) != 1 {
		t.Errorf("a.got=%v b.got=%v, want each len 1", a.got, b.got)
	}
}

func TestRoundRobinRotates(t *testing.T) {
	top := NewTopic("emails", "roundrobin")
	a := &recordingObserver{id: "a"}
	b := &recordingObserver{id: "b"}
	top.Attach(a)
	top.Attach(b)

	top.Publish(Message{ID: "m1"})
	top.Publish(Message{ID: "m2"})
	top.Publish(Message{ID: "m3"})

	// a gets m1, m3; b gets m2 (rotation).
	if len(a.got) != 2 || len(b.got) != 1 {
		t.Errorf("a.got=%v b.got=%v, want a=2 b=1", a.got, b.got)
	}
}

func TestDetachStopsDelivery(t *testing.T) {
	top := NewTopic("events", "broadcast")
	a := &recordingObserver{id: "a"}
	top.Attach(a)
	top.Detach("a")
	notified := top.Publish(Message{ID: "m1"})
	if len(notified) != 0 || len(a.got) != 0 {
		t.Errorf("after detach notified=%d a.got=%v, want none", len(notified), a.got)
	}
}

func TestPublishExcludesFailedObservers(t *testing.T) {
	top := NewTopic("events", "broadcast")
	ok := &recordingObserver{id: "ok"}
	bad := &recordingObserver{id: "bad", fail: true}
	top.Attach(ok)
	top.Attach(bad)
	notified := top.Publish(Message{ID: "m1"})
	if len(notified) != 1 || notified[0].ID() != "ok" {
		t.Errorf("notified=%v, want just [ok]", notified)
	}
}

// fakeTopicStore is an in-memory TopicStore for broker tests.
type fakeTopicStore struct {
	topics   map[string]string
	declares int
}

func newFakeStore() *fakeTopicStore { return &fakeTopicStore{topics: map[string]string{}} }

func (f *fakeTopicStore) DeclareTopic(name, mode string) error {
	f.declares++
	f.topics[name] = mode
	return nil
}
func (f *fakeTopicStore) LoadTopics() (map[string]string, error) {
	out := make(map[string]string, len(f.topics))
	for k, v := range f.topics {
		out[k] = v
	}
	return out, nil
}

func TestDeclareCreatesTopic(t *testing.T) {
	fs := newFakeStore()
	b := NewBroker(fs)
	if err := b.Declare("emails", "roundrobin"); err != nil {
		t.Fatalf("Declare: %v", err)
	}
	top, ok := b.Topic("emails")
	if !ok || top.Mode() != "roundrobin" {
		t.Fatalf("Topic = %v ok=%v, want roundrobin", top, ok)
	}
	if fs.declares != 1 {
		t.Errorf("store declares = %d, want 1", fs.declares)
	}
}

func TestDeclareIdempotentSameMode(t *testing.T) {
	fs := newFakeStore()
	b := NewBroker(fs)
	_ = b.Declare("emails", "roundrobin")
	if err := b.Declare("emails", "roundrobin"); err != nil {
		t.Fatalf("second Declare: %v", err)
	}
	if fs.declares != 1 {
		t.Errorf("store declares = %d, want 1 (idempotent)", fs.declares)
	}
}

func TestDeclareConflictingModeErrors(t *testing.T) {
	b := NewBroker(newFakeStore())
	_ = b.Declare("emails", "roundrobin")
	if err := b.Declare("emails", "broadcast"); err != ErrModeConflict {
		t.Fatalf("err = %v, want ErrModeConflict", err)
	}
}

func TestLoadRestoresTopics(t *testing.T) {
	fs := newFakeStore()
	fs.topics["emails"] = "roundrobin"
	b := NewBroker(fs)
	if err := b.Load(); err != nil {
		t.Fatalf("Load: %v", err)
	}
	top, ok := b.Topic("emails")
	if !ok || top.Mode() != "roundrobin" {
		t.Errorf("after Load Topic = %v ok=%v, want roundrobin", top, ok)
	}
}
