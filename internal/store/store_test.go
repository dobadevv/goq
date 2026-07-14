package store

import (
	"path/filepath"
	"testing"
)

func openTemp(t *testing.T) *Store {
	t.Helper()
	s, err := Open(filepath.Join(t.TempDir(), "goq.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestDeclareAndLoadTopics(t *testing.T) {
	s := openTemp(t)
	if err := s.DeclareTopic("emails", "roundrobin"); err != nil {
		t.Fatalf("DeclareTopic: %v", err)
	}
	if err := s.DeclareTopic("events", "broadcast"); err != nil {
		t.Fatalf("DeclareTopic: %v", err)
	}
	topics, err := s.LoadTopics()
	if err != nil {
		t.Fatalf("LoadTopics: %v", err)
	}
	if topics["emails"] != "roundrobin" || topics["events"] != "broadcast" {
		t.Errorf("topics = %v, want emails=roundrobin events=broadcast", topics)
	}
}

func TestTopicsSurviveReopen(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "goq.db")
	s1, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := s1.DeclareTopic("emails", "roundrobin"); err != nil {
		t.Fatalf("DeclareTopic: %v", err)
	}
	_ = s1.Close()

	s2, err := Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s2.Close()
	topics, err := s2.LoadTopics()
	if err != nil {
		t.Fatalf("LoadTopics: %v", err)
	}
	if topics["emails"] != "roundrobin" {
		t.Errorf("after reopen topics = %v, want emails=roundrobin", topics)
	}
}

func TestMessageAndDeliveryLifecycle(t *testing.T) {
	s := openTemp(t)
	if err := s.DeclareTopic("emails", "roundrobin"); err != nil {
		t.Fatalf("DeclareTopic: %v", err)
	}
	if err := s.InsertMessage("m1", "emails", []byte("hi")); err != nil {
		t.Fatalf("InsertMessage: %v", err)
	}
	if err := s.InsertDelivery("m1", "c1"); err != nil {
		t.Fatalf("InsertDelivery: %v", err)
	}
	assertStatus(t, s, "m1", "c1", "queued")

	if err := s.MarkDelivered("m1", "c1"); err != nil {
		t.Fatalf("MarkDelivered: %v", err)
	}
	assertStatus(t, s, "m1", "c1", "delivered")

	if err := s.MarkAcked("m1", "c1"); err != nil {
		t.Fatalf("MarkAcked: %v", err)
	}
	assertStatus(t, s, "m1", "c1", "acked")
}

func TestMarkDeliveredDoesNotRegressAcked(t *testing.T) {
	s := openTemp(t)
	_ = s.DeclareTopic("emails", "roundrobin")
	_ = s.InsertMessage("m1", "emails", []byte("hi"))
	_ = s.InsertDelivery("m1", "c1")
	// Ack arrives before the delivered-mark write (a real race in the server).
	if err := s.MarkAcked("m1", "c1"); err != nil {
		t.Fatalf("MarkAcked: %v", err)
	}
	if err := s.MarkDelivered("m1", "c1"); err != nil {
		t.Fatalf("MarkDelivered: %v", err)
	}
	assertStatus(t, s, "m1", "c1", "acked") // must not regress to delivered
}

func assertStatus(t *testing.T, s *Store, msgID, consumerID, want string) {
	t.Helper()
	got, err := s.DeliveryStatus(msgID, consumerID)
	if err != nil {
		t.Fatalf("DeliveryStatus: %v", err)
	}
	if got != want {
		t.Errorf("status = %q, want %q", got, want)
	}
}
