package store

import (
	"errors"
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
	defer func() { _ = s2.Close() }()
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

func TestUpsertAndGetUser(t *testing.T) {
	s := openTemp(t)
	if err := s.UpsertUser("alice", "hash1", true); err != nil {
		t.Fatalf("UpsertUser: %v", err)
	}
	u, err := s.GetUser("alice")
	if err != nil {
		t.Fatalf("GetUser: %v", err)
	}
	want := User{Username: "alice", PasswordHash: "hash1", IsSuperAdmin: true}
	if u != want {
		t.Errorf("GetUser = %+v, want %+v", u, want)
	}
}

func TestUpsertUserOverwritesExistingHash(t *testing.T) {
	s := openTemp(t)
	if err := s.UpsertUser("alice", "hash1", true); err != nil {
		t.Fatalf("UpsertUser (create): %v", err)
	}
	if err := s.UpsertUser("alice", "hash2", true); err != nil {
		t.Fatalf("UpsertUser (update): %v", err)
	}
	u, err := s.GetUser("alice")
	if err != nil {
		t.Fatalf("GetUser: %v", err)
	}
	if u.PasswordHash != "hash2" {
		t.Errorf("PasswordHash = %q, want %q (rotation)", u.PasswordHash, "hash2")
	}
}

func TestGetUserNotFound(t *testing.T) {
	s := openTemp(t)
	_, err := s.GetUser("ghost")
	if !errors.Is(err, ErrUserNotFound) {
		t.Errorf("GetUser error = %v, want ErrUserNotFound", err)
	}
}
