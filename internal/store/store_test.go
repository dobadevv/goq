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
