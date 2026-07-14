package main

import (
	"bytes"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"goq/internal/broker"
	"goq/internal/server"
	"goq/internal/store"
)

// syncBuffer guards a bytes.Buffer with a mutex: the subscriber goroutine
// writes to it while the test goroutine polls its contents, and plain
// bytes.Buffer is not safe for concurrent use.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func startBroker(t *testing.T) *server.Server {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "goq.db"))
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	b := broker.NewBroker(st)
	if err := b.Load(); err != nil {
		t.Fatalf("broker.Load: %v", err)
	}
	srv := server.New(server.DefaultConfig(), b, st)
	go func() { _ = srv.ListenAndServe() }()
	for i := 0; i < 200 && srv.Addr() == nil; i++ {
		time.Sleep(time.Millisecond)
	}
	t.Cleanup(func() { _ = srv.Shutdown(); _ = st.Close() })
	return srv
}

func TestCLIEndToEnd(t *testing.T) {
	srv := startBroker(t)
	addr := srv.Addr().String()

	if err := runDeclare(addr, "d1", "emails", "broadcast"); err != nil {
		t.Fatalf("declare: %v", err)
	}

	var out syncBuffer
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		_ = runSubscribe(addr, "sub1", "emails", &out, stop)
		close(done)
	}()
	time.Sleep(100 * time.Millisecond) // let the subscription attach

	if err := runPublish(addr, "pub1", "emails", []byte("hello-world")); err != nil {
		t.Fatalf("publish: %v", err)
	}

	// Wait for the subscriber to print the message.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && !strings.Contains(out.String(), "hello-world") {
		time.Sleep(10 * time.Millisecond)
	}
	close(stop)
	<-done
	if !strings.Contains(out.String(), "hello-world") {
		t.Errorf("subscriber output = %q, want it to contain hello-world", out.String())
	}
}
