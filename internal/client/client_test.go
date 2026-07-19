package client_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"goq/internal/broker"
	"goq/internal/client"
	"goq/internal/server"
	"goq/internal/store"
)

// startBroker starts a real in-process broker on a loopback port for tests
// to dial. It returns both the server (for its address) and the store (so
// tests can assert on persisted delivery/ack state).
func startBroker(t *testing.T) (*server.Server, *store.Store) {
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
	return srv, st
}

func TestConnectAndClose(t *testing.T) {
	srv, _ := startBroker(t)
	c := client.New(srv.Addr().String(), client.WithClientID("c1"))
	if err := c.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestConnectDuplicateClientID(t *testing.T) {
	srv, _ := startBroker(t)
	addr := srv.Addr().String()

	first := client.New(addr, client.WithClientID("dup"))
	if err := first.Connect(context.Background()); err != nil {
		t.Fatalf("first Connect: %v", err)
	}
	defer first.Close()

	second := client.New(addr, client.WithClientID("dup"))
	err := second.Connect(context.Background())
	if err == nil {
		t.Fatal("second Connect: want error, got nil")
	}
	var serverErr *client.ServerError
	if !errors.As(err, &serverErr) {
		t.Fatalf("second Connect error = %v, want *client.ServerError", err)
	}
	if serverErr.Reason != "client_id already in use" {
		t.Errorf("Reason = %q, want %q", serverErr.Reason, "client_id already in use")
	}
}

func TestConnectDefaultClientID(t *testing.T) {
	srv, _ := startBroker(t)
	addr := srv.Addr().String()

	a := client.New(addr)
	if err := a.Connect(context.Background()); err != nil {
		t.Fatalf("first Connect: %v", err)
	}
	defer a.Close()

	b := client.New(addr)
	if err := b.Connect(context.Background()); err != nil {
		t.Fatalf("second Connect: %v", err)
	}
	defer b.Close()
}
