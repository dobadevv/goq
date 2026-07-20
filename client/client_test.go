package client_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/dobadevv/goq/client"
	"github.com/dobadevv/goq/internal/auth"
	"github.com/dobadevv/goq/internal/broker"
	"github.com/dobadevv/goq/internal/server"
	"github.com/dobadevv/goq/internal/store"
)

const (
	testUsername = "test-user"
	testPassword = "test-pass"
)

// startBroker starts a real in-process broker on a loopback port for tests
// to dial. It returns both the server (for its address) and the store (so
// tests can assert on persisted delivery/ack state). It also provisions the
// standard test credentials (testUsername/testPassword) as a super admin,
// since the broker rejects any CONNECT without valid credentials.
func startBroker(t *testing.T) (*server.Server, *store.Store) {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "goq.db"))
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	hash, err := auth.HashPassword(testPassword)
	if err != nil {
		t.Fatalf("auth.HashPassword: %v", err)
	}
	if err := st.UpsertUser(testUsername, hash, true); err != nil {
		t.Fatalf("UpsertUser: %v", err)
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
	c := client.New(srv.Addr().String(), client.WithClientID("c1"), client.WithCredentials(testUsername, testPassword))
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

	first := client.New(addr, client.WithClientID("dup"), client.WithCredentials(testUsername, testPassword))
	if err := first.Connect(context.Background()); err != nil {
		t.Fatalf("first Connect: %v", err)
	}
	defer func() { _ = first.Close() }()

	second := client.New(addr, client.WithClientID("dup"), client.WithCredentials(testUsername, testPassword))
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

	a := client.New(addr, client.WithCredentials(testUsername, testPassword))
	if err := a.Connect(context.Background()); err != nil {
		t.Fatalf("first Connect: %v", err)
	}
	defer func() { _ = a.Close() }()

	b := client.New(addr, client.WithCredentials(testUsername, testPassword))
	if err := b.Connect(context.Background()); err != nil {
		t.Fatalf("second Connect: %v", err)
	}
	defer func() { _ = b.Close() }()
}

func TestConnectRejectsWrongPassword(t *testing.T) {
	srv, _ := startBroker(t)
	c := client.New(srv.Addr().String(), client.WithCredentials(testUsername, "wrong-password"))
	err := c.Connect(context.Background())
	if err == nil {
		t.Fatal("Connect: want error for wrong password, got nil")
	}
	var serverErr *client.ServerError
	if !errors.As(err, &serverErr) {
		t.Fatalf("Connect error = %v, want *client.ServerError", err)
	}
	if serverErr.Reason != "invalid credentials" {
		t.Errorf("Reason = %q, want %q", serverErr.Reason, "invalid credentials")
	}
}
