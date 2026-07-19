package client_test

import (
	"context"
	"errors"
	"testing"

	"goq/internal/client"
)

func TestDeclareAndPublish(t *testing.T) {
	srv, _ := startBroker(t)
	c := client.New(srv.Addr().String(), client.WithClientID("producer"))
	if err := c.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer c.Close()

	if err := c.Declare(context.Background(), "emails", client.ModeBroadcast); err != nil {
		t.Fatalf("Declare: %v", err)
	}
	if err := c.Publish(context.Background(), "emails", []byte("hello")); err != nil {
		t.Fatalf("Publish: %v", err)
	}
}

func TestDeclareModeConflict(t *testing.T) {
	srv, _ := startBroker(t)
	c := client.New(srv.Addr().String(), client.WithClientID("producer"))
	if err := c.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer c.Close()

	if err := c.Declare(context.Background(), "emails", client.ModeBroadcast); err != nil {
		t.Fatalf("first Declare: %v", err)
	}
	err := c.Declare(context.Background(), "emails", client.ModeRoundRobin)
	if err == nil {
		t.Fatal("second Declare: want error, got nil")
	}
	var serverErr *client.ServerError
	if !errors.As(err, &serverErr) {
		t.Fatalf("second Declare error = %v, want *client.ServerError", err)
	}
}

func TestPublishToUndeclaredTopic(t *testing.T) {
	srv, _ := startBroker(t)
	c := client.New(srv.Addr().String(), client.WithClientID("producer"))
	if err := c.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer c.Close()

	err := c.Publish(context.Background(), "missing", []byte("hello"))
	if err == nil {
		t.Fatal("Publish: want error, got nil")
	}
	var serverErr *client.ServerError
	if !errors.As(err, &serverErr) {
		t.Fatalf("Publish error = %v, want *client.ServerError", err)
	}
}
