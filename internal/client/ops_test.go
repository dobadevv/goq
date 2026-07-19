package client_test

import (
	"context"
	"errors"
	"testing"
	"time"

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

func TestSubscribeReceivesAndAcksMessage(t *testing.T) {
	srv, st := startBroker(t)
	addr := srv.Addr().String()

	producer := client.New(addr, client.WithClientID("producer"))
	if err := producer.Connect(context.Background()); err != nil {
		t.Fatalf("producer Connect: %v", err)
	}
	defer producer.Close()
	if err := producer.Declare(context.Background(), "emails", client.ModeBroadcast); err != nil {
		t.Fatalf("Declare: %v", err)
	}

	consumer := client.New(addr, client.WithClientID("consumer"))
	if err := consumer.Connect(context.Background()); err != nil {
		t.Fatalf("consumer Connect: %v", err)
	}
	defer consumer.Close()

	received := make(chan client.Message, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- consumer.Subscribe(ctx, "emails", func(m client.Message) error {
			received <- m
			return nil
		})
	}()
	time.Sleep(100 * time.Millisecond) // let the subscription attach

	if err := producer.Publish(context.Background(), "emails", []byte("hello-world")); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	var msg client.Message
	select {
	case msg = <-received:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for message")
	}
	if string(msg.Payload) != "hello-world" {
		t.Errorf("Payload = %q, want %q", msg.Payload, "hello-world")
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		status, err := st.DeliveryStatus(msg.ID, "consumer")
		if err != nil {
			t.Fatalf("DeliveryStatus: %v", err)
		}
		if status == "acked" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("status = %q, want %q", status, "acked")
		}
		time.Sleep(10 * time.Millisecond)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Subscribe returned error after cancel: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for Subscribe to return after cancel")
	}
}

func TestSubscribeHandlerErrorStopsLoop(t *testing.T) {
	srv, _ := startBroker(t)
	addr := srv.Addr().String()

	producer := client.New(addr, client.WithClientID("producer"))
	if err := producer.Connect(context.Background()); err != nil {
		t.Fatalf("producer Connect: %v", err)
	}
	defer producer.Close()
	if err := producer.Declare(context.Background(), "emails", client.ModeBroadcast); err != nil {
		t.Fatalf("Declare: %v", err)
	}

	consumer := client.New(addr, client.WithClientID("consumer"))
	if err := consumer.Connect(context.Background()); err != nil {
		t.Fatalf("consumer Connect: %v", err)
	}
	defer consumer.Close()

	wantErr := errors.New("boom")
	done := make(chan error, 1)
	go func() {
		done <- consumer.Subscribe(context.Background(), "emails", func(m client.Message) error {
			return wantErr
		})
	}()
	time.Sleep(100 * time.Millisecond) // let the subscription attach

	if err := producer.Publish(context.Background(), "emails", []byte("hello")); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	select {
	case err := <-done:
		if !errors.Is(err, wantErr) {
			t.Errorf("Subscribe error = %v, want wrapping %v", err, wantErr)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for Subscribe to return")
	}
}
