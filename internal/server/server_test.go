package server

import (
	"net"
	"path/filepath"
	"testing"
	"time"

	"goq/internal/broker"
	"goq/internal/protocol"
	"goq/internal/store"
)

// startTestServer boots a server on an ephemeral port with a temp DB.
func startTestServer(t *testing.T) *Server {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "goq.db"))
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	b := broker.NewBroker(st)
	if err := b.Load(); err != nil {
		t.Fatalf("broker.Load: %v", err)
	}
	cfg := DefaultConfig()
	srv := New(cfg, b, st)
	ready := make(chan struct{})
	go func() {
		close(ready)
		_ = srv.ListenAndServe()
	}()
	<-ready
	// Wait until the listener is bound.
	for i := 0; i < 100; i++ {
		if srv.Addr() != nil {
			break
		}
		time.Sleep(time.Millisecond)
	}
	t.Cleanup(func() { _ = srv.Shutdown(); _ = st.Close() })
	return srv
}

// dial opens a raw TCP client to the server.
func dial(t *testing.T, srv *Server) net.Conn {
	t.Helper()
	c, err := net.Dial("tcp", srv.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func send(t *testing.T, c net.Conn, cmdType string, payload any) {
	t.Helper()
	env, err := protocol.Encode(cmdType, payload)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if err := protocol.WriteFrame(c, env); err != nil {
		t.Fatalf("WriteFrame: %v", err)
	}
}

func recv(t *testing.T, c net.Conn) protocol.Envelope {
	t.Helper()
	_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
	env, err := protocol.ReadFrame(c)
	if err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}
	return env
}

func TestConnectReturnsOK(t *testing.T) {
	srv := startTestServer(t)
	c := dial(t, srv)
	send(t, c, protocol.TypeConnect, protocol.Connect{Role: "producer", ClientID: "p1"})
	if got := recv(t, c); got.Type != protocol.TypeOK {
		t.Errorf("reply = %q, want OK", got.Type)
	}
}

func TestFirstFrameMustBeConnect(t *testing.T) {
	srv := startTestServer(t)
	c := dial(t, srv)
	send(t, c, protocol.TypeSubscribe, protocol.Subscribe{Topic: "x"})
	if got := recv(t, c); got.Type != protocol.TypeError {
		t.Errorf("reply = %q, want ERROR", got.Type)
	}
}

func TestDuplicateClientIDRejected(t *testing.T) {
	srv := startTestServer(t)
	c1 := dial(t, srv)
	send(t, c1, protocol.TypeConnect, protocol.Connect{Role: "consumer", ClientID: "dup"})
	if got := recv(t, c1); got.Type != protocol.TypeOK {
		t.Fatalf("first connect reply = %q, want OK", got.Type)
	}
	c2 := dial(t, srv)
	send(t, c2, protocol.TypeConnect, protocol.Connect{Role: "consumer", ClientID: "dup"})
	if got := recv(t, c2); got.Type != protocol.TypeError {
		t.Errorf("duplicate connect reply = %q, want ERROR", got.Type)
	}
}

// connectClient completes the CONNECT handshake and returns the raw conn.
func connectClient(t *testing.T, srv *Server, role, id string) net.Conn {
	t.Helper()
	c := dial(t, srv)
	send(t, c, protocol.TypeConnect, protocol.Connect{Role: role, ClientID: id})
	if got := recv(t, c); got.Type != protocol.TypeOK {
		t.Fatalf("connect reply = %q, want OK", got.Type)
	}
	return c
}

func TestDeclareThenConflict(t *testing.T) {
	srv := startTestServer(t)
	c := connectClient(t, srv, "producer", "p1")
	send(t, c, protocol.TypeDeclare, protocol.Declare{Topic: "emails", Mode: "roundrobin"})
	if got := recv(t, c); got.Type != protocol.TypeOK {
		t.Fatalf("declare reply = %q, want OK", got.Type)
	}
	send(t, c, protocol.TypeDeclare, protocol.Declare{Topic: "emails", Mode: "broadcast"})
	if got := recv(t, c); got.Type != protocol.TypeError {
		t.Errorf("conflicting declare reply = %q, want ERROR", got.Type)
	}
}

func TestPublishToUndeclaredTopicErrors(t *testing.T) {
	srv := startTestServer(t)
	c := connectClient(t, srv, "producer", "p1")
	send(t, c, protocol.TypePublish, protocol.Publish{Topic: "ghost", Payload: []byte("x")})
	if got := recv(t, c); got.Type != protocol.TypeError {
		t.Errorf("publish reply = %q, want ERROR", got.Type)
	}
}

func TestEndToEndDeliveryAndAck(t *testing.T) {
	srv := startTestServer(t)
	prod := connectClient(t, srv, "producer", "p1")
	send(t, prod, protocol.TypeDeclare, protocol.Declare{Topic: "emails", Mode: "roundrobin"})
	_ = recv(t, prod) // OK

	cons := connectClient(t, srv, "consumer", "c1")
	send(t, cons, protocol.TypeSubscribe, protocol.Subscribe{Topic: "emails"})
	if got := recv(t, cons); got.Type != protocol.TypeOK {
		t.Fatalf("subscribe reply = %q, want OK", got.Type)
	}

	send(t, prod, protocol.TypePublish, protocol.Publish{Topic: "emails", Payload: []byte("hello")})
	if got := recv(t, prod); got.Type != protocol.TypeOK {
		t.Fatalf("publish reply = %q, want OK", got.Type)
	}

	msg := recv(t, cons)
	if msg.Type != protocol.TypeMessage {
		t.Fatalf("consumer got %q, want MESSAGE", msg.Type)
	}
	var m protocol.Message
	if err := msg.Decode(&m); err != nil {
		t.Fatalf("decode message: %v", err)
	}
	if string(m.Payload) != "hello" {
		t.Errorf("payload = %q, want hello", m.Payload)
	}

	send(t, cons, protocol.TypeAck, protocol.Ack{MessageID: m.ID})
	if got := recv(t, cons); got.Type != protocol.TypeOK {
		t.Fatalf("ack reply = %q, want OK", got.Type)
	}

	// Verify persisted state reached 'acked'.
	waitForStatus(t, srv, m.ID, "c1", "acked")
}

// waitForStatus polls the delivery status (async marks) until it matches.
func waitForStatus(t *testing.T, srv *Server, msgID, consumerID, want string) {
	t.Helper()
	for i := 0; i < 200; i++ {
		got, err := srv.store.DeliveryStatus(msgID, consumerID)
		if err == nil && got == want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("delivery %s/%s never reached %q", msgID, consumerID, want)
}
