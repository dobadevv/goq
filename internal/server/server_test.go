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
