// Package server hosts the goq TCP broker: it accepts connections, runs a
// read and writer goroutine per connection, and translates protocol commands
// into broker dispatch and store persistence.
package server

import (
	"log/slog"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/dobadevv/goq/internal/broker"
	"github.com/dobadevv/goq/internal/store"
)

// Config controls server behaviour.
type Config struct {
	Host                string
	Port                int
	OutboundCapacity    int
	SlowConsumerTimeout time.Duration
}

// DefaultConfig returns safe defaults for local use.
func DefaultConfig() Config {
	return Config{
		Host:                "127.0.0.1",
		Port:                0,
		OutboundCapacity:    256,
		SlowConsumerTimeout: 5 * time.Second,
	}
}

// Server owns the listener and the set of live connections.
type Server struct {
	cfg      Config
	broker   *broker.Broker
	store    *store.Store
	clients  *clientRegistry
	listener net.Listener
	mu       sync.Mutex
	conns    map[*connection]struct{}
	wg       sync.WaitGroup
	closed   bool
}

func New(cfg Config, b *broker.Broker, s *store.Store) *Server {
	return &Server{
		cfg:     cfg,
		broker:  b,
		store:   s,
		clients: newClientRegistry(),
		conns:   make(map[*connection]struct{}),
	}
}

// ListenAndServe binds the configured address and serves until Shutdown.
func (s *Server) ListenAndServe() error {
	addr := net.JoinHostPort(s.cfg.Host, strconv.Itoa(s.cfg.Port))
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.listener = ln
	s.mu.Unlock()

	for {
		netConn, err := ln.Accept()
		if err != nil {
			s.mu.Lock()
			closed := s.closed
			s.mu.Unlock()
			if closed {
				return nil
			}
			return err
		}
		s.serveConn(netConn)
	}
}

// Addr returns the bound listener address, or nil before binding.
func (s *Server) Addr() net.Addr {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.listener == nil {
		return nil
	}
	return s.listener.Addr()
}

func (s *Server) serveConn(netConn net.Conn) {
	c := newConnection(s, netConn)
	s.mu.Lock()
	s.conns[c] = struct{}{}
	s.mu.Unlock()

	s.wg.Add(2)
	go func() { defer s.wg.Done(); c.writeLoop() }()
	go func() { defer s.wg.Done(); c.readLoop() }()
}

func (s *Server) removeConn(c *connection) {
	s.mu.Lock()
	delete(s.conns, c)
	s.mu.Unlock()
}

// Shutdown stops accepting connections and closes all live connections.
func (s *Server) Shutdown() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	ln := s.listener
	conns := make([]*connection, 0, len(s.conns))
	for c := range s.conns {
		conns = append(conns, c)
	}
	s.mu.Unlock()

	if ln != nil {
		_ = ln.Close()
	}
	for _, c := range conns {
		c.close()
	}
	s.wg.Wait()
	slog.Info("server shut down")
	return nil
}

// clientRegistry enforces unique client IDs across live connections.
type clientRegistry struct {
	mu  sync.Mutex
	ids map[string]struct{}
}

func newClientRegistry() *clientRegistry {
	return &clientRegistry{ids: make(map[string]struct{})}
}

// add reserves an id, returning false if it is already in use.
func (r *clientRegistry) add(id string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.ids[id]; ok {
		return false
	}
	r.ids[id] = struct{}{}
	return true
}

func (r *clientRegistry) remove(id string) {
	r.mu.Lock()
	delete(r.ids, id)
	r.mu.Unlock()
}
