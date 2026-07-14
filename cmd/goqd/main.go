// Command goqd runs the goq message broker server.
package main

import (
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"goq/internal/broker"
	"goq/internal/server"
	"goq/internal/store"
)

func main() {
	host := flag.String("host", "127.0.0.1", "host/interface to bind")
	port := flag.Int("port", 7711, "TCP port to listen on")
	dbPath := flag.String("db-path", "goq.db", "path to the SQLite database file")
	slowTimeout := flag.Duration("slow-consumer-timeout", 5*time.Second,
		"disconnect a consumer whose send queue stays full this long")
	flag.Parse()

	st, err := store.Open(*dbPath)
	if err != nil {
		slog.Error("open store", "err", err)
		os.Exit(1)
	}
	defer st.Close()

	b := broker.NewBroker(st)
	if err := b.Load(); err != nil {
		slog.Error("load topics", "err", err)
		os.Exit(1)
	}

	cfg := server.DefaultConfig()
	cfg.Host = *host
	cfg.Port = *port
	cfg.SlowConsumerTimeout = *slowTimeout
	srv := server.New(cfg, b, st)

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-stop
		slog.Info("shutting down")
		_ = srv.Shutdown()
	}()

	slog.Info("goqd listening", "host", *host, "port", *port, "db", *dbPath)
	if err := srv.ListenAndServe(); err != nil {
		slog.Error("serve", "err", err)
		os.Exit(1)
	}
}
