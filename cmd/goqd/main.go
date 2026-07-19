// Command goqd runs the goq message broker server.
package main

import (
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/dobadevv/goq/internal/broker"
	"github.com/dobadevv/goq/internal/server"
	"github.com/dobadevv/goq/internal/store"
)

func main() {
	cfg, err := loadConfig(os.Getenv)
	if err != nil {
		slog.Error("load config", "err", err)
		os.Exit(1)
	}

	st, err := store.Open(cfg.DBPath)
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

	srvCfg := server.DefaultConfig()
	srvCfg.Host = cfg.Host
	srvCfg.Port = cfg.Port
	srvCfg.SlowConsumerTimeout = cfg.SlowConsumerTimeout
	srv := server.New(srvCfg, b, st)

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-stop
		slog.Info("shutting down")
		_ = srv.Shutdown()
	}()

	slog.Info("goqd listening", "host", cfg.Host, "port", cfg.Port, "db", cfg.DBPath)
	if err := srv.ListenAndServe(); err != nil {
		slog.Error("serve", "err", err)
		os.Exit(1)
	}
}
