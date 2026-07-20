// Package main's config.go loads goqd's runtime settings from environment
// variables, replacing the previous CLI-flag configuration.
package main

import (
	"fmt"
	"strconv"
	"time"
)

// config holds goqd's runtime settings.
type config struct {
	Host                string
	Port                int
	DBPath              string
	SlowConsumerTimeout time.Duration
	Username            string
	Password            string
}

// loadConfig reads goqd's settings from GOQD_*/GOQ_* environment variables
// via getenv, applying defaults for any optional var that is unset or
// empty. GOQ_USERNAME and GOQ_PASSWORD have no default: they are required,
// since they provision the sole super-admin account goqd enforces on every
// CONNECT.
func loadConfig(getenv func(string) string) (config, error) {
	cfg := config{
		Host:                "127.0.0.1",
		Port:                7711,
		DBPath:              "goq.db",
		SlowConsumerTimeout: 5 * time.Second,
	}

	if v := getenv("GOQD_HOST"); v != "" {
		cfg.Host = v
	}

	if v := getenv("GOQD_PORT"); v != "" {
		port, err := strconv.Atoi(v)
		if err != nil {
			return config{}, fmt.Errorf("invalid GOQD_PORT: %w", err)
		}
		cfg.Port = port
	}

	if v := getenv("GOQD_DB_PATH"); v != "" {
		cfg.DBPath = v
	}

	if v := getenv("GOQD_SLOW_CONSUMER_TIMEOUT"); v != "" {
		timeout, err := time.ParseDuration(v)
		if err != nil {
			return config{}, fmt.Errorf("invalid GOQD_SLOW_CONSUMER_TIMEOUT: %w", err)
		}
		cfg.SlowConsumerTimeout = timeout
	}

	username := getenv("GOQ_USERNAME")
	if username == "" {
		return config{}, fmt.Errorf("GOQ_USERNAME is required")
	}
	cfg.Username = username

	password := getenv("GOQ_PASSWORD")
	if password == "" {
		return config{}, fmt.Errorf("GOQ_PASSWORD is required")
	}
	cfg.Password = password

	return cfg, nil
}
