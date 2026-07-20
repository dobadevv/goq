package main

import (
	"strings"
	"testing"
	"time"
)

func fakeGetenv(values map[string]string) func(string) string {
	return func(key string) string {
		return values[key]
	}
}

// baseEnv returns the minimal env map that satisfies loadConfig's required
// vars, for tests that only care about the optional GOQD_* overrides.
func baseEnv(overrides map[string]string) map[string]string {
	env := map[string]string{
		"GOQ_USERNAME": "admin",
		"GOQ_PASSWORD": "s3cret",
	}
	for k, v := range overrides {
		env[k] = v
	}
	return env
}

func TestLoadConfigDefaults(t *testing.T) {
	cfg, err := loadConfig(fakeGetenv(baseEnv(nil)))
	if err != nil {
		t.Fatalf("loadConfig() error = %v, want nil", err)
	}
	want := config{
		Host:                "127.0.0.1",
		Port:                7711,
		DBPath:              "goq.db",
		SlowConsumerTimeout: 5 * time.Second,
		Username:            "admin",
		Password:            "s3cret",
	}
	if cfg != want {
		t.Errorf("loadConfig() = %+v, want %+v", cfg, want)
	}
}

func TestLoadConfigOverrides(t *testing.T) {
	cfg, err := loadConfig(fakeGetenv(baseEnv(map[string]string{
		"GOQD_HOST":                  "0.0.0.0",
		"GOQD_PORT":                  "9999",
		"GOQD_DB_PATH":               "/tmp/custom.db",
		"GOQD_SLOW_CONSUMER_TIMEOUT": "10s",
	})))
	if err != nil {
		t.Fatalf("loadConfig() error = %v, want nil", err)
	}
	want := config{
		Host:                "0.0.0.0",
		Port:                9999,
		DBPath:              "/tmp/custom.db",
		SlowConsumerTimeout: 10 * time.Second,
		Username:            "admin",
		Password:            "s3cret",
	}
	if cfg != want {
		t.Errorf("loadConfig() = %+v, want %+v", cfg, want)
	}
}

func TestLoadConfigInvalidPort(t *testing.T) {
	_, err := loadConfig(fakeGetenv(baseEnv(map[string]string{"GOQD_PORT": "not-a-number"})))
	if err == nil {
		t.Fatal("loadConfig() error = nil, want error for invalid GOQD_PORT")
	}
	if !strings.Contains(err.Error(), "GOQD_PORT") {
		t.Errorf("loadConfig() error = %v, want it to mention GOQD_PORT", err)
	}
}

func TestLoadConfigInvalidSlowConsumerTimeout(t *testing.T) {
	_, err := loadConfig(fakeGetenv(baseEnv(map[string]string{"GOQD_SLOW_CONSUMER_TIMEOUT": "not-a-duration"})))
	if err == nil {
		t.Fatal("loadConfig() error = nil, want error for invalid GOQD_SLOW_CONSUMER_TIMEOUT")
	}
	if !strings.Contains(err.Error(), "GOQD_SLOW_CONSUMER_TIMEOUT") {
		t.Errorf("loadConfig() error = %v, want it to mention GOQD_SLOW_CONSUMER_TIMEOUT", err)
	}
}

func TestLoadConfigMissingUsername(t *testing.T) {
	_, err := loadConfig(fakeGetenv(map[string]string{"GOQ_PASSWORD": "s3cret"}))
	if err == nil {
		t.Fatal("loadConfig() error = nil, want error for missing GOQ_USERNAME")
	}
	if !strings.Contains(err.Error(), "GOQ_USERNAME") {
		t.Errorf("loadConfig() error = %v, want it to mention GOQ_USERNAME", err)
	}
}

func TestLoadConfigMissingPassword(t *testing.T) {
	_, err := loadConfig(fakeGetenv(map[string]string{"GOQ_USERNAME": "admin"}))
	if err == nil {
		t.Fatal("loadConfig() error = nil, want error for missing GOQ_PASSWORD")
	}
	if !strings.Contains(err.Error(), "GOQ_PASSWORD") {
		t.Errorf("loadConfig() error = %v, want it to mention GOQ_PASSWORD", err)
	}
}
