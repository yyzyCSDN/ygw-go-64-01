package config

import (
	"testing"
	"time"
)

func TestDefaultConfigValid(t *testing.T) {
	cfg := Default()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("default config should validate: %v", err)
	}
	if cfg.ChunkSize <= 0 || cfg.ReplayChunkSize <= 0 || cfg.DedupWindow <= 0 {
		t.Fatalf("default config has invalid bounds: %+v", cfg)
	}
}

func TestLoadMissingFileFallsBack(t *testing.T) {
	cfg, err := Load("definitely-missing-config.json")
	if err != nil {
		t.Fatalf("missing config should fall back: %v", err)
	}
	if cfg.Addr != Default().Addr {
		t.Fatalf("expected default addr, got %s", cfg.Addr)
	}
}

func TestWithEnvOverrides(t *testing.T) {
	cfg := Default()
	cfg.WithEnv(func(name string) string {
		if name == "PACKETREPLAY_ADDR" {
			return "0.0.0.0:9999"
		}
		if name == "PACKETREPLAY_CHUNK_SIZE" {
			return "64"
		}
		return ""
	})
	if cfg.Addr != "0.0.0.0:9999" || cfg.ChunkSize != 64 {
		t.Fatalf("env override failed: %+v", cfg)
	}
}

func TestDedupWindowPositive(t *testing.T) {
	cfg := Default()
	cfg.DedupWindow = 0
	if err := cfg.Validate(); err == nil {
		t.Fatal("zero dedup window must fail validation")
	}
	_ = time.Second
}
