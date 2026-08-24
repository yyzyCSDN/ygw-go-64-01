package config

import (
	"encoding/json"
	"fmt"
	"os"
	"time"
)

// Config 汇总服务运行所需的全部参数，支持文件加载与环境变量覆盖。
type Config struct {
	Addr            string        `json:"addr"`
	DataDir         string        `json:"data_dir"`
	ChunkSize       int           `json:"chunk_size"`
	ReplayChunkSize int           `json:"replay_chunk_size"`
	DedupWindow     time.Duration `json:"dedup_window"`
	CaptureInterval time.Duration `json:"capture_interval"`
	ReassemblyLimit int           `json:"reassembly_limit"`
	PcapFiles       []string      `json:"pcap_files"`
}

// Default 返回内置默认配置，未指定配置文件的进程直接使用。
func Default() Config {
	return Config{
		Addr:            "127.0.0.1:8920",
		DataDir:         "data",
		ChunkSize:       512,
		ReplayChunkSize: 128,
		DedupWindow:     60 * time.Second,
		CaptureInterval: 200 * time.Millisecond,
		ReassemblyLimit: 64,
	}
}

// Load 从 JSON 文件读取配置；文件不存在时回落到默认值。
func Load(path string) (Config, error) {
	cfg := Default()
	if path == "" {
		return cfg, nil
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return cfg, nil
		}
		return cfg, fmt.Errorf("read config: %w", err)
	}
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return cfg, fmt.Errorf("parse config: %w", err)
	}
	if err := cfg.Validate(); err != nil {
		return cfg, err
	}
	return cfg, nil
}

// Validate 检查配置项的取值边界。
func (c Config) Validate() error {
	if c.ChunkSize <= 0 {
		return fmt.Errorf("chunk_size must be positive")
	}
	if c.ReplayChunkSize <= 0 {
		return fmt.Errorf("replay_chunk_size must be positive")
	}
	if c.DedupWindow <= 0 {
		return fmt.Errorf("dedup_window must be positive")
	}
	if c.CaptureInterval <= 0 {
		return fmt.Errorf("capture_interval must be positive")
	}
	if c.ReassemblyLimit <= 0 {
		return fmt.Errorf("reassembly_limit must be positive")
	}
	return nil
}

// WithEnv 用环境变量覆盖配置项，便于容器与脚本启动。
func (c *Config) WithEnv(getenv func(string) string) {
	if value := getenv("PACKETREPLAY_ADDR"); value != "" {
		c.Addr = value
	}
	if value := getenv("PACKETREPLAY_CHUNK_SIZE"); value != "" {
		if parsed, err := parseInt(value); err == nil {
			c.ChunkSize = parsed
		}
	}
	if value := getenv("PACKETREPLAY_REPLAY_CHUNK"); value != "" {
		if parsed, err := parseInt(value); err == nil {
			c.ReplayChunkSize = parsed
		}
	}
}

func parseInt(text string) (int, error) {
	var value int
	_, err := fmt.Sscanf(text, "%d", &value)
	return value, err
}
