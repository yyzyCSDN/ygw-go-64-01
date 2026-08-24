package config

import (
	"fmt"
	"os"
	"path/filepath"
)

// EnsureDataDir 创建数据目录并返回其绝对路径。
func EnsureDataDir(cfg Config) (string, error) {
	abs, err := filepath.Abs(cfg.DataDir)
	if err != nil {
		return "", fmt.Errorf("resolve data dir: %w", err)
	}
	if err := os.MkdirAll(abs, 0o755); err != nil {
		return "", fmt.Errorf("create data dir: %w", err)
	}
	return abs, nil
}

// CursorPath 返回回放游标在数据目录下的落盘路径。
func CursorPath(dataDir string) string {
	return filepath.Join(dataDir, "replay-cursor.json")
}

// SamplePcapPath 返回演示抓包文件路径。
func SamplePcapPath(dataDir string) string {
	return filepath.Join(dataDir, "sample.pcap.txt")
}
