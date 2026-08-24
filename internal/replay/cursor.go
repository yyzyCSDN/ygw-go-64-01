package replay

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// Cursor 记录回放任务的恢复点：最后一条已回放报文 ID 与当时的索引版本。
type Cursor struct {
	LastPacketID string `json:"last_packet_id"`
	IndexVersion uint64 `json:"index_version"`
}

// CursorStore 抽象游标的保存与加载。
type CursorStore interface {
	Save(Cursor) error
	Load() (Cursor, error)
}

// FileCursorStore 把游标持久化到 JSON 文件，重启后恢复。
type FileCursorStore struct {
	path string
}

// NewFileCursorStore 构造文件游标存储。
func NewFileCursorStore(path string) *FileCursorStore {
	return &FileCursorStore{path: path}
}

// Save 原子写入游标文件。
func (f *FileCursorStore) Save(cursor Cursor) error {
	if err := os.MkdirAll(filepath.Dir(f.path), 0o755); err != nil {
		return fmt.Errorf("create cursor dir: %w", err)
	}
	raw, err := json.Marshal(cursor)
	if err != nil {
		return fmt.Errorf("encode cursor: %w", err)
	}
	temporary := f.path + ".tmp"
	if err := os.WriteFile(temporary, raw, 0o644); err != nil {
		return fmt.Errorf("write cursor: %w", err)
	}
	if err := os.Rename(temporary, f.path); err != nil {
		return fmt.Errorf("commit cursor: %w", err)
	}
	return nil
}

// Load 读取游标；文件不存在时返回空游标而不是错误。
func (f *FileCursorStore) Load() (Cursor, error) {
	raw, err := os.ReadFile(f.path)
	if err != nil {
		if os.IsNotExist(err) {
			return Cursor{}, nil
		}
		return Cursor{}, fmt.Errorf("read cursor: %w", err)
	}
	var cursor Cursor
	if err := json.Unmarshal(raw, &cursor); err != nil {
		return Cursor{}, fmt.Errorf("decode cursor: %w", err)
	}
	return cursor, nil
}
