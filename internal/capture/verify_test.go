package capture

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

type countingOpener struct {
	mu         sync.Mutex
	openCount  int
	closeCount int
}

func (c *countingOpener) Open(name string) (io.ReadCloser, error) {
	file, err := os.Open(name)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	c.openCount++
	c.mu.Unlock()
	return &trackedReadCloser{file: file, owner: c}, nil
}

type trackedReadCloser struct {
	file  *os.File
	owner *countingOpener
}

func (t *trackedReadCloser) Read(p []byte) (int, error) {
	return t.file.Read(p)
}

func (t *trackedReadCloser) Close() error {
	t.owner.mu.Lock()
	t.owner.closeCount++
	t.owner.mu.Unlock()
	return t.file.Close()
}

func TestPcapHandleClosed(t *testing.T) {
	dir := t.TempDir()
	files := make([]string, 3)
	for i := 0; i < 3; i++ {
		path := filepath.Join(dir, fmt.Sprintf("pcap-%d.txt", i))
		line := fmt.Sprintf("ts=2026-08-24T10:00:0%d.000Z key=10.0.0.1:1000-10.0.0.2:80/tcp seq=%d frag=0 off=0 total=0 hex=aa\n", i, i)
		if err := os.WriteFile(path, []byte(line), 0o644); err != nil {
			t.Fatalf("write pcap file: %v", err)
		}
		files[i] = path
	}
	opener := &countingOpener{}
	source := NewFileSourceWithOpener(files, opener)
	if err := source.Open(); err != nil {
		t.Fatalf("open source: %v", err)
	}
	ctx := context.Background()
	for {
		_, err := source.Next(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("read pcap: %v", err)
		}
	}
	_ = source.Close()
	if opener.openCount != 3 || opener.closeCount != 3 {
		t.Fatalf("pcap file handles leaked: opened=%d closed=%d", opener.openCount, opener.closeCount)
	}
}
