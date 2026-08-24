package capture

import (
	"context"
	"errors"
	"testing"
	"time"

	"packetreplay/internal/dedup"
	"packetreplay/internal/model"
)

type failingStore struct {
	calls int
}

func (f *failingStore) Put(model.Packet) error {
	f.calls++
	return errors.New("disk full")
}

func TestCaptureWriteErrorNotSwallowed(t *testing.T) {
	key, err := model.ParseStreamKey("10.0.0.1:1000-10.0.0.2:80/tcp")
	if err != nil {
		t.Fatalf("stream key: %v", err)
	}
	source := NewLiveSource(key, time.Now(), 0, 1, []byte("payload"))
	store := &failingStore{}
	service := NewService(source, store, dedup.New(time.Minute))
	_, ok, err := service.ProcessOne(context.Background())
	if err == nil {
		t.Fatal("capture swallowed the store write failure and reported success")
	}
	if ok {
		t.Fatal("capture must not report success for a failed write")
	}
}
