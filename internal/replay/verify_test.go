package replay

import (
	"context"
	"testing"

	"packetreplay/internal/index"
	"packetreplay/internal/model"
	"packetreplay/internal/store"
)

func TestEmptyStreamNoNilPanic(t *testing.T) {
	st := store.NewStore(8)
	idx := index.New()
	idx.SetLookup(func(id string) (model.Packet, bool) { return st.Get(id) })
	service := NewReplayService(
		st,
		idx,
		NewScheduler(4),
		NewFileCursorStore(t.TempDir()+"/cursor.json"),
		func(model.Packet) error { return nil },
	)
	key, err := model.ParseStreamKey("10.0.0.1:1000-10.0.0.2:80/tcp")
	if err != nil {
		t.Fatalf("stream key: %v", err)
	}
	emitted, err := service.ReplayStream(context.Background(), key)
	if err != nil {
		t.Fatalf("empty stream replay returned error: %v", err)
	}
	if emitted != 0 {
		t.Fatalf("empty stream must emit 0 packets, got %d", emitted)
	}
}
