package replay

import (
	"context"
	"testing"
	"time"

	"packetreplay/internal/index"
	"packetreplay/internal/model"
	"packetreplay/internal/store"
)

func TestSchedulerExactMultiple(t *testing.T) {
	scheduler := NewScheduler(3)
	chunks := scheduler.Chunks(6)
	if len(chunks) != 2 {
		t.Fatalf("expected 2 chunks, got %d", len(chunks))
	}
	if chunks[1][1] != 6 {
		t.Fatalf("second chunk end must be 6: %v", chunks[1])
	}
	if scheduler.ChunkCount(6) != 2 {
		t.Fatalf("unexpected chunk count: %d", scheduler.ChunkCount(6))
	}
}

func TestReplaySortedBatch(t *testing.T) {
	key, _ := model.ParseStreamKey("10.0.0.1:1000-10.0.0.2:80/tcp")
	now := time.Now()
	st := store.NewStore(8)
	idx := index.New()
	idx.SetLookup(func(id string) (model.Packet, bool) { return st.Get(id) })
	var outputs []model.Packet
	service := NewReplayService(st, idx, NewScheduler(2), NewFileCursorStore(t.TempDir()+"/cursor.json"), func(pkt model.Packet) error {
		outputs = append(outputs, pkt)
		return nil
	})
	packets := []model.Packet{
		model.NewDatagram(key, 1, now, []byte("a")),
		model.NewDatagram(key, 2, now.Add(time.Millisecond), []byte("b")),
		model.NewDatagram(key, 3, now.Add(2*time.Millisecond), []byte("c")),
		model.NewDatagram(key, 4, now.Add(3*time.Millisecond), []byte("d")),
	}
	emitted, err := service.Replay(context.Background(), packets)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if emitted != 4 {
		t.Fatalf("expected 4 emitted, got %d", emitted)
	}
	for i := 1; i < len(outputs); i++ {
		if outputs[i].Timestamp.Before(outputs[i-1].Timestamp) {
			t.Fatal("output must be timestamp ascending")
		}
	}
}

func TestMemoryCursorStore(t *testing.T) {
	store := NewFileCursorStore(t.TempDir() + "/cursor.json")
	if err := store.Save(Cursor{LastPacketID: "p1", IndexVersion: 3}); err != nil {
		t.Fatalf("save: %v", err)
	}
	cursor, err := store.Load()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cursor.LastPacketID != "p1" || cursor.IndexVersion != 3 {
		t.Fatalf("unexpected cursor: %+v", cursor)
	}
}
