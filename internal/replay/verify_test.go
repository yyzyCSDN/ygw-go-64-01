package replay

import (
	"context"
	"fmt"
	"testing"
	"time"

	"packetreplay/internal/index"
	"packetreplay/internal/model"
	"packetreplay/internal/store"
)

func TestReplayChunkKeepsAllPackets(t *testing.T) {
	st := store.NewStore(16)
	idx := index.New()
	idx.SetLookup(func(id string) (model.Packet, bool) { return st.Get(id) })
	var outputs []model.Packet
	service := NewReplayService(
		st,
		idx,
		NewScheduler(3),
		NewFileCursorStore(t.TempDir()+"/cursor.json"),
		func(pkt model.Packet) error {
			outputs = append(outputs, pkt)
			return nil
		},
	)
	key, err := model.ParseStreamKey("10.0.0.1:1000-10.0.0.2:80/tcp")
	if err != nil {
		t.Fatalf("stream key: %v", err)
	}
	base := time.Now()
	packets := make([]model.Packet, 7)
	for i := 0; i < 7; i++ {
		packets[i] = model.NewDatagram(key, uint64(i), base.Add(time.Duration(i)*time.Millisecond), []byte(fmt.Sprintf("p%d", i)))
	}
	emitted, err := service.Replay(context.Background(), packets)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if emitted != 7 {
		t.Fatalf("expected all 7 packets, got %d emitted", emitted)
	}
	if len(outputs) != 7 {
		t.Fatalf("expected 7 outputs, got %d", len(outputs))
	}
}
