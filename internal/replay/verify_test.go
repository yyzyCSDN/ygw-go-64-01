package replay

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"packetreplay/internal/index"
	"packetreplay/internal/model"
	"packetreplay/internal/store"
)

func TestReplayCursorUsesLatestIndex(t *testing.T) {
	st := store.NewStore(16)
	idx := index.New()
	idx.SetLookup(func(id string) (model.Packet, bool) { return st.Get(id) })
	cursorPath := filepath.Join(t.TempDir(), "cursor.json")
	key, err := model.ParseStreamKey("10.0.0.1:1000-10.0.0.2:80/tcp")
	if err != nil {
		t.Fatalf("stream key: %v", err)
	}
	base := time.Now()
	for i := 0; i < 6; i++ {
		if err := st.Put(model.NewDatagram(key, uint64(i), base.Add(time.Duration(i)*time.Millisecond), []byte(fmt.Sprintf("p%d", i)))); err != nil {
			t.Fatalf("seed store: %v", err)
		}
	}
	if _, err := idx.Rebuild(st); err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	first := NewReplayService(st, idx, NewScheduler(3), NewFileCursorStore(cursorPath), func(model.Packet) error { return nil })
	if emitted, err := first.RunTask(context.Background()); err != nil || emitted != 6 {
		t.Fatalf("first run: emitted=%d err=%v", emitted, err)
	}
	_ = st.Put(model.NewDatagram(key, 6, base.Add(6*time.Millisecond), []byte("p6")))
	_ = st.Put(model.NewDatagram(key, 7, base.Add(7*time.Millisecond), []byte("p7")))
	if _, err := idx.Rebuild(st); err != nil {
		t.Fatalf("rebuild after append: %v", err)
	}
	var outputs []model.Packet
	second := NewReplayService(st, idx, NewScheduler(3), NewFileCursorStore(cursorPath), func(pkt model.Packet) error {
		outputs = append(outputs, pkt)
		return nil
	})
	emitted, err := second.Resume(context.Background())
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	if emitted != 2 {
		t.Fatalf("resume must replay only the new packets, got %d emitted (%v)", emitted, packetSeqs(outputs))
	}
	if len(outputs) != 2 {
		t.Fatalf("resume outputs = %d", len(outputs))
	}
}

func packetSeqs(packets []model.Packet) []uint64 {
	out := make([]uint64, 0, len(packets))
	for _, pkt := range packets {
		out = append(out, pkt.Seq)
	}
	return out
}
