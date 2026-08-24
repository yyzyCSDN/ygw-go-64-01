package replay

import (
	"context"
	"testing"
	"time"

	"packetreplay/internal/index"
	"packetreplay/internal/model"
	"packetreplay/internal/store"
)

// TestReplayStreamEmpty 验证回放一个不存在的流不会崩溃，
// 而是输出零报文、零错误、空输出列表。
// 回归：此前 ReplayStream 直接取 packets[0].ID 构造任务名，
// 空流会越界 panic，把进程拖崩。
func TestReplayStreamEmpty(t *testing.T) {
	key, _ := model.ParseStreamKey("10.0.0.1:1000-10.0.0.2:80/tcp")
	st := store.NewStore(8)
	idx := index.New()
	idx.SetLookup(func(id string) (model.Packet, bool) { return st.Get(id) })

	var outputs []model.Packet
	service := NewReplayService(
		st, idx, NewScheduler(2),
		NewFileCursorStore(t.TempDir()+"/cursor.json"),
		func(pkt model.Packet) error {
			outputs = append(outputs, pkt)
			return nil
		},
	)

	// 不向 store 写入任何属于该流的报文，模拟空流。
	emitted, err := service.ReplayStream(context.Background(), key)
	if err != nil {
		t.Fatalf("replay empty stream: %v", err)
	}
	if emitted != 0 {
		t.Fatalf("expected 0 emitted for empty stream, got %d", emitted)
	}
	if got := service.Outputs(); len(got) != 0 {
		t.Fatalf("expected empty outputs for empty stream, got %d packets", len(got))
	}
	if len(outputs) != 0 {
		t.Fatalf("output callback must not fire for empty stream, got %d", len(outputs))
	}
}

// TestReplayStreamNonEmpty 对照组：非空流仍正常回放，行为不变。
func TestReplayStreamNonEmpty(t *testing.T) {
	key, _ := model.ParseStreamKey("10.0.0.1:1000-10.0.0.2:80/tcp")
	now := time.Now()
	st := store.NewStore(8)
	idx := index.New()
	idx.SetLookup(func(id string) (model.Packet, bool) { return st.Get(id) })

	if err := st.Put(model.NewDatagram(key, 1, now, []byte("a"))); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := st.Put(model.NewDatagram(key, 2, now.Add(time.Millisecond), []byte("b"))); err != nil {
		t.Fatalf("put: %v", err)
	}

	service := NewReplayService(
		st, idx, NewScheduler(2),
		NewFileCursorStore(t.TempDir()+"/cursor.json"),
		func(pkt model.Packet) error { return nil },
	)

	emitted, err := service.ReplayStream(context.Background(), key)
	if err != nil {
		t.Fatalf("replay stream: %v", err)
	}
	if emitted != 2 {
		t.Fatalf("expected 2 emitted, got %d", emitted)
	}
}
