package reassemble

import (
	"testing"
	"time"

	"packetreplay/internal/model"
)

func TestWindowRange(t *testing.T) {
	key, _ := model.ParseStreamKey("10.0.0.1:1000-10.0.0.2:80/tcp")
	window := NewWindow(key, 10, 3)
	if window.End != 2 {
		t.Fatalf("expected window end 2, got %d", window.End)
	}
	if !window.Within(0) {
		t.Fatal("start offset must be inside the window")
	}
	if window.Within(3) {
		t.Fatal("offset beyond window end must be outside")
	}
	if window.Complete() {
		t.Fatal("empty window must not be complete")
	}
}

func TestDatagramPassThrough(t *testing.T) {
	assembler := NewAssembler(4)
	key, _ := model.ParseStreamKey("10.0.0.1:1000-10.0.0.2:80/tcp")
	pkt := model.NewDatagram(key, 1, time.Now(), []byte("data"))
	got, ok, err := assembler.Feed(pkt)
	if err != nil || !ok {
		t.Fatalf("datagram should pass through: ok=%v err=%v", ok, err)
	}
	if got.ID != pkt.ID {
		t.Fatalf("passthrough mismatch: %s", got.ID)
	}
}

func TestAssemblerStatsCounters(t *testing.T) {
	assembler := NewAssembler(2)
	key, _ := model.ParseStreamKey("10.0.0.1:1000-10.0.0.2:80/tcp")
	pkt := model.NewDatagram(key, 1, time.Now(), []byte("data"))
	_, _, _ = assembler.Feed(pkt)
	stats := assembler.Stats()
	if stats.PassThrough != 1 {
		t.Fatalf("expected 1 passthrough, got %+v", stats)
	}
}

func TestServiceFlushEmpty(t *testing.T) {
	assembler := NewAssembler(4)
	store := &memoryStore{}
	service := NewService(assembler, store)
	if err := service.Flush(); err != nil {
		t.Fatalf("flush empty assembler: %v", err)
	}
}

type memoryStore struct {
	packets []model.Packet
}

func (m *memoryStore) Put(pkt model.Packet) error {
	m.packets = append(m.packets, pkt)
	return nil
}
