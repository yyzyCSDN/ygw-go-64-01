package reassemble

import (
	"testing"
	"time"

	"packetreplay/internal/model"
)

type recordingStore struct {
	packets []model.Packet
}

func (r *recordingStore) Put(pkt model.Packet) error {
	r.packets = append(r.packets, pkt)
	return nil
}

func TestReassemblyWindowKeepsBoundaryFragments(t *testing.T) {
	assembler := NewAssembler(4)
	store := &recordingStore{}
	service := NewService(assembler, store)
	key, err := model.ParseStreamKey("10.0.0.1:1000-10.0.0.2:80/tcp")
	if err != nil {
		t.Fatalf("stream key: %v", err)
	}
	ts := time.Now()
	fragments := []model.Packet{
		model.NewFragment(key, 10, 0, 3, ts, []byte("AAA")),
		model.NewFragment(key, 10, 1, 3, ts, []byte("BBB")),
		model.NewFragment(key, 10, 2, 3, ts, []byte("CCC")),
	}
	for _, fragment := range fragments {
		if err := service.Handle(fragment); err != nil {
			t.Fatalf("handle fragment offset %d: %v", fragment.FragOffset, err)
		}
	}
	if len(store.packets) == 0 {
		t.Fatal("reassembled packet was not produced; window boundary fragment was lost")
	}
	assembled := store.packets[len(store.packets)-1]
	if string(assembled.Payload) != "AAABBBCCC" {
		t.Fatalf("reassembled payload mismatch: %q", assembled.Payload)
	}
}
