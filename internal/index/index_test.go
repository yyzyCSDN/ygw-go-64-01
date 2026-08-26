package index

import (
	"testing"
	"time"

	"packetreplay/internal/model"
)

func TestAddSearchBasic(t *testing.T) {
	idx := New()
	key, _ := model.ParseStreamKey("10.0.0.1:1000-10.0.0.2:80/tcp")
	now := time.Now()
	pkt := model.NewDatagram(key, 1, now, []byte("hello"))
	idx.SetLookup(func(id string) (model.Packet, bool) { return pkt, id == pkt.ID })
	idx.Add(pkt)
	if idx.Size() != 1 {
		t.Fatalf("expected size 1, got %d", idx.Size())
	}
	ids := idx.Search(Query{Proto: "tcp"})
	if len(ids) != 1 || ids[0] != pkt.ID {
		t.Fatalf("proto search mismatch: %v", ids)
	}
	ids = idx.Search(Query{Port: 80})
	if len(ids) != 1 {
		t.Fatalf("port search mismatch: %v", ids)
	}
}

func TestSearchIntersection(t *testing.T) {
	idx := New()
	key, _ := model.ParseStreamKey("10.0.0.1:1000-10.0.0.2:80/tcp")
	pkt := model.NewDatagram(key, 1, time.Now(), []byte("x"))
	idx.SetLookup(func(id string) (model.Packet, bool) { return pkt, id == pkt.ID })
	idx.Add(pkt)
	ids := idx.Search(Query{Proto: "tcp", Port: 9999})
	if len(ids) != 0 {
		t.Fatalf("intersection should be empty: %v", ids)
	}
}

func TestSearchText(t *testing.T) {
	idx := New()
	key, _ := model.ParseStreamKey("10.0.0.1:1000-10.0.0.2:80/tcp")
	pkt := model.NewDatagram(key, 1, time.Now(), []byte("x"))
	idx.SetLookup(func(id string) (model.Packet, bool) { return pkt, id == pkt.ID })
	idx.Add(pkt)
	ids := idx.Search(Query{Text: "10.0.0.1:1000"})
	if len(ids) != 1 {
		t.Fatalf("text search mismatch: %v", ids)
	}
}
