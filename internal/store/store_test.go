package store

import (
	"testing"
	"time"

	"packetreplay/internal/model"
)

func testKey() model.StreamKey {
	key, _ := model.ParseStreamKey("10.0.0.1:1000-10.0.0.2:80/tcp")
	return key
}

func TestPutGetRoundTrip(t *testing.T) {
	st := NewStore(4)
	key := testKey()
	pkt := model.NewDatagram(key, 1, time.Now(), []byte("payload-1"))
	if err := st.Put(pkt); err != nil {
		t.Fatalf("put: %v", err)
	}
	got, ok := st.Get(pkt.ID)
	if !ok || got.Seq != pkt.Seq {
		t.Fatalf("get mismatch: %+v ok=%v", got, ok)
	}
	stats := st.Stats()
	if stats.Packets != 1 || stats.Streams != 1 {
		t.Fatalf("unexpected stats: %+v", stats)
	}
}

func TestStoreSegmentsFullChunks(t *testing.T) {
	st := NewStore(3)
	key := testKey()
	base := time.Now()
	for i := 0; i < 6; i++ {
		pkt := model.NewDatagram(key, uint64(i), base.Add(time.Duration(i)*time.Millisecond), []byte("x"))
		if err := st.Put(pkt); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}
	segments := st.Segments()
	if len(segments) != 2 {
		t.Fatalf("expected 2 segments, got %d", len(segments))
	}
	if segments[1].Packets != 3 || segments[1].End != 6 {
		t.Fatalf("unexpected second segment: %+v", segments[1])
	}
}

func TestPutRejectsEmptyPayload(t *testing.T) {
	st := NewStore(4)
	key := testKey()
	pkt := model.NewDatagram(key, 1, time.Now(), nil)
	if err := st.Put(pkt); err == nil {
		t.Fatal("empty payload must be rejected")
	}
}

func TestSnapshotAndSince(t *testing.T) {
	st := NewStore(4)
	key := testKey()
	base := time.Now()
	for i := 0; i < 3; i++ {
		_ = st.Put(model.NewDatagram(key, uint64(i), base, []byte("p")))
	}
	snap := st.Snapshot()
	if snap.Version != 3 || len(snap.PacketIDs) != 3 {
		t.Fatalf("unexpected snapshot: %+v", snap)
	}
	_ = st.Put(model.NewDatagram(key, 3, base, []byte("q")))
	delta := st.Since(snap.Version)
	if len(delta) != 1 {
		t.Fatalf("expected 1 delta packet, got %d", len(delta))
	}
}
