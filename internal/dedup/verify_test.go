package dedup

import (
	"testing"
	"time"

	"packetreplay/internal/model"
	"packetreplay/internal/store"
)

func TestDedupKeyClearedAfterStore(t *testing.T) {
	d := New(time.Minute)
	st := store.NewStore(8)
	st.SetKeyReleaser(d)
	key, err := model.ParseStreamKey("10.0.0.1:1000-10.0.0.2:80/tcp")
	if err != nil {
		t.Fatalf("stream key: %v", err)
	}
	pkt := model.NewDatagram(key, 1, time.Now(), []byte("same-payload"))
	digest := d.Key(pkt)
	if !d.Record(digest) {
		t.Fatal("first record should be accepted")
	}
	if err := st.Put(pkt); err != nil {
		t.Fatalf("store put: %v", err)
	}
	if d.Seen(digest) {
		t.Fatal("dedup key must be cleared after the packet is durably stored")
	}
}
