package dedup

import (
	"testing"
	"time"

	"packetreplay/internal/model"
)

func TestRecordSeenRelease(t *testing.T) {
	d := New(time.Minute)
	key, _ := model.ParseStreamKey("10.0.0.1:1000-10.0.0.2:80/tcp")
	pkt := model.NewDatagram(key, 1, time.Now(), []byte("payload"))
	digest := d.Key(pkt)
	if !d.Record(digest) {
		t.Fatal("first record should return true")
	}
	if !d.Seen(digest) {
		t.Fatal("recorded key must be seen")
	}
	d.Release(digest)
	if d.Seen(digest) {
		t.Fatal("released key must not be seen")
	}
}

func TestPurgeExpired(t *testing.T) {
	d := New(time.Minute)
	key, _ := model.ParseStreamKey("10.0.0.1:1000-10.0.0.2:80/tcp")
	pkt := model.NewDatagram(key, 1, time.Now(), []byte("payload"))
	digest := d.Key(pkt)
	d.Record(digest)
	removed := d.PurgeExpired(time.Now().Add(2 * time.Minute))
	if removed != 1 {
		t.Fatalf("expected 1 removed, got %d", removed)
	}
	if d.Len() != 0 {
		t.Fatalf("window should be empty, got %d", d.Len())
	}
}

func TestExpiredCount(t *testing.T) {
	d := New(time.Minute)
	key, _ := model.ParseStreamKey("10.0.0.1:1000-10.0.0.2:80/tcp")
	pkt := model.NewDatagram(key, 1, time.Now(), []byte("payload"))
	digest := d.Key(pkt)
	d.Record(digest)
	if d.ExpiredCount(time.Now().Add(time.Minute)) != 1 {
		t.Fatal("expired count should be 1 after window")
	}
}
