package stamp

import (
	"testing"
	"time"

	"packetreplay/internal/model"
)

func TestParseTimestampFormats(t *testing.T) {
	for _, text := range []string{
		"2026-08-24T10:00:00.123Z",
		"2026-08-24T10:00:00Z",
		"2026-08-24 10:00:00.000",
	} {
		if _, err := ParseTimestamp(text); err != nil {
			t.Fatalf("parse %q: %v", text, err)
		}
	}
	if _, err := ParseTimestamp("not-a-time"); err == nil {
		t.Fatal("invalid timestamp should fail")
	}
}

func TestCompareTieBreak(t *testing.T) {
	key, _ := model.ParseStreamKey("10.0.0.1:1000-10.0.0.2:80/tcp")
	now := time.Now()
	early := model.NewDatagram(key, 1, now, []byte("a"))
	late := model.NewDatagram(key, 2, now.Add(time.Second), []byte("b"))
	if Compare(early, late) >= 0 {
		t.Fatal("early packet must sort before late packet")
	}
	sameA := model.NewDatagram(key, 5, now, []byte("c"))
	sameB := model.NewDatagram(key, 6, now, []byte("d"))
	if Compare(sameA, sameB) >= 0 {
		t.Fatal("same-timestamp packets must sort by seq")
	}
}

func TestTagBatchAssignsKeys(t *testing.T) {
	key, _ := model.ParseStreamKey("10.0.0.1:1000-10.0.0.2:80/tcp")
	now := time.Now()
	packets := []model.Packet{
		model.NewDatagram(key, 1, now, []byte("a")),
		model.NewDatagram(key, 2, now.Add(time.Minute), []byte("b")),
	}
	tagged := TagBatch(packets)
	if len(tagged) != 2 || tagged[1].Key <= tagged[0].Key {
		t.Fatalf("tag keys not ascending: %+v", tagged)
	}
}

func TestMeasureWindow(t *testing.T) {
	key, _ := model.ParseStreamKey("10.0.0.1:1000-10.0.0.2:80/tcp")
	now := time.Now()
	packets := []model.Packet{
		model.NewDatagram(key, 1, now, []byte("a")),
		model.NewDatagram(key, 2, now.Add(2*time.Second), []byte("b")),
	}
	window := MeasureWindow(packets)
	if window.Count != 2 || window.Span() != 2*time.Second {
		t.Fatalf("unexpected window: %+v", window)
	}
}
