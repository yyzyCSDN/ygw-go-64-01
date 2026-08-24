package capture

import (
	"context"
	"testing"
	"time"

	"packetreplay/internal/model"
)

func TestParsePacketLineDatagram(t *testing.T) {
	line := "ts=2026-08-24T10:00:00.100Z key=10.0.0.1:1000-10.0.0.2:80/tcp seq=3 frag=0 off=0 total=0 hex=aabbcc"
	pkt, err := parsePacketLine(line)
	if err != nil {
		t.Fatalf("parse line: %v", err)
	}
	if pkt.IsFragment || pkt.Seq != 3 || len(pkt.Payload) != 3 {
		t.Fatalf("unexpected packet: %+v", pkt)
	}
}

func TestParsePacketLineFragment(t *testing.T) {
	line := "ts=2026-08-24T10:00:00.100Z key=10.0.0.1:1000-10.0.0.2:80/tcp seq=9 frag=1 off=1 total=2 hex=dead"
	pkt, err := parsePacketLine(line)
	if err != nil {
		t.Fatalf("parse fragment line: %v", err)
	}
	if !pkt.IsFragment || pkt.FragOffset != 1 || pkt.FragTotal != 2 {
		t.Fatalf("unexpected fragment: %+v", pkt)
	}
}

func TestParsePacketLineMalformed(t *testing.T) {
	if _, err := parsePacketLine("key=10.0.0.1:1000-10.0.0.2:80/tcp seq=x"); err == nil {
		t.Fatal("malformed seq must fail")
	}
}

func TestLiveSourceBoundary(t *testing.T) {
	key := testStreamKey()
	source := NewLiveSource(key, mustTime("2026-08-24T10:00:00Z"), 0, 2, []byte("x"))
	if err := source.Open(); err != nil {
		t.Fatalf("open: %v", err)
	}
	ctx := context.Background()
	first, err := source.Next(ctx)
	if err != nil {
		t.Fatalf("first packet: %v", err)
	}
	_, err = source.Next(ctx)
	if err != nil {
		t.Fatalf("second packet: %v", err)
	}
	_ = first
}

func testStreamKey() model.StreamKey {
	key, err := model.ParseStreamKey("10.0.0.1:1000-10.0.0.2:80/tcp")
	if err != nil {
		panic(err)
	}
	return key
}

func mustTime(text string) time.Time {
	value, err := time.Parse(time.RFC3339, text)
	if err != nil {
		panic(err)
	}
	return value
}
