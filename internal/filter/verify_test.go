package filter

import (
	"testing"
	"time"

	"packetreplay/internal/model"
)

func TestFilterKeepsBoundaryPackets(t *testing.T) {
	f := New()
	rule, err := ParseRule("dstport in 8000 8002")
	if err != nil {
		t.Fatalf("parse rule: %v", err)
	}
	f.AddRule(rule)
	lowKey, err := model.ParseStreamKey("10.0.0.1:1000-10.0.0.2:8000/tcp")
	if err != nil {
		t.Fatalf("low key: %v", err)
	}
	highKey, err := model.ParseStreamKey("10.0.0.1:1000-10.0.0.2:8002/tcp")
	if err != nil {
		t.Fatalf("high key: %v", err)
	}
	low := model.NewDatagram(lowKey, 1, time.Now(), []byte("low"))
	high := model.NewDatagram(highKey, 2, time.Now(), []byte("high"))
	out := f.Apply([]model.Packet{low, high})
	if len(out) != 2 {
		t.Fatalf("range endpoints must be kept by an inclusive rule, got %d kept", len(out))
	}
}
