package filter

import (
	"testing"
	"time"

	"packetreplay/internal/model"
)

func TestParseRuleEquality(t *testing.T) {
	rule, err := ParseRule("proto eq tcp")
	if err != nil {
		t.Fatalf("parse proto rule: %v", err)
	}
	if rule.Field != FieldProto || rule.Op != OpEqual || rule.Value != "tcp" {
		t.Fatalf("unexpected rule: %+v", rule)
	}
}

func TestParseRuleRangeValidation(t *testing.T) {
	if _, err := ParseRule("dstport in 9000 8000"); err == nil {
		t.Fatal("reversed range must be rejected")
	}
	if _, err := ParseRule("srcport eq 80"); err == nil {
		t.Fatal("port field with equality must be rejected")
	}
}

func TestFilterEqualityMatch(t *testing.T) {
	key, _ := model.ParseStreamKey("10.0.0.1:1000-10.0.0.2:80/tcp")
	f := New()
	rule, _ := ParseRule("proto eq udp")
	f.AddRule(rule)
	pkt := model.NewDatagram(key, 1, time.Now(), []byte("x"))
	if f.Match(pkt) {
		t.Fatal("tcp packet must not match udp rule")
	}
}

func TestFilterRangeMiddleValue(t *testing.T) {
	key, _ := model.ParseStreamKey("10.0.0.1:1000-10.0.0.2:8500/tcp")
	f := New()
	rule, _ := ParseRule("dstport in 8000 9000")
	f.AddRule(rule)
	pkt := model.NewDatagram(key, 1, time.Now(), []byte("x"))
	if !f.Match(pkt) {
		t.Fatal("mid-range port must match")
	}
	out := f.Apply([]model.Packet{pkt})
	if len(out) != 1 {
		t.Fatalf("apply should keep the packet, got %d", len(out))
	}
}

func TestFilterApply(t *testing.T) {
	key, _ := model.ParseStreamKey("10.0.0.1:1000-10.0.0.2:80/tcp")
	f := New()
	rule, _ := ParseRule("srcip eq 10.0.0.1")
	f.AddRule(rule)
	packets := []model.Packet{
		model.NewDatagram(key, 1, time.Now(), []byte("a")),
		model.NewDatagram(model.StreamKey{SrcIP: "10.0.0.9", SrcPort: 1000, DstIP: "10.0.0.2", DstPort: 80, Proto: 6}, 2, time.Now(), []byte("b")),
	}
	out := f.Apply(packets)
	if len(out) != 1 {
		t.Fatalf("expected 1 match, got %d", len(out))
	}
}
