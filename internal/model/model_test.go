package model

import (
	"testing"
	"time"
)

func TestStreamKeyParseRoundTrip(t *testing.T) {
	key, err := ParseStreamKey("10.0.0.1:1000-10.0.0.2:80/tcp")
	if err != nil {
		t.Fatalf("parse stream key: %v", err)
	}
	if key.SrcIP != "10.0.0.1" || key.DstPort != 80 || key.Proto != ProtoTCP {
		t.Fatalf("unexpected key: %+v", key)
	}
	if key.String() != "10.0.0.1:1000-10.0.0.2:80/6" {
		t.Fatalf("unexpected canonical form: %s", key.String())
	}
}

func TestPacketValidate(t *testing.T) {
	key, _ := ParseStreamKey("10.0.0.1:1000-10.0.0.2:80/tcp")
	pkt := NewDatagram(key, 1, time.Now(), []byte("hello"))
	if err := pkt.Validate(); err != nil {
		t.Fatalf("datagram should validate: %v", err)
	}
	frag := NewFragment(key, 2, 0, 2, time.Now(), []byte("a"))
	if err := frag.Validate(); err != nil {
		t.Fatalf("fragment should validate: %v", err)
	}
	frag.FragTotal = 0
	if err := frag.Validate(); err == nil {
		t.Fatal("fragment with zero total should fail validation")
	}
}

func TestParseHexPayload(t *testing.T) {
	payload, err := ParseHexPayload("aabbccdd")
	if err != nil {
		t.Fatalf("parse hex: %v", err)
	}
	if len(payload) != 4 || payload[0] != 0xaa || payload[3] != 0xdd {
		t.Fatalf("unexpected payload: %v", payload)
	}
	if _, err := ParseHexPayload("abc"); err == nil {
		t.Fatal("odd-length hex should fail")
	}
}

func TestPacketKeyDeterministic(t *testing.T) {
	key, _ := ParseStreamKey("10.0.0.1:1000-10.0.0.2:80/tcp")
	a := NewDatagram(key, 5, time.Now(), []byte("same payload"))
	b := NewDatagram(key, 6, time.Now(), []byte("same payload"))
	if PacketKey(a) != PacketKey(b) {
		t.Fatal("identical payloads on one stream must share a key")
	}
	if PacketKey(a) == PacketKey(NewDatagram(key, 5, time.Now(), []byte("other"))) {
		t.Fatal("different payload must produce different key")
	}
}

func TestNormalizeIP(t *testing.T) {
	if NormalizeIP("10.0.0.1") != "10.0.0.1" {
		t.Fatalf("ip normalization failed: %s", NormalizeIP("10.0.0.1"))
	}
	if NormalizeIP("  ::1  ") != "::1" {
		t.Fatalf("ipv6 normalization failed: %s", NormalizeIP("  ::1  "))
	}
}
