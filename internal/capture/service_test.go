package capture

import (
	"context"
	"errors"
	"testing"
	"time"

	"packetreplay/internal/dedup"
	"packetreplay/internal/model"
)

// memorySource 在内存里按顺序吐出预设报文，读完返回 io.EOF。
type memorySource struct {
	pkts []model.Packet
	idx  int
}

func (m *memorySource) Open() error  { return nil }
func (m *memorySource) Close() error { return nil }
func (m *memorySource) Name() string  { return "memory" }
func (m *memorySource) Next(ctx context.Context) (model.Packet, error) {
	if m.idx >= len(m.pkts) {
		return model.Packet{}, EOF
	}
	pkt := m.pkts[m.idx]
	m.idx++
	return pkt, nil
}

// flakyStore 每次 Put 都返回非 invalid-packet 的瞬时错误，模拟写存储持续失败。
type flakyStore struct {
	puts int
}

func (f *flakyStore) Put(pkt model.Packet) error {
	f.puts++
	return errors.New("disk i/o error")
}

// flakyOnceStore 第一次 Put 失败、第二次成功，验证重试确实会再次写入。
type flakyOnceStore struct {
	puts   int
	stored []model.Packet
}

func (f *flakyOnceStore) Put(pkt model.Packet) error {
	f.puts++
	if f.puts == 1 {
		return errors.New("transient error")
	}
	f.stored = append(f.stored, pkt)
	return nil
}

// makePacket 构造一条可落库的数据报。
func makePacket(seq uint64) model.Packet {
	key, _ := model.ParseStreamKey("10.0.0.1:1000-10.0.0.2:80/tcp")
	return model.NewDatagram(key, seq, time.Now(), []byte("payload"))
}

// TestProcessOneReportsRetryFailure 确保写存储重试仍失败时错误被真实上报，
// 而不是被吞掉伪装成成功——这是“采集显示正常但报文没入库”的根因。
func TestProcessOneReportsRetryFailure(t *testing.T) {
	store := &flakyStore{}
	svc := NewService(&memorySource{pkts: []model.Packet{makePacket(1)}}, store, dedup.New(time.Minute))

	_, stored, err := svc.ProcessOne(context.Background())
	if err == nil {
		t.Fatal("write failure after retry must surface an error, got nil")
	}
	if stored {
		t.Fatal("stored flag must be false when put fails")
	}
	// 初次失败 + 一次重试，确认重试确实发生而非直接放弃。
	if store.puts != 2 {
		t.Fatalf("expected 2 put attempts (initial + retry), got %d", store.puts)
	}
	stats := svc.Stats()
	if stats.Failed != 1 {
		t.Fatalf("expected Failed=1, got %d", stats.Failed)
	}
	if stats.Stored != 0 {
		t.Fatalf("Stored must stay 0 when nothing was persisted, got %d", stats.Stored)
	}
	if stats.LastError == "" {
		t.Fatal("LastError must be populated with the real error")
	}
}

// TestProcessOneRetryRecoversWhenTransient 确保瞬时错误重试成功后报文正常落库，
// 不会因为重试路径把成功的报文误判为失败。
func TestProcessOneRetryRecoversWhenTransient(t *testing.T) {
	store := &flakyOnceStore{}
	svc := NewService(&memorySource{pkts: []model.Packet{makePacket(2)}}, store, dedup.New(time.Minute))

	_, stored, err := svc.ProcessOne(context.Background())
	if err != nil {
		t.Fatalf("transient error retried successfully must not surface, got %v", err)
	}
	if !stored {
		t.Fatal("stored flag must be true after successful retry")
	}
	if store.puts != 2 {
		t.Fatalf("expected 2 put attempts (fail + retry), got %d", store.puts)
	}
	if len(store.stored) != 1 {
		t.Fatalf("expected 1 packet persisted, got %d", len(store.stored))
	}
	stats := svc.Stats()
	if stats.Stored != 1 || stats.Failed != 0 {
		t.Fatalf("expected Stored=1 Failed=0, got Stored=%d Failed=%d", stats.Stored, stats.Failed)
	}
}
