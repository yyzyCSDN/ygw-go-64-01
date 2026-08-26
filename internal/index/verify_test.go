package index

import (
	"testing"
	"time"

	"packetreplay/internal/model"
	"packetreplay/internal/store"
)

// midRebuildStore 在重建扫描到第一条报文时向存储注入一条新报文，
// 模拟重建期间采集仍在写入的真实场景。
type midRebuildStore struct {
	*store.Store
	key      model.StreamKey
	injected bool
}

func (m *midRebuildStore) Get(id string) (model.Packet, bool) {
	if !m.injected {
		m.injected = true
		_ = m.Store.Put(model.NewDatagram(m.key, 99, time.Now(), []byte("mid-rebuild")))
	}
	return m.Store.Get(id)
}

func TestIndexRebuildIncludesNewPackets(t *testing.T) {
	st := store.NewStore(8)
	key, err := model.ParseStreamKey("10.0.0.1:1000-10.0.0.2:80/tcp")
	if err != nil {
		t.Fatalf("stream key: %v", err)
	}
	if err := st.Put(model.NewDatagram(key, 1, time.Now(), []byte("first"))); err != nil {
		t.Fatalf("seed store: %v", err)
	}
	idx := New()
	hijacked := &midRebuildStore{Store: st, key: key}
	idx.SetLookup(hijacked.Get)
	result, err := idx.Rebuild(hijacked)
	if err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	matches := idx.Search(Query{Proto: "tcp"})
	if len(matches) != 2 {
		t.Fatalf("rebuild missed the packet written during rebuild: result=%+v matches=%v", result, matches)
	}
}
