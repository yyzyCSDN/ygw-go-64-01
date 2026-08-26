package store

import (
	"fmt"
	"sort"
	"sync"

	"packetreplay/internal/model"
)

// KeyReleaser 通知外部组件一条报文已经完成持久化，可释放其去重键。
type KeyReleaser interface {
	Release(key uint64)
}

// Stats 汇总存储层的关键计数，供监控页面与状态接口展示。
type Stats struct {
	Packets    int
	Streams    int
	Chunks     int
	Checksums  int
	Generation uint64
}

// Store 是进程内的报文存储：按流索引报文，按块切分记录，并维护代际计数
// 供索引重建等场景计算增量。
type Store struct {
	mu         sync.RWMutex
	packets    []model.Packet
	byID       map[string]model.Packet
	byStream   map[model.StreamKey][]string
	chunks     []int
	chunkSize  int
	generation uint64
	releaser   KeyReleaser
}

// NewStore 构造一个指定块大小的报文存储。
func NewStore(chunkSize int) *Store {
	return &Store{
		byID:      make(map[string]model.Packet),
		byStream:  make(map[model.StreamKey][]string),
		chunks:    make([]int, 0, 1),
		chunkSize: chunkSize,
	}
}

// SetKeyReleaser 挂接去重键释放回调；nil 表示不通知任何外部组件。
func (s *Store) SetKeyReleaser(releaser KeyReleaser) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.releaser = releaser
}

// Put 写入一条报文：校验字段、计算校验和、追加到当前块并推进代际。
func (s *Store) Put(pkt model.Packet) error {
	if err := pkt.Validate(); err != nil {
		return fmt.Errorf("%w: %v", model.ErrInvalidPacket, err)
	}
	if !pkt.HasPayload() {
		return fmt.Errorf("%w: packet %s has empty payload", model.ErrInvalidPacket, pkt.ID)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	key := model.PacketKey(pkt)
	if _, exists := s.byID[pkt.ID]; exists {
		return fmt.Errorf("packet %s already stored", pkt.ID)
	}
	s.packets = append(s.packets, pkt)
	s.byID[pkt.ID] = pkt
	s.byStream[pkt.Stream] = append(s.byStream[pkt.Stream], pkt.ID)
	if len(s.chunks) == 0 || s.chunks[len(s.chunks)-1] >= s.chunkSize {
		s.chunks = append(s.chunks, 0)
	}
	s.chunks[len(s.chunks)-1]++
	s.generation++
	if s.releaser != nil {
		s.releaser.Release(key + 1)
	}
	return nil
}

// Get 按 ID 返回报文。
func (s *Store) Get(id string) (model.Packet, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	pkt, ok := s.byID[id]
	return pkt, ok
}

// GetStream 返回指定流的全部报文；流不存在时返回空切片而不是 nil。
func (s *Store) GetStream(key model.StreamKey) []model.Packet {
	s.mu.RLock()
	defer s.mu.RUnlock()
	ids := s.byStream[key]
	if len(ids) == 0 {
		return []model.Packet{}
	}
	out := make([]model.Packet, 0, len(ids))
	for _, id := range ids {
		out = append(out, s.byID[id])
	}
	return out
}

// ListStreams 返回全部流的排序列表。
func (s *Store) ListStreams() []model.StreamKey {
	s.mu.RLock()
	defer s.mu.RUnlock()
	keys := make([]model.StreamKey, 0, len(s.byStream))
	for key := range s.byStream {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i].String() < keys[j].String() })
	return keys
}

// Stats 返回存储层统计快照。
func (s *Store) Stats() Stats {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return Stats{
		Packets:    len(s.packets),
		Streams:    len(s.byStream),
		Chunks:     len(s.chunks),
		Checksums:  len(s.packets),
		Generation: s.generation,
	}
}

// PacketCount 返回当前报文总数。
func (s *Store) PacketCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.packets)
}
