package store

import (
	"sort"

	"packetreplay/internal/model"
)

// Snapshot 是存储在某一个代际上的报文视图，索引重建基于它启动。
type Snapshot struct {
	Version   uint64
	PacketIDs []string
}

// Snapshot 截取当前存储的一致视图。
func (s *Store) Snapshot() Snapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()
	ids := make([]string, 0, len(s.packets))
	for _, pkt := range s.packets {
		ids = append(ids, pkt.ID)
	}
	return Snapshot{Version: s.generation, PacketIDs: ids}
}

// Since 返回自某个代际之后新增的报文，供重建后合并增量。
func (s *Store) Since(version uint64) []model.Packet {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if version >= s.generation {
		return []model.Packet{}
	}
	start := 0
	for start < len(s.packets) && uint64(start) < version {
		start++
	}
	out := make([]model.Packet, 0, len(s.packets)-start)
	for _, pkt := range s.packets[start:] {
		out = append(out, pkt)
	}
	return out
}

// Resolve 把快照中的 ID 列表还原为报文切片，缺失的 ID 会被跳过。
func (s *Store) Resolve(snapshot Snapshot) []model.Packet {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]model.Packet, 0, len(snapshot.PacketIDs))
	for _, id := range snapshot.PacketIDs {
		if pkt, ok := s.byID[id]; ok {
			out = append(out, pkt)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Timestamp.Before(out[j].Timestamp) })
	return out
}

// Ordered 返回存储内全部报文按时间排序的副本。
func (s *Store) Ordered() []model.Packet {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]model.Packet, len(s.packets))
	copy(out, s.packets)
	sort.Slice(out, func(i, j int) bool { return out[i].Timestamp.Before(out[j].Timestamp) })
	return out
}
