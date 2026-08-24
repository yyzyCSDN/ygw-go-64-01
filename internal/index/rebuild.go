package index

import (
	"sort"

	"packetreplay/internal/model"
	"packetreplay/internal/store"
)

// RebuildResult 汇总一次重建的产出。
type RebuildResult struct {
	Built    int
	Merged   int
	Version  uint64
	Snapshot uint64
}

// Rebuild 从存储当前状态重建索引：先截取一致快照，重建完成后并入快照之后
// 新增的报文，保证重建期间写入的数据不丢失。
func (x *Index) Rebuild(src *store.Store) (RebuildResult, error) {
	snapshot := src.Snapshot()
	x.mu.Lock()
	lookup := x.lookup
	x.mu.Unlock()
	if lookup == nil {
		lookup = func(id string) (model.Packet, bool) { return src.Get(id) }
	}

	x.mu.Lock()
	x.resetLocked()
	built := 0
	for _, id := range snapshot.PacketIDs {
		if pkt, ok := lookup(id); ok {
			x.addLocked(pkt)
			built++
		}
	}
	x.mu.Unlock()

	// 重建期间新到达的报文：按快照之后的代际增量并入。
	delta := src.Since(snapshot.Version)
	x.mu.Lock()
	merged := 0
	for _, pkt := range delta {
		if !x.containsIDLocked(pkt.ID) {
			x.addLocked(pkt)
			merged++
		}
	}
	x.recordHistoryLocked(src.PacketCount())
	x.mu.Unlock()

	return RebuildResult{Built: built, Merged: merged, Version: x.CurrentVersion(), Snapshot: snapshot.Version}, nil
}

func (x *Index) resetLocked() {
	x.byStream = make(map[model.StreamKey][]string)
	x.byProto = make(map[string][]string)
	x.byPort = make(map[uint16][]string)
}

func (x *Index) containsIDLocked(id string) bool {
	for _, ids := range x.byStream {
		if contains(ids, id) {
			return true
		}
	}
	return false
}

// recordHistoryLocked 记录当前版本对应的有序 ID 快照。
func (x *Index) recordHistoryLocked(packetCount int) {
	all := make([]string, 0)
	for _, ids := range x.byStream {
		all = append(all, ids...)
	}
	sort.Strings(all)
	x.version = uint64(packetCount)
	x.history[x.version] = all
}
