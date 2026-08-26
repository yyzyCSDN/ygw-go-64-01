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

// PacketSource 描述重建所需的存储视图能力，*store.Store 与测试包装器都满足它。
type PacketSource interface {
	Snapshot() store.Snapshot
	Since(version uint64) []model.Packet
	Get(id string) (model.Packet, bool)
	PacketCount() int
}

// Rebuild 从存储当前状态重建索引。
func (x *Index) Rebuild(src PacketSource) (RebuildResult, error) {
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
	x.recordHistoryLocked(src.PacketCount())
	x.mu.Unlock()

	return RebuildResult{Built: built, Merged: 0, Version: x.CurrentVersion(), Snapshot: snapshot.Version}, nil
}

func (x *Index) resetLocked() {
	x.byStream = make(map[model.StreamKey][]string)
	x.byProto = make(map[string][]string)
	x.byPort = make(map[uint16][]string)
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
