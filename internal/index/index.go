package index

import (
	"fmt"
	"sort"
	"strings"
	"sync"

	"packetreplay/internal/model"
)

// Query 描述一次检索请求的过滤条件，条件之间为与关系。
type Query struct {
	StreamKey *model.StreamKey
	Proto     string
	Port      uint16
	SrcIP     string
	Text      string
}

// Lookup 按 ID 还原报文。
type Lookup func(id string) (model.Packet, bool)

// Index 是进程内的检索索引：按流、协议、端口维护反向映射，并保留版本化快照
// 供回放游标恢复与审计使用。
type Index struct {
	mu       sync.RWMutex
	byStream map[model.StreamKey][]string
	byProto  map[string][]string
	byPort   map[uint16][]string
	history  map[uint64][]string
	version  uint64
	lookup   Lookup
}

// New 构造空索引。
func New() *Index {
	return &Index{
		byStream: make(map[model.StreamKey][]string),
		byProto:  make(map[string][]string),
		byPort:   make(map[uint16][]string),
		history:  make(map[uint64][]string),
	}
}

// SetLookup 挂接报文还原回调，供搜索与快照还原使用。
func (x *Index) SetLookup(lookup Lookup) {
	x.mu.Lock()
	defer x.mu.Unlock()
	x.lookup = lookup
}

// Add 把一条报文并入索引。
func (x *Index) Add(pkt model.Packet) {
	x.mu.Lock()
	defer x.mu.Unlock()
	x.addLocked(pkt)
}

func (x *Index) addLocked(pkt model.Packet) {
	if !contains(x.byStream[pkt.Stream], pkt.ID) {
		x.byStream[pkt.Stream] = append(x.byStream[pkt.Stream], pkt.ID)
	}
	proto := strings.ToLower(pkt.Proto)
	if !contains(x.byProto[proto], pkt.ID) {
		x.byProto[proto] = append(x.byProto[proto], pkt.ID)
	}
	if !contains(x.byPort[pkt.Stream.DstPort], pkt.ID) {
		x.byPort[pkt.Stream.DstPort] = append(x.byPort[pkt.Stream.DstPort], pkt.ID)
	}
}

// Search 按查询条件返回匹配报文的 ID 列表。
func (x *Index) Search(q Query) []string {
	x.mu.RLock()
	defer x.mu.RUnlock()
	var candidates []string
	first := true
	apply := func(ids []string) {
		if first {
			candidates = append(candidates, ids...)
			first = false
			return
		}
		candidates = intersect(candidates, ids)
	}
	if q.StreamKey != nil {
		apply(x.byStream[*q.StreamKey])
	}
	if q.Proto != "" {
		apply(x.byProto[strings.ToLower(q.Proto)])
	}
	if q.Port != 0 {
		apply(x.byPort[q.Port])
	}
	if q.Text != "" {
		apply(x.textMatches(q.Text))
	}
	if first {
		return []string{}
	}
	out := make([]string, len(candidates))
	copy(out, candidates)
	sort.Strings(out)
	return out
}

func (x *Index) textMatches(text string) []string {
	out := make([]string, 0)
	needle := strings.ToLower(text)
	for streamKey, ids := range x.byStream {
		if strings.Contains(strings.ToLower(streamKey.String()), needle) {
			out = append(out, ids...)
		}
	}
	return out
}

// Resolve 把 ID 列表还原为按时间排序的报文。
func (x *Index) Resolve(ids []string) []model.Packet {
	x.mu.RLock()
	lookup := x.lookup
	x.mu.RUnlock()
	out := make([]model.Packet, 0, len(ids))
	if lookup == nil {
		return out
	}
	for _, id := range ids {
		if pkt, ok := lookup(id); ok {
			out = append(out, pkt)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Timestamp.Before(out[j].Timestamp) })
	return out
}

// CurrentVersion 返回索引当前对应的存储代际。
func (x *Index) CurrentVersion() uint64 {
	x.mu.RLock()
	defer x.mu.RUnlock()
	return x.version
}

// PacketsAt 返回某个版本快照中的报文 ID 列表（按存储顺序）。
func (x *Index) PacketsAt(version uint64) []string {
	x.mu.RLock()
	defer x.mu.RUnlock()
	ids, ok := x.history[version]
	if !ok {
		return []string{}
	}
	out := make([]string, len(ids))
	copy(out, ids)
	return out
}

// History 返回全部版本快照的键列表，供审计展示。
func (x *Index) History() []uint64 {
	x.mu.RLock()
	defer x.mu.RUnlock()
	versions := make([]uint64, 0, len(x.history))
	for version := range x.history {
		versions = append(versions, version)
	}
	sort.Slice(versions, func(i, j int) bool { return versions[i] < versions[j] })
	return versions
}

// Size 返回索引中已登记的报文数。
func (x *Index) Size() int {
	x.mu.RLock()
	defer x.mu.RUnlock()
	total := 0
	for _, ids := range x.byStream {
		total += len(ids)
	}
	return total
}

func contains(ids []string, id string) bool {
	for _, value := range ids {
		if value == id {
			return true
		}
	}
	return false
}

func intersect(a, b []string) []string {
	set := make(map[string]bool, len(b))
	for _, id := range b {
		set[id] = true
	}
	out := make([]string, 0)
	for _, id := range a {
		if set[id] {
			out = append(out, id)
		}
	}
	return out
}

// Describe 生成索引状态的文本摘要。
func (x *Index) Describe() string {
	x.mu.RLock()
	defer x.mu.RUnlock()
	return fmt.Sprintf("index version=%d entries=%d snapshots=%d", x.version, x.entryCountLocked(), len(x.history))
}

func (x *Index) entryCountLocked() int {
	total := 0
	for _, ids := range x.byStream {
		total += len(ids)
	}
	return total
}
