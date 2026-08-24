package reassemble

import (
	"container/list"
	"fmt"
	"sync"

	"packetreplay/internal/model"
)

// assemblerEntry 记录每个流当前活跃的重组窗口。
type assemblerEntry struct {
	window *Window
}

// Assembler 按流维护重组窗口，窗口过期或被新窗口顶替时输出部分结果。
type Assembler struct {
	mu      sync.Mutex
	limit   int
	entries map[model.StreamKey]*assemblerEntry
	order   *list.List
	pending []model.Packet
	stats   Stats
}

// Stats 汇总重组器收到的分片与产出。
type Stats struct {
	Fragments   int
	Assembled   int
	Expired     int
	Dropped     int
	PassThrough int
}

// NewAssembler 构造带窗口上限的重组器。
func NewAssembler(limit int) *Assembler {
	return &Assembler{
		limit:   limit,
		entries: make(map[model.StreamKey]*assemblerEntry),
		order:   list.New(),
	}
}

// Feed 喂入一条报文：独立数据报直接透传，分片进入对应流的重组窗口。
func (a *Assembler) Feed(pkt model.Packet) (model.Packet, bool, error) {
	if !pkt.IsFragment {
		a.mu.Lock()
		a.stats.PassThrough++
		a.mu.Unlock()
		return pkt, true, nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.stats.Fragments++
	entry := a.entries[pkt.Stream]
	if entry == nil {
		entry = &assemblerEntry{window: NewWindow(pkt.Stream, pkt.Seq, pkt.FragTotal)}
		a.entries[pkt.Stream] = entry
		a.order.PushBack(pkt.Stream)
		a.enforceLimitLocked()
	} else if pkt.Seq > entry.window.Seq {
		a.expireEntryLocked(pkt.Stream, entry)
		entry = &assemblerEntry{window: NewWindow(pkt.Stream, pkt.Seq, pkt.FragTotal)}
		a.entries[pkt.Stream] = entry
		a.order.PushBack(pkt.Stream)
	}
	window := entry.window
	if pkt.Seq != window.Seq || !window.Within(pkt.FragOffset) {
		a.stats.Dropped++
		return model.Packet{}, false, nil
	}
	if err := window.Add(pkt.FragOffset, pkt.Payload); err != nil {
		a.stats.Dropped++
		return model.Packet{}, false, nil
	}
	if window.Complete() {
		payload := window.Assemble()
		a.stats.Assembled++
		delete(a.entries, pkt.Stream)
		assembled := model.Packet{
			ID:        fmt.Sprintf("%s-reassembled-%d", pkt.Stream.String(), pkt.Seq),
			Stream:    pkt.Stream,
			Seq:       pkt.Seq,
			Timestamp: pkt.Timestamp,
			Payload:   payload,
			Proto:     pkt.Proto,
		}
		return assembled, true, nil
	}
	return model.Packet{}, false, nil
}

// enforceLimitLocked 在活跃窗口超过上限时按最旧顺序淘汰并输出部分结果。
func (a *Assembler) enforceLimitLocked() {
	for a.order.Len() > a.limit {
		front := a.order.Front()
		key := front.Value.(model.StreamKey)
		a.order.Remove(front)
		entry := a.entries[key]
		if entry == nil {
			continue
		}
		a.expireEntryLocked(key, entry)
	}
}

// expireEntryLocked 把未完成窗口标记为过期并把部分结果放入待输出队列。
func (a *Assembler) expireEntryLocked(key model.StreamKey, entry *assemblerEntry) {
	window := entry.window
	if window.State == StateComplete {
		delete(a.entries, key)
		return
	}
	payload := window.Assemble()
	window.Expire()
	a.stats.Expired++
	if len(payload) > 0 {
		a.pending = append(a.pending, model.Packet{
			ID:        fmt.Sprintf("%s-partial-%d", key.String(), window.Seq),
			Stream:    key,
			Seq:       window.Seq,
			Timestamp: timeNow(),
			Payload:   payload,
			Proto:     key.ProtoName(),
		})
	}
	delete(a.entries, key)
}

// Drain 返回并清空待输出的过期部分结果。
func (a *Assembler) Drain() []model.Packet {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := a.pending
	a.pending = nil
	return out
}

// FlushExpired 输出全部未完成窗口的部分负载并清空状态，返回每个流的聚合报文。
func (a *Assembler) FlushExpired() []model.Packet {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make([]model.Packet, 0)
	for element := a.order.Front(); element != nil; {
		next := element.Next()
		key := element.Value.(model.StreamKey)
		entry := a.entries[key]
		if entry == nil {
			a.order.Remove(element)
			element = next
			continue
		}
		window := entry.window
		if window.State == StateComplete {
			a.order.Remove(element)
			delete(a.entries, key)
			element = next
			continue
		}
		a.expireEntryLocked(key, entry)
		out = append(out, a.pending...)
		a.pending = nil
		element = next
	}
	return out
}

// Stats 返回重组器统计快照。
func (a *Assembler) Stats() Stats {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.stats
}
