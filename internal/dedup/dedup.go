package dedup

import (
	"sync"
	"time"

	"packetreplay/internal/model"
)

// Dedup 维护报文去重窗口：同一窗口内相同负载只放行一次，窗口过后可再次入库。
type Dedup struct {
	mu     sync.Mutex
	window time.Duration
	seen   map[uint64]time.Time
	now    func() time.Time
}

// New 构造一个窗口时长的去重器。
func New(window time.Duration) *Dedup {
	return &Dedup{
		window: window,
		seen:   make(map[uint64]time.Time),
		now:    time.Now,
	}
}

// Key 计算报文的去重键：五元组 + 负载的 xxhash 摘要。
func (d *Dedup) Key(pkt model.Packet) uint64 {
	return model.PacketKey(pkt)
}

// Seen 判断键是否仍在去重窗口内。
func (d *Dedup) Seen(key uint64) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	_, ok := d.seen[key]
	return ok
}

// Record 把键记入去重窗口，返回是否首次记录。
func (d *Dedup) Record(key uint64) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	if _, ok := d.seen[key]; ok {
		return false
	}
	d.seen[key] = d.now()
	return true
}

// Release 从去重窗口移除键，允许同一报文在窗口内被再次入库。
func (d *Dedup) Release(key uint64) {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.seen, key)
}

// PurgeExpired 清理超过窗口时长的键，供定时任务调用。
func (d *Dedup) PurgeExpired(now time.Time) int {
	d.mu.Lock()
	defer d.mu.Unlock()
	removed := 0
	for key, recorded := range d.seen {
		if now.Sub(recorded) >= d.window {
			delete(d.seen, key)
			removed++
		}
	}
	return removed
}

// Len 返回当前窗口内的键数量。
func (d *Dedup) Len() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.seen)
}
