package dedup

import (
	"time"
)

// WindowDuration 返回去重窗口时长。
func (d *Dedup) WindowDuration() time.Duration {
	return d.window
}

// ExpiredCount 统计截至某个时刻已过期的键数量，不修改窗口内容。
func (d *Dedup) ExpiredCount(now time.Time) int {
	d.mu.Lock()
	defer d.mu.Unlock()
	count := 0
	for _, recorded := range d.seen {
		if now.Sub(recorded) >= d.window {
			count++
		}
	}
	return count
}
