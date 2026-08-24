package stamp

import (
	"time"

	"packetreplay/internal/model"
)

// Tagged 是完成时间对齐后的报文，携带排序键与批内序号。
type Tagged struct {
	Packet model.Packet
	Key    int64
	Index  int
}

// TagBatch 给一批报文打上排序键，不打乱原始顺序；排序由 SortBatch 负责。
func TagBatch(pkts []model.Packet) []Tagged {
	out := make([]Tagged, 0, len(pkts))
	for index, pkt := range pkts {
		out = append(out, Tagged{Packet: pkt, Key: OrderKey(pkt), Index: index})
	}
	return out
}

// TagAndSortBatch 给一批报文打上排序键并严格按时间升序重排，同刻报文按序号稳定排列。
func TagAndSortBatch(pkts []model.Packet) []Tagged {
	tagged := TagBatch(pkts)
	for i := 1; i < len(tagged); i++ {
		current := tagged[i]
		pos := i
		for pos > 0 && Compare(tagged[pos-1].Packet, current.Packet) > 0 {
			tagged[pos] = tagged[pos-1]
			pos--
		}
		tagged[pos] = current
	}
	return tagged
}

// Window 描述一批报文的采集时间窗口，监控页面用它展示延迟。
type Window struct {
	Earliest time.Time
	Latest   time.Time
	Count    int
}

// MeasureWindow 计算一批报文的起止时间与数量。
func MeasureWindow(pkts []model.Packet) Window {
	if len(pkts) == 0 {
		return Window{}
	}
	earliest, latest := pkts[0].Timestamp, pkts[0].Timestamp
	for _, pkt := range pkts[1:] {
		if pkt.Timestamp.Before(earliest) {
			earliest = pkt.Timestamp
		}
		if pkt.Timestamp.After(latest) {
			latest = pkt.Timestamp
		}
	}
	return Window{Earliest: earliest, Latest: latest, Count: len(pkts)}
}

// Span 返回时间窗口的跨度。
func (w Window) Span() time.Duration {
	if w.Count == 0 {
		return 0
	}
	return w.Latest.Sub(w.Earliest)
}
