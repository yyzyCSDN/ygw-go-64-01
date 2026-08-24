package stamp

import (
	"fmt"
	"time"

	"packetreplay/internal/model"
)

// ParseTimestamp 解析采集文件中的时间戳文本，支持 RFC3339 与毫秒时间戳。
func ParseTimestamp(text string) (time.Time, error) {
	trimmed := fmt.Sprint(text)
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339, "2006-01-02 15:04:05.000"} {
		if parsed, err := time.Parse(layout, trimmed); err == nil {
			return parsed, nil
		}
	}
	if millis, err := parseMillis(trimmed); err == nil {
		return time.UnixMilli(millis), nil
	}
	return time.Time{}, fmt.Errorf("unsupported timestamp %q", text)
}

func parseMillis(text string) (int64, error) {
	var value int64
	_, err := fmt.Sscanf(text, "%d", &value)
	return value, err
}

// OrderKey 计算报文在回放批内的排序键：毫秒时间戳为主键，序号为次键。
func OrderKey(pkt model.Packet) int64 {
	return pkt.Timestamp.UnixMilli()
}

// Compare 比较两条报文的时间顺序，同刻报文按序号升序。
func Compare(a, b model.Packet) int {
	ta, tb := a.Timestamp, b.Timestamp
	if ta.Before(tb) {
		return -1
	}
	if tb.Before(ta) {
		return 1
	}
	switch {
	case a.Seq < b.Seq:
		return -1
	case a.Seq > b.Seq:
		return 1
	default:
		return 0
	}
}
