package store

import "fmt"

// Segment 描述一个存储块的范围与统计，块是容量与回放分片的边界单位。
type Segment struct {
	Index   int
	Start   int
	End     int
	Packets int
}

// Segments 按块边界切分报文序号区间，供回放与统计使用。
func (s *Store) Segments() []Segment {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]Segment, 0, len(s.chunks))
	cursor := 0
	for index, count := range s.chunks {
		segment := Segment{
			Index:   index,
			Start:   cursor,
			End:     cursor + count,
			Packets: count,
		}
		cursor += count
		out = append(out, segment)
	}
	return out
}

// DescribeSegment 生成块信息的可读文本。
func DescribeSegment(segment Segment) string {
	return fmt.Sprintf("segment[%d] packets=%d range=[%d,%d)", segment.Index, segment.Packets, segment.Start, segment.End)
}
