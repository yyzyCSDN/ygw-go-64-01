package model

import (
	"fmt"
)

// Header 描述一条报文的关键检索字段，索引与监控页面都依赖它。
type Header struct {
	Stream  StreamKey
	Proto   string
	SrcPort uint16
	DstPort uint16
	Seq     uint64
	Size    int
	Tags    []string
}

// FromPacket 从报文构造检索头。
func FromPacket(p Packet) Header {
	srcPort, dstPort := p.Stream.Ports()
	return Header{
		Stream:  p.Stream,
		Proto:   p.Proto,
		SrcPort: srcPort,
		DstPort: dstPort,
		Seq:     p.Seq,
		Size:    p.DisplaySize(),
		Tags:    p.MetadataTags(),
	}
}

// Summarize 生成头部的一行文本摘要。
func (h Header) Summarize() string {
	return fmt.Sprintf("%s %s seq=%d size=%d", h.Stream.String(), h.Proto, h.Seq, h.Size)
}
