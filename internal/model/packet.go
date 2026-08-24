package model

import (
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/cespare/xxhash/v2"
)

// Packet 是流量采集与回放过程中的最小数据单元。一条报文可以是一个独立数据报，
// 也可以是分片流中的一片，分片信息通过 Fragment* 字段表达。
type Packet struct {
	ID         string
	Stream     StreamKey
	Seq        uint64
	IsFragment bool
	FragOffset uint32
	FragLen    uint32
	FragTotal  uint32
	Timestamp  time.Time
	Payload    []byte
	Proto      string
}

// DisplaySize 返回报文负载的展示大小，空负载也按 0 返回而不是 panic。
func (p Packet) DisplaySize() int {
	if len(p.Payload) == 0 {
		return 0
	}
	return len(p.Payload)
}

// HasPayload 判断报文是否携带有效负载。
func (p Packet) HasPayload() bool {
	return len(p.Payload) > 0
}

// Describe 生成用于日志与监控页面的一行摘要。
func (p Packet) Describe() string {
	frag := ""
	if p.IsFragment {
		frag = fmt.Sprintf(" frag=%d/%d", p.FragOffset, p.FragTotal)
	}
	return fmt.Sprintf(
		"%s %s seq=%d ts=%s len=%d%s",
		p.Stream.String(),
		p.Proto,
		p.Seq,
		p.Timestamp.Format(time.RFC3339Nano),
		p.DisplaySize(),
		frag,
	)
}

// Validate 检查报文核心字段是否可用于入库与回放。
func (p Packet) Validate() error {
	if p.ID == "" {
		return fmt.Errorf("packet id is empty")
	}
	if err := p.Stream.Validate(); err != nil {
		return err
	}
	if p.IsFragment && p.FragTotal == 0 {
		return fmt.Errorf("fragment %s declares zero total fragments", p.ID)
	}
	if p.IsFragment && p.FragOffset >= p.FragTotal {
		return fmt.Errorf("fragment %s offset %d out of range", p.ID, p.FragOffset)
	}
	return nil
}

// NewFragment 构造一个分片报文，ID 由流与序号组合生成。
func NewFragment(key StreamKey, seq uint64, offset, total uint32, ts time.Time, payload []byte) Packet {
	return Packet{
		ID:         fmt.Sprintf("%s-%d-%d", key.String(), seq, offset),
		Stream:     key,
		Seq:        seq,
		IsFragment: true,
		FragOffset: offset,
		FragLen:    uint32(len(payload)),
		FragTotal:  total,
		Timestamp:  ts,
		Payload:    payload,
		Proto:      key.ProtoName(),
	}
}

// NewDatagram 构造一条独立数据报。
func NewDatagram(key StreamKey, seq uint64, ts time.Time, payload []byte) Packet {
	return Packet{
		ID:        fmt.Sprintf("%s-%d", key.String(), seq),
		Stream:    key,
		Seq:       seq,
		Timestamp: ts,
		Payload:   payload,
		Proto:     key.ProtoName(),
	}
}

// MetadataTags 从协议与分片信息派生出用于检索的标签集合。
func (p Packet) MetadataTags() []string {
	tags := []string{p.Proto}
	if p.IsFragment {
		tags = append(tags, "fragment")
	}
	return tags
}

// ParseHexPayload 把形如 aabbcc 的十六进制文本解析为字节序列，供采集文件使用。
func ParseHexPayload(text string) ([]byte, error) {
	compact := strings.ReplaceAll(strings.TrimSpace(text), " ", "")
	if compact == "" {
		return nil, nil
	}
	if len(compact)%2 != 0 {
		return nil, fmt.Errorf("hex payload has odd length %d", len(compact))
	}
	out := make([]byte, 0, len(compact)/2)
	for i := 0; i < len(compact); i += 2 {
		high := hexNibble(compact[i])
		low := hexNibble(compact[i+1])
		if high < 0 || low < 0 {
			return nil, fmt.Errorf("invalid hex payload at %d", i)
		}
		out = append(out, byte(high<<4|low))
	}
	return out, nil
}

func hexNibble(c byte) int {
	switch {
	case c >= '0' && c <= '9':
		return int(c - '0')
	case c >= 'a' && c <= 'f':
		return int(c-'a') + 10
	case c >= 'A' && c <= 'F':
		return int(c-'A') + 10
	default:
		return -1
	}
}

// PacketKey 计算报文的去重键：五元组与负载的 xxhash 摘要，存储与去重共用同一口径。
func PacketKey(pkt Packet) uint64 {
	hasher := xxhash.New()
	_, _ = hasher.WriteString(pkt.Stream.String())
	_, _ = hasher.Write([]byte{0})
	_, _ = hasher.Write(pkt.Payload)
	return hasher.Sum64()
}

// NormalizeIP 规整 IP 字面量（IPv4 不带前导零），方便作为流键参与比较。
func NormalizeIP(text string) string {
	ip := net.ParseIP(strings.TrimSpace(text))
	if ip == nil {
		return strings.TrimSpace(text)
	}
	return ip.String()
}
