package model

import (
	"fmt"
	"strconv"
	"strings"
)

// Proto 常量，用于五元组中的协议编号。
const (
	ProtoTCP = 6
	ProtoUDP = 17
)

// StreamKey 是流的五元组标识：源/目的地址、源/目的端口与协议号。
type StreamKey struct {
	SrcIP   string
	SrcPort uint16
	DstIP   string
	DstPort uint16
	Proto   uint8
}

// Validate 检查五元组各字段是否可用于建流。
func (k StreamKey) Validate() error {
	if k.SrcIP == "" || k.DstIP == "" {
		return fmt.Errorf("stream key has empty address")
	}
	if k.SrcPort == 0 || k.DstPort == 0 {
		return fmt.Errorf("stream key has zero port")
	}
	if k.Proto != ProtoTCP && k.Proto != ProtoUDP {
		return fmt.Errorf("stream key has unsupported protocol %d", k.Proto)
	}
	return nil
}

// String 生成五元组的规范文本形式。
func (k StreamKey) String() string {
	return fmt.Sprintf("%s:%d-%s:%d/%d", k.SrcIP, k.SrcPort, k.DstIP, k.DstPort, k.Proto)
}

// ProtoName 返回协议的可读名称。
func (k StreamKey) ProtoName() string {
	switch k.Proto {
	case ProtoTCP:
		return "tcp"
	case ProtoUDP:
		return "udp"
	default:
		return "unknown"
	}
}

// Ports 返回流的源端口与目的端口，供过滤与索引使用。
func (k StreamKey) Ports() (uint16, uint16) {
	return k.SrcPort, k.DstPort
}

// ParseStreamKey 解析 "ip:port-ip:port/proto" 形式的五元组文本。
func ParseStreamKey(text string) (StreamKey, error) {
	var key StreamKey
	text = strings.TrimSpace(text)
	endpoints, protoText, ok := strings.Cut(text, "/")
	if !ok {
		return key, fmt.Errorf("stream key %q missing protocol", text)
	}
	left, right, ok := strings.Cut(endpoints, "-")
	if !ok {
		return key, fmt.Errorf("stream key %q missing dash separator", text)
	}
	if _, err := parseEndpoint(left, &key.SrcIP, &key.SrcPort); err != nil {
		return key, fmt.Errorf("source endpoint: %w", err)
	}
	if _, err := parseEndpoint(right, &key.DstIP, &key.DstPort); err != nil {
		return key, fmt.Errorf("destination endpoint: %w", err)
	}
	switch protoText {
	case "tcp":
		key.Proto = ProtoTCP
	case "udp":
		key.Proto = ProtoUDP
	default:
		return key, fmt.Errorf("unknown protocol %q", protoText)
	}
	if err := key.Validate(); err != nil {
		return key, err
	}
	return key, nil
}

func parseEndpoint(text string, ip *string, port *uint16) (string, error) {
	addr, portText, ok := strings.Cut(strings.TrimSpace(text), ":")
	if !ok {
		return "", fmt.Errorf("endpoint %q missing port", text)
	}
	*ip = NormalizeIP(addr)
	value, err := strconv.ParseUint(portText, 10, 16)
	if err != nil || value == 0 {
		return "", fmt.Errorf("invalid port %q", portText)
	}
	*port = uint16(value)
	return "", nil
}
