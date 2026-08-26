package capture

import (
	"context"
	"fmt"
	"io"
	"strconv"
	"strings"

	"packetreplay/internal/model"
	"packetreplay/internal/stamp"
)

// Source 是采集源抽象：可以是一个抓包文件列表，也可以是一个实时生成器。
type Source interface {
	Open() error
	Next(ctx context.Context) (model.Packet, error)
	Close() error
	Name() string
}

// parsePacketLine 解析采集文件中的一行报文记录。
func parsePacketLine(text string) (model.Packet, error) {
	fields := make(map[string]string)
	for _, token := range strings.Fields(text) {
		key, value, ok := strings.Cut(token, "=")
		if !ok {
			return model.Packet{}, fmt.Errorf("malformed field %q", token)
		}
		fields[key] = value
	}
	keyText := fields["key"]
	if keyText == "" {
		return model.Packet{}, fmt.Errorf("missing stream key")
	}
	streamKey, err := model.ParseStreamKey(keyText)
	if err != nil {
		return model.Packet{}, fmt.Errorf("parse stream key: %w", err)
	}
	ts, err := stamp.ParseTimestamp(fields["ts"])
	if err != nil {
		return model.Packet{}, fmt.Errorf("parse timestamp: %w", err)
	}
	seq, err := parseUint64(fields["seq"])
	if err != nil {
		return model.Packet{}, fmt.Errorf("parse seq: %w", err)
	}
	payload, err := model.ParseHexPayload(fields["hex"])
	if err != nil {
		return model.Packet{}, fmt.Errorf("parse payload: %w", err)
	}
	if fields["frag"] == "1" {
		offset, err := parseUint32(fields["off"])
		if err != nil {
			return model.Packet{}, fmt.Errorf("parse fragment offset: %w", err)
		}
		total, err := parseUint32(fields["total"])
		if err != nil {
			return model.Packet{}, fmt.Errorf("parse fragment total: %w", err)
		}
		return model.NewFragment(streamKey, seq, offset, total, ts, payload), nil
	}
	pkt := model.NewDatagram(streamKey, seq, ts, payload)
	if err := pkt.Validate(); err != nil {
		return model.Packet{}, err
	}
	return pkt, nil
}

func parseUint64(text string) (uint64, error) {
	return strconv.ParseUint(strings.TrimSpace(text), 10, 64)
}

func parseUint32(text string) (uint32, error) {
	value, err := strconv.ParseUint(strings.TrimSpace(text), 10, 32)
	return uint32(value), err
}

// EOF 与 io.EOF 一致，采集循环用它判断源是否读完。
var EOF = io.EOF
