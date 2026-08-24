package web

import (
	"sort"

	"packetreplay/internal/model"
)

// packetSummary 是返回给前端的最小报文视图。
type packetSummary struct {
	ID        string `json:"id"`
	Stream    string `json:"stream"`
	Proto     string `json:"proto"`
	Seq       uint64 `json:"seq"`
	Timestamp string `json:"timestamp"`
	Size      int    `json:"size"`
	Fragment  bool   `json:"fragment"`
	Header    string `json:"header"`
}

func summarize(pkt model.Packet) packetSummary {
	header := model.FromPacket(pkt)
	return packetSummary{
		ID:        pkt.ID,
		Stream:    pkt.Stream.String(),
		Proto:     pkt.Proto,
		Seq:       pkt.Seq,
		Timestamp: pkt.Timestamp.Format("2006-01-02T15:04:05.000Z"),
		Size:      pkt.DisplaySize(),
		Fragment:  pkt.IsFragment,
		Header:    header.Summarize(),
	}
}

func packetsSummary(packets []model.Packet) []packetSummary {
	out := make([]packetSummary, 0, len(packets))
	for _, pkt := range packets {
		out = append(out, summarize(pkt))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Timestamp < out[j].Timestamp })
	return out
}
