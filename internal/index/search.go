package index

import "packetreplay/internal/model"

// SearchPackets 执行检索并返回还原后的报文列表。
func (x *Index) SearchPackets(q Query) []model.Packet {
	ids := x.Search(q)
	return x.Resolve(ids)
}
