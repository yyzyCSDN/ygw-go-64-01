package filter

import (
	"sort"
	"strings"

	"packetreplay/internal/model"
)

// Filter 按规则集合过滤报文，规则之间为与关系。
type Filter struct {
	rules []Rule
}

// New 构造空过滤器。
func New() *Filter {
	return &Filter{}
}

// AddRule 追加一条规则。
func (f *Filter) AddRule(rule Rule) {
	f.rules = append(f.rules, rule)
}

// Rules 返回规则的排序副本。
func (f *Filter) Rules() []Rule {
	out := make([]Rule, len(f.rules))
	copy(out, f.rules)
	sort.Slice(out, func(i, j int) bool { return out[i].Describe() < out[j].Describe() })
	return out
}

// Match 判断单条报文是否通过全部规则。
func (f *Filter) Match(pkt model.Packet) bool {
	for _, rule := range f.rules {
		if !matchRule(rule, pkt) {
			return false
		}
	}
	return true
}

// matchRule 实现单条规则：范围规则使用包含端点的闭区间比较。
func matchRule(rule Rule, pkt model.Packet) bool {
	switch rule.Field {
	case FieldSrcPort, FieldDstPort:
		port := rule.portOf(pkt)
		return port >= rule.Lo && port <= rule.Hi
	case FieldProto:
		return strings.EqualFold(pkt.Proto, rule.Value)
	case FieldSrcIP:
		return strings.EqualFold(pkt.Stream.SrcIP, rule.Value)
	default:
		return false
	}
}

// Apply 返回通过全部规则的报文副本。
func (f *Filter) Apply(pkts []model.Packet) []model.Packet {
	out := make([]model.Packet, 0, len(pkts))
	for _, pkt := range pkts {
		if f.Match(pkt) {
			out = append(out, pkt)
		}
	}
	return out
}
