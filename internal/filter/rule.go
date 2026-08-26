package filter

import (
	"fmt"
	"strconv"
	"strings"

	"packetreplay/internal/model"
)

// Field 表示规则作用的报文字段。
type Field string

const (
	FieldSrcPort Field = "srcport"
	FieldDstPort Field = "dstport"
	FieldProto   Field = "proto"
	FieldSrcIP   Field = "srcip"
)

// Op 表示比较操作。
type Op string

const (
	OpRange Op = "in"
	OpEqual Op = "eq"
)

// Rule 是一条过滤规则：范围规则使用闭区间 [Lo, Hi]。
type Rule struct {
	Field Field
	Op    Op
	Lo    uint16
	Hi    uint16
	Value string
}

// ParseRule 解析 "字段 操作 参数" 形式的规则文本，例如 "dstport in 8000 8002"。
func ParseRule(text string) (Rule, error) {
	parts := strings.Fields(strings.TrimSpace(text))
	if len(parts) < 3 {
		return Rule{}, fmt.Errorf("rule %q needs at least three parts", text)
	}
	rule := Rule{Field: Field(strings.ToLower(parts[0])), Op: Op(strings.ToLower(parts[1]))}
	switch rule.Op {
	case OpRange:
		if len(parts) != 4 {
			return Rule{}, fmt.Errorf("range rule %q needs lower and upper bounds", text)
		}
		lo, err := parsePort(parts[2])
		if err != nil {
			return Rule{}, fmt.Errorf("range lower bound: %w", err)
		}
		hi, err := parsePort(parts[3])
		if err != nil {
			return Rule{}, fmt.Errorf("range upper bound: %w", err)
		}
		if lo > hi {
			return Rule{}, fmt.Errorf("range lower bound %d exceeds upper bound %d", lo, hi)
		}
		rule.Lo, rule.Hi = lo, hi
	case OpEqual:
		rule.Value = parts[2]
	default:
		return Rule{}, fmt.Errorf("unsupported operator %q", parts[1])
	}
	if err := rule.Validate(); err != nil {
		return Rule{}, err
	}
	return rule, nil
}

func parsePort(text string) (uint16, error) {
	value, err := strconv.ParseUint(text, 10, 16)
	if err != nil || value == 0 {
		return 0, fmt.Errorf("invalid port %q", text)
	}
	return uint16(value), nil
}

// Validate 校验规则字段与操作组合的合法性。
func (r Rule) Validate() error {
	switch r.Field {
	case FieldSrcPort, FieldDstPort:
		if r.Op != OpRange {
			return fmt.Errorf("port field %s only supports range rules", r.Field)
		}
	case FieldProto, FieldSrcIP:
		if r.Op != OpEqual {
			return fmt.Errorf("field %s only supports equality rules", r.Field)
		}
	default:
		return fmt.Errorf("unsupported filter field %q", r.Field)
	}
	return nil
}

// Describe 生成规则的可读描述，供监控页面回显。
func (r Rule) Describe() string {
	if r.Op == OpRange {
		return fmt.Sprintf("%s in [%d,%d]", r.Field, r.Lo, r.Hi)
	}
	return fmt.Sprintf("%s = %s", r.Field, r.Value)
}

// portOf 取报文在规则字段上对应的端口。
func (r Rule) portOf(pkt model.Packet) uint16 {
	if r.Field == FieldDstPort {
		return pkt.Stream.DstPort
	}
	return pkt.Stream.SrcPort
}
