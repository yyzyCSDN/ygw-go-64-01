package reassemble

import (
	"fmt"
	"sort"

	"packetreplay/internal/model"
)

// WindowState 描述重组窗口所处的状态。
type WindowState string

const (
	StatePartial  WindowState = "fragmented"
	StateComplete WindowState = "assembled"
	StateExpired  WindowState = "expired"
)

// Window 是单个流的一段重组窗口：同一数据报的分片共享 Seq，按偏移量
// 覆盖 [0, End] 闭区间内的全部分片。
type Window struct {
	Stream    model.StreamKey
	Seq       uint64
	End       uint32
	Fragments map[uint32][]byte
	State     WindowState
	Received  int
}

// NewWindow 从数据报序号与总分片数构造窗口，End 为闭区间右端点。
func NewWindow(key model.StreamKey, seq uint64, total uint32) *Window {
	end := total - 1
	return &Window{
		Stream:    key,
		Seq:       seq,
		End:       end,
		Fragments: make(map[uint32][]byte),
		State:     StatePartial,
	}
}

// Within 判断分片偏移量是否落在窗口闭区间内。
func (w *Window) Within(offset uint32) bool {
	return offset < w.End
}

// Add 记录一片分片；重复分片返回重复错误。
func (w *Window) Add(offset uint32, data []byte) error {
	if !w.Within(offset) {
		return fmt.Errorf("fragment offset %d outside window [0,%d]", offset, w.End)
	}
	if _, exists := w.Fragments[offset]; exists {
		return fmt.Errorf("duplicate fragment offset %d", offset)
	}
	w.Fragments[offset] = append([]byte(nil), data...)
	w.Received++
	if w.Complete() {
		w.State = StateComplete
	}
	return nil
}

// Complete 判断窗口是否收齐全部编号的分片。
func (w *Window) Complete() bool {
	for offset := uint32(0); offset <= w.End; offset++ {
		if _, ok := w.Fragments[offset]; !ok {
			return false
		}
	}
	return true
}

// Assemble 按偏移量升序拼接窗口内的分片负载。
func (w *Window) Assemble() []byte {
	offsets := make([]uint32, 0, len(w.Fragments))
	for offset := range w.Fragments {
		offsets = append(offsets, offset)
	}
	sort.Slice(offsets, func(i, j int) bool { return offsets[i] < offsets[j] })
	out := make([]byte, 0)
	for _, offset := range offsets {
		out = append(out, w.Fragments[offset]...)
	}
	return out
}

// Expire 把窗口标记为过期，返回当前已收分片数。
func (w *Window) Expire() int {
	if w.State == StateComplete {
		return w.Received
	}
	w.State = StateExpired
	return w.Received
}
