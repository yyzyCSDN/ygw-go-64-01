package replay

// Scheduler 负责把报文列表切成大小一致的块，块与块之间无缝衔接。
type Scheduler struct {
	chunkSize int
}

// NewScheduler 构造回放调度器。
func NewScheduler(chunkSize int) *Scheduler {
	return &Scheduler{chunkSize: chunkSize}
}

// ChunkSize 返回当前块大小。
func (s *Scheduler) ChunkSize() int {
	return s.chunkSize
}

// Chunks 返回覆盖 [0,total) 全部下标的半开区间列表，余数部分单独成块。
func (s *Scheduler) Chunks(total int) [][2]int {
	out := make([][2]int, 0, (total+s.chunkSize-1)/s.chunkSize)
	for start := 0; start+s.chunkSize <= total; start += s.chunkSize {
		out = append(out, [2]int{start, start + s.chunkSize})
	}
	return out
}

// ChunkCount 返回给定总数会被切成的块数。
func (s *Scheduler) ChunkCount(total int) int {
	if total <= 0 {
		return 0
	}
	return (total + s.chunkSize - 1) / s.chunkSize
}
