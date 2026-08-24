package replay

import (
	"context"
	"fmt"
	"sync"

	"packetreplay/internal/index"
	"packetreplay/internal/model"
	"packetreplay/internal/stamp"
	"packetreplay/internal/store"
)

// Output 是回放输出的回调，HTTP 层与测试都通过它收集报文。
type Output func(model.Packet) error

// Stamper 抽象批内排序能力。
type Stamper interface {
	Order(pkts []model.Packet) []stamp.Tagged
}

// replayStamper 使用时间戳对齐模块完成批内排序。
type replayStamper struct{}

func (replayStamper) Order(pkts []model.Packet) []stamp.Tagged {
	return stamp.TagAndSortBatch(pkts)
}

// Stats 汇总回放服务的运行计数。
type Stats struct {
	Tasks      int
	Packets    int
	Duplicates int
	LastError  string
}

// ReplayService 负责把报文按块调度、按时间排序输出，并通过游标支持断点恢复。
type ReplayService struct {
	store       *store.Store
	index       *index.Index
	scheduler   *Scheduler
	cursorStore CursorStore
	out         Output
	stamper     Stamper
	tracker     *TaskTracker
	outputs     []model.Packet
	statsMu     sync.Mutex
	stats       Stats
}

// NewReplayService 构造回放服务。
func NewReplayService(st *store.Store, idx *index.Index, scheduler *Scheduler, cursorStore CursorStore, out Output) *ReplayService {
	return &ReplayService{
		store:       st,
		index:       idx,
		scheduler:   scheduler,
		cursorStore: cursorStore,
		out:         out,
		stamper:     replayStamper{},
		tracker:     NewTaskTracker(),
	}
}

// RunTask 回放存储中的全部报文，每块输出完成后保存一次游标。
func (s *ReplayService) RunTask(ctx context.Context) (int, error) {
	task := s.tracker.Begin("replay-all")
	packets := s.store.Ordered()
	version := s.index.CurrentVersion()
	chunks := s.scheduler.Chunks(len(packets))
	emitted := 0
	lastID := ""
	s.outputs = nil
	for _, bounds := range chunks {
		batch := packets[bounds[0]:bounds[1]]
		for _, tagged := range s.stamper.Order(batch) {
			if err := s.out(tagged.Packet); err != nil {
				return emitted, err
			}
			s.outputs = append(s.outputs, tagged.Packet)
			lastID = tagged.Packet.ID
			emitted++
		}
		if err := s.cursorStore.Save(Cursor{LastPacketID: lastID, IndexVersion: version}); err != nil {
			return emitted, err
		}
	}
	s.tracker.Complete(task.ID, emitted)
	s.record(emitted)
	return emitted, nil
}

// Replay 回放调用方给定的报文列表，不触碰游标。
func (s *ReplayService) Replay(ctx context.Context, packets []model.Packet) (int, error) {
	task := s.tracker.Begin("replay-batch")
	chunks := s.scheduler.Chunks(len(packets))
	emitted := 0
	s.outputs = nil
	for _, bounds := range chunks {
		batch := packets[bounds[0]:bounds[1]]
		for _, tagged := range s.stamper.Order(batch) {
			if err := s.out(tagged.Packet); err != nil {
				return emitted, err
			}
			s.outputs = append(s.outputs, tagged.Packet)
			emitted++
		}
	}
	s.tracker.Complete(task.ID, emitted)
	s.record(emitted)
	return emitted, nil
}

// ReplayStream 回放指定流的全部报文并记录流级任务；空流直接返回。
func (s *ReplayService) ReplayStream(ctx context.Context, key model.StreamKey) (int, error) {
	packets := s.store.GetStream(key)
	if len(packets) == 0 {
		return 0, nil
	}
	task := s.tracker.Begin("replay-stream-" + packets[0].ID)
	emitted, err := s.Replay(ctx, packets)
	if err != nil {
		return emitted, err
	}
	s.tracker.Complete(task.ID, emitted)
	return emitted, nil
}

// Resume 从游标恢复回放：先对齐最新索引，再只输出游标之后尚未回放的报文。
func (s *ReplayService) Resume(ctx context.Context) (int, error) {
	task := s.tracker.Begin("replay-resume")
	cursor, err := s.cursorStore.Load()
	if err != nil {
		return 0, err
	}
	if _, err := s.index.Rebuild(s.store); err != nil {
		return 0, fmt.Errorf("refresh index before resume: %w", err)
	}
	packets := s.store.Ordered()
	start := indexAfter(packets, cursor.LastPacketID)
	tail := packets[start:]
	emitted := 0
	lastID := cursor.LastPacketID
	s.outputs = nil
	for _, pkt := range tail {
		if err := s.out(pkt); err != nil {
			return emitted, err
		}
		s.outputs = append(s.outputs, pkt)
		lastID = pkt.ID
		emitted++
	}
	if err := s.cursorStore.Save(Cursor{LastPacketID: lastID, IndexVersion: s.index.CurrentVersion()}); err != nil {
		return emitted, err
	}
	s.tracker.MarkResumed(task.ID)
	s.tracker.Complete(task.ID, emitted)
	s.record(emitted)
	return emitted, nil
}

// indexAfter 返回列表中第一条排在给定 ID 之后的报文下标。
func indexAfter(packets []model.Packet, id string) int {
	if id == "" {
		return 0
	}
	for position, pkt := range packets {
		if pkt.ID == id {
			return position + 1
		}
	}
	return 0
}

// Stats 返回回放统计快照。
func (s *ReplayService) Stats() Stats {
	s.statsMu.Lock()
	defer s.statsMu.Unlock()
	return s.stats
}

func (s *ReplayService) record(emitted int) {
	s.statsMu.Lock()
	s.stats.Tasks++
	s.stats.Packets += emitted
	s.statsMu.Unlock()
}

// Tasks 返回任务状态快照。
func (s *ReplayService) Tasks() []Task {
	return s.tracker.Snapshot()
}

// Outputs 返回最近一次回放输出的报文副本。
func (s *ReplayService) Outputs() []model.Packet {
	out := make([]model.Packet, len(s.outputs))
	copy(out, s.outputs)
	return out
}

// PlannedChunks 返回当前存储按调度器切分后的计划块数。
func (s *ReplayService) PlannedChunks() int {
	return s.scheduler.ChunkCount(len(s.store.Ordered()))
}
