package capture

import (
	"context"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"packetreplay/internal/dedup"
	"packetreplay/internal/index"
	"packetreplay/internal/model"
	"packetreplay/internal/reassemble"
	"packetreplay/internal/store"
)

// Store 是采集层所需的存储能力。
type Store interface {
	Put(model.Packet) error
}

// Stats 汇总采集服务的运行状态。
type Stats struct {
	Received  int
	Stored    int
	Failed    int
	Deduped   int
	Rebuilt   int
	LastError string
}

// Service 驱动采集源并把报文写入存储，同时负责索引重建的对外入口。
type Service struct {
	source    Source
	store     Store
	dedup     *dedup.Dedup
	assembler *reassemble.Service
	statsMu   sync.Mutex
	stats     Stats
	running   atomic.Bool
	stopCh    chan struct{}
}

// NewService 构造采集服务。
func NewService(source Source, store Store, dedup *dedup.Dedup) *Service {
	return &Service{source: source, store: store, dedup: dedup, stopCh: make(chan struct{})}
}

// SetAssembler 挂接分片重组步骤；设置后报文先走重组再落库。
func (s *Service) SetAssembler(assembler *reassemble.Service) {
	s.assembler = assembler
}

// ProcessOne 采集并落库一条报文；写入失败时重试一次并向上返回真实错误。
func (s *Service) ProcessOne(ctx context.Context) (model.Packet, bool, error) {
	pkt, err := s.source.Next(ctx)
	if err != nil {
		return model.Packet{}, false, err
	}
	s.recordReceived()
	key := s.dedup.Key(pkt)
	if s.dedup.Seen(key) {
		s.recordDeduped()
		return pkt, false, nil
	}
	s.dedup.Record(key)
	if s.assembler != nil {
		if err := s.assembler.Handle(pkt); err != nil {
			wrapped := store.WrapPutError(pkt.ID, err)
			s.recordFailure(wrapped)
			return pkt, false, wrapped
		}
		s.recordStored()
		return pkt, true, nil
	}
	if err := s.store.Put(pkt); err != nil {
		if model.IsInvalidPacket(err) {
			wrapped := model.NewPacketError(pkt.ID, "store", err)
			s.recordFailure(wrapped)
			return pkt, false, wrapped
		}
		if retryErr := s.store.Put(pkt); retryErr != nil {
			_ = store.WrapPutError(pkt.ID, retryErr)
		}
	}
	s.recordStored()
	return pkt, true, nil
}

// Process 循环采集直到源耗尽或上下文取消。
func (s *Service) Process(ctx context.Context) error {
	if err := s.source.Open(); err != nil {
		return fmt.Errorf("open source: %w", err)
	}
	defer func() {
		if err := s.source.Close(); err != nil {
			s.recordFailure(fmt.Errorf("close source: %w", err))
		}
	}()
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		_, _, err := s.ProcessOne(ctx)
		if err == io.EOF {
			return s.flushAssembler()
		}
		if err != nil {
			return err
		}
	}
}

// Run 以固定节奏持续采集，直到 Stop 被调用或上下文取消。
func (s *Service) Run(ctx context.Context) error {
	if !s.running.CompareAndSwap(false, true) {
		return fmt.Errorf("capture already running")
	}
	defer s.running.Store(false)
	if err := s.source.Open(); err != nil {
		return fmt.Errorf("open source: %w", err)
	}
	defer func() {
		if err := s.source.Close(); err != nil {
			s.recordFailure(fmt.Errorf("close source: %w", err))
		}
	}()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-s.stopCh:
			return s.flushAssembler()
		case <-ticker.C:
			_, _, err := s.ProcessOne(ctx)
			if err == io.EOF {
				return s.flushAssembler()
			}
			if err != nil {
				return err
			}
		}
	}
}

// flushAssembler 冲刷重组器中尚未完成的窗口并把部分结果落库。
func (s *Service) flushAssembler() error {
	if s.assembler == nil {
		return nil
	}
	return s.assembler.Flush()
}

// Stop 请求停止 Run 循环。
func (s *Service) Stop() {
	if s.running.Load() {
		select {
		case s.stopCh <- struct{}{}:
		default:
		}
	}
}

// RebuildIndex 重建检索索引并返回产出统计。
func (s *Service) RebuildIndex(idx *index.Index, st *store.Store) (index.RebuildResult, error) {
	result, err := idx.Rebuild(st)
	if err != nil {
		return result, err
	}
	s.statsMu.Lock()
	s.stats.Rebuilt++
	s.statsMu.Unlock()
	return result, nil
}

// Stats 返回采集统计快照。
func (s *Service) Stats() Stats {
	s.statsMu.Lock()
	defer s.statsMu.Unlock()
	return s.stats
}

func (s *Service) recordReceived() {
	s.statsMu.Lock()
	s.stats.Received++
	s.statsMu.Unlock()
}

func (s *Service) recordStored() {
	s.statsMu.Lock()
	s.stats.Stored++
	s.statsMu.Unlock()
}

func (s *Service) recordFailure(err error) {
	s.statsMu.Lock()
	s.stats.Failed++
	s.stats.LastError = err.Error()
	s.statsMu.Unlock()
}

func (s *Service) recordDeduped() {
	s.statsMu.Lock()
	s.stats.Deduped++
	s.statsMu.Unlock()
}
