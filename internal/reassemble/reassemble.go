package reassemble

import (
	"time"

	"packetreplay/internal/model"
)

func timeNow() time.Time {
	return time.Now().UTC()
}

// Service 是重组模块对外的统一入口，采集层喂入报文，完整流在此产出。
type Service struct {
	assembler *Assembler
	store     Store
}

// Store 描述重组结果写入所需的存储能力。
type Store interface {
	Put(model.Packet) error
}

// NewService 构造重组服务。
func NewService(assembler *Assembler, store Store) *Service {
	return &Service{assembler: assembler, store: store}
}

// Handle 处理一条报文：重组完成后把聚合结果写入存储。
func (s *Service) Handle(pkt model.Packet) error {
	assembled, ok, err := s.assembler.Feed(pkt)
	if err != nil {
		return err
	}
	for _, partial := range s.assembler.Drain() {
		if err := s.store.Put(partial); err != nil {
			return err
		}
	}
	if ok && assembled.ID != "" {
		return s.store.Put(assembled)
	}
	return nil
}

// Flush 冲刷全部未完成窗口并把部分结果落库。
func (s *Service) Flush() error {
	partials := s.assembler.FlushExpired()
	for _, pkt := range partials {
		if err := s.store.Put(pkt); err != nil {
			return err
		}
	}
	return nil
}

// Stats 返回重组统计。
func (s *Service) Stats() Stats {
	return s.assembler.Stats()
}
