package capture

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"packetreplay/internal/model"
)

// LiveSource 是演示用的实时采集源：按固定节奏生成一组确定性报文。
type LiveSource struct {
	key       model.StreamKey
	startTime time.Time
	interval  time.Duration
	limit     int64
	emitted   atomic.Int64
	payload   []byte
}

// NewLiveSource 构造实时采集源。
func NewLiveSource(key model.StreamKey, start time.Time, interval time.Duration, limit int64, payload []byte) *LiveSource {
	return &LiveSource{
		key:       key,
		startTime: start,
		interval:  interval,
		limit:     limit,
		payload:   payload,
	}
}

// Open 实现 Source 接口。
func (s *LiveSource) Open() error {
	s.emitted.Store(0)
	return nil
}

// Next 生成下一条报文，达到上限后返回 io.EOF。
func (s *LiveSource) Next(ctx context.Context) (model.Packet, error) {
	if ctx.Err() != nil {
		return model.Packet{}, ctx.Err()
	}
	seq := s.emitted.Load()
	if seq >= s.limit {
		return model.Packet{}, EOF
	}
	if seq > 0 {
		select {
		case <-ctx.Done():
			return model.Packet{}, ctx.Err()
		case <-time.After(s.interval):
		}
	}
	s.emitted.Add(1)
	ts := s.startTime.Add(time.Duration(seq) * s.interval)
	return model.NewDatagram(s.key, uint64(seq), ts, s.payload), nil
}

// Close 实现 Source 接口。
func (s *LiveSource) Close() error {
	return nil
}

// Name 实现 Source 接口。
func (s *LiveSource) Name() string {
	return fmt.Sprintf("live:%s", s.key.String())
}
