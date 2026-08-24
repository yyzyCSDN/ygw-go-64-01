package model

import "errors"

// 包级错误定义，采集、存储与回放各层直接复用。
var (
	ErrInvalidPacket = errors.New("invalid packet")
	ErrStreamClosed  = errors.New("stream is closed")
)

// PacketError 包装一条报文在流转过程中产生的错误，保留报文 ID 用于审计。
type PacketError struct {
	PacketID string
	Stage    string
	Err      error
}

// Error 实现 error 接口。
func (e *PacketError) Error() string {
	return "packet " + e.PacketID + " failed at " + e.Stage + ": " + e.Err.Error()
}

// Unwrap 允许 errors.Is 穿透到原始错误。
func (e *PacketError) Unwrap() error {
	return e.Err
}

// NewPacketError 构造 PacketError。
func NewPacketError(packetID, stage string, err error) error {
	return &PacketError{PacketID: packetID, Stage: stage, Err: err}
}

// IsInvalidPacket 判断错误是否源于报文本身不合法。
func IsInvalidPacket(err error) bool {
	return errors.Is(err, ErrInvalidPacket)
}
