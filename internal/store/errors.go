package store

import "fmt"

// WrapPutError 给 Put 失败补充报文级上下文，供上层审计使用。
func WrapPutError(packetID string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("store put %s: %w", packetID, err)
}
