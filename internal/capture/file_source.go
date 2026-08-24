package capture

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"strings"

	"packetreplay/internal/model"
)

// Opener 抽象文件打开能力，采集测试通过计数 opener 观察句柄生命周期。
type Opener interface {
	Open(name string) (io.ReadCloser, error)
}

// osOpener 使用系统调用打开文件。
type osOpener struct{}

func (osOpener) Open(name string) (io.ReadCloser, error) {
	return os.Open(name)
}

// FileSource 按顺序读取一组抓包文件，读完当前文件自动轮转到下一个。
type FileSource struct {
	files   []string
	index   int
	current io.ReadCloser
	scanner *bufio.Scanner
	opener  Opener
	lineNo  int
	closed  bool
}

// NewFileSource 构造使用系统文件打开器的文件采集源。
func NewFileSource(files []string) *FileSource {
	return NewFileSourceWithOpener(files, osOpener{})
}

// NewFileSourceWithOpener 构造文件采集源并指定文件打开器，供测试注入计数实现。
func NewFileSourceWithOpener(files []string, opener Opener) *FileSource {
	return &FileSource{files: files, opener: opener}
}

// Open 打开文件列表中的第一个文件。
func (s *FileSource) Open() error {
	s.closed = false
	return s.openFile(0)
}

func (s *FileSource) openFile(index int) error {
	if index >= len(s.files) {
		return io.EOF
	}
	handle, err := s.opener.Open(s.files[index])
	if err != nil {
		return fmt.Errorf("open %s: %w", s.files[index], err)
	}
	s.current = handle
	s.scanner = bufio.NewScanner(handle)
	s.index = index
	s.lineNo = 0
	return nil
}

// Next 读取下一条报文；当前文件读完时轮转到下一个文件。
func (s *FileSource) Next(ctx context.Context) (model.Packet, error) {
	if s.closed {
		return model.Packet{}, model.ErrStreamClosed
	}
	for {
		if s.scanner == nil {
			return model.Packet{}, io.EOF
		}
		if !s.scanner.Scan() {
			if err := s.scanner.Err(); err != nil {
				return model.Packet{}, err
			}
			if s.index+1 >= len(s.files) {
				return model.Packet{}, io.EOF
			}
			if err := s.advance(); err != nil {
				return model.Packet{}, err
			}
			continue
		}
		s.lineNo++
		text := strings.TrimSpace(s.scanner.Text())
		if text == "" || strings.HasPrefix(text, "#") {
			continue
		}
		return parsePacketLine(text)
	}
}

// advance 关闭当前文件句柄并打开下一个文件，避免轮转时句柄泄漏。
func (s *FileSource) advance() error {
	if s.current != nil {
		_ = s.current.Close()
		s.current = nil
		s.scanner = nil
	}
	return s.openFile(s.index + 1)
}

// Close 关闭当前文件句柄并标记源已关闭。
func (s *FileSource) Close() error {
	s.closed = true
	if s.current == nil {
		return nil
	}
	err := s.current.Close()
	s.current = nil
	s.scanner = nil
	return err
}

// Name 返回采集源名称。
func (s *FileSource) Name() string {
	return "file-source"
}
