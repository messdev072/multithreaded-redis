package protocol

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
	"sync"
)

// BufferPool reuses byte slices to reduce GC pressure
var BufferPool = sync.Pool{
	New: func() interface{} {
		return make([]byte, 4096) // 4KB initial buffer
	},
}

// StringBuilderPool reuses strings.Builder instances
var StringBuilderPool = sync.Pool{
	New: func() interface{} {
		return &ByteBuffer{
			buf: make([]byte, 0, 512),
		}
	},
}

// ByteBuffer is a reusable byte buffer
type ByteBuffer struct {
	buf []byte
}

func (b *ByteBuffer) Reset() {
	b.buf = b.buf[:0]
}

func (b *ByteBuffer) Write(data []byte) {
	b.buf = append(b.buf, data...)
}

func (b *ByteBuffer) WriteByte(c byte) error {
	b.buf = append(b.buf, c)
	return nil
}

func (b *ByteBuffer) Bytes() []byte {
	return b.buf
}

func (b *ByteBuffer) String() string {
	return string(b.buf)
}

// StreamingParser provides an optimized RESP parser with buffer reuse
type StreamingParser struct {
	r          *bufio.Reader
	lineBuffer *ByteBuffer
}

// NewStreamingParser creates a new optimized RESP parser
func NewStreamingParser(r *bufio.Reader) *StreamingParser {
	return &StreamingParser{
		r:          r,
		lineBuffer: StringBuilderPool.Get().(*ByteBuffer),
	}
}

// Release returns the parser's buffers to the pool
func (p *StreamingParser) Release() {
	if p.lineBuffer != nil {
		p.lineBuffer.Reset()
		StringBuilderPool.Put(p.lineBuffer)
		p.lineBuffer = nil
	}
}

// ParseRESPOptimized parses RESP with optimized buffer reuse
func (p *StreamingParser) ParseRESP() (RESPType, error) {
	prefix, err := p.r.ReadByte()
	if err != nil {
		return nil, err
	}

	switch prefix {
	case '+': // Simple String
		line, err := p.readLine()
		if err != nil {
			return nil, err
		}
		return SimpleString(string(line)), nil

	case '-': // Error
		line, err := p.readLine()
		if err != nil {
			return nil, err
		}
		return Error(string(line)), nil

	case ':': // Integer
		line, err := p.readLine()
		if err != nil {
			return nil, err
		}
		val, err := strconv.ParseInt(string(line), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid integer: %s", line)
		}
		return Integer(val), nil

	case '$': // Bulk String
		line, err := p.readLine()
		if err != nil {
			return nil, err
		}
		length, err := strconv.Atoi(string(line))
		if err != nil {
			return nil, fmt.Errorf("invalid bulk string length: %s", line)
		}
		if length == -1 {
			return BulkString(nil), nil
		}
		if length < 0 {
			return nil, fmt.Errorf("invalid bulk string length: %d", length)
		}

		// Get buffer from pool
		var buf []byte
		if length <= 4096 {
			buf = BufferPool.Get().([]byte)
			defer BufferPool.Put(buf)
		} else {
			buf = make([]byte, length+2)
		}

		buf = buf[:length+2] // +2 for \r\n
		_, err = io.ReadFull(p.r, buf)
		if err != nil {
			return nil, err
		}

		// Copy the actual data (excluding \r\n)
		result := make([]byte, length)
		copy(result, buf[:length])
		return BulkString(result), nil

	case '*': // Array
		line, err := p.readLine()
		if err != nil {
			return nil, err
		}
		length, err := strconv.Atoi(string(line))
		if err != nil {
			return nil, fmt.Errorf("invalid array length: %s", line)
		}
		if length == -1 {
			return Array(nil), nil
		}
		if length < 0 {
			return nil, fmt.Errorf("invalid array length: %d", length)
		}

		arr := make(Array, length)
		for i := 0; i < length; i++ {
			elem, err := p.ParseRESP()
			if err != nil {
				return nil, err
			}
			arr[i] = elem
		}
		return arr, nil

	default:
		return nil, fmt.Errorf("invalid RESP prefix: %q", prefix)
	}
}

// readLine reads a line and trims \r\n, reusing the line buffer
func (p *StreamingParser) readLine() ([]byte, error) {
	p.lineBuffer.Reset()

	for {
		c, err := p.r.ReadByte()
		if err != nil {
			return nil, err
		}

		if c == '\r' {
			// Expect \n next
			n, err := p.r.ReadByte()
			if err != nil {
				return nil, err
			}
			if n != '\n' {
				return nil, fmt.Errorf("expected \\n after \\r, got %q", n)
			}
			break
		} else if c == '\n' {
			// Line ending without \r
			break
		} else {
			err := p.lineBuffer.WriteByte(c)
			if err != nil {
				return nil, err
			}
		}
	}

	return p.lineBuffer.Bytes(), nil
}

// ParseRESPOptimized is the main entry point for optimized parsing
func ParseRESPOptimized(r *bufio.Reader) (RESPType, error) {
	parser := NewStreamingParser(r)
	defer parser.Release()
	return parser.ParseRESP()
}
