package net

import (
	"bufio"
	"net"
	"sync"
	"time"

	"multithreaded-redis/internal/protocol"
)

// BufferedConn wraps a net.Conn with buffered writing for better performance
type BufferedConn struct {
	conn   net.Conn
	writer *bufio.Writer
	mu     sync.Mutex

	// Flush control
	flushCh chan struct{}
	stopCh  chan struct{}
	once    sync.Once
}

// NewBufferedConn creates a new buffered connection wrapper
func NewBufferedConn(conn net.Conn, bufferSize int) *BufferedConn {
	bc := &BufferedConn{
		conn:    conn,
		writer:  bufio.NewWriterSize(conn, bufferSize),
		flushCh: make(chan struct{}, 1),
		stopCh:  make(chan struct{}),
	}

	// Start periodic flusher
	go bc.flushLoop()

	return bc
}

// Write writes data to the buffer
func (bc *BufferedConn) Write(data []byte) (int, error) {
	bc.mu.Lock()
	defer bc.mu.Unlock()

	n, err := bc.writer.Write(data)
	if err != nil {
		return n, err
	}

	// Trigger flush if buffer is getting full or for immediate responses
	if bc.writer.Buffered() > bc.writer.Size()/2 {
		bc.triggerFlush()
	}

	return n, nil
}

// WriteResponse writes a RESP response and triggers a flush
func (bc *BufferedConn) WriteResponse(resp interface{}) error {
	data := []byte(protocol.Encode(resp))
	_, err := bc.Write(data)
	if err != nil {
		return err
	}

	// Always flush after a complete response
	bc.triggerFlush()
	return nil
}

// triggerFlush requests a flush without blocking
func (bc *BufferedConn) triggerFlush() {
	select {
	case bc.flushCh <- struct{}{}:
	default:
		// Channel full, flush already pending
	}
}

// Flush forces a flush of the buffer
func (bc *BufferedConn) Flush() error {
	bc.mu.Lock()
	defer bc.mu.Unlock()
	return bc.writer.Flush()
}

// flushLoop handles periodic and triggered flushes
func (bc *BufferedConn) flushLoop() {
	ticker := time.NewTicker(10 * time.Millisecond) // Flush every 10ms max
	defer ticker.Stop()

	for {
		select {
		case <-bc.flushCh:
			bc.Flush()
		case <-ticker.C:
			// Periodic flush for any pending data
			bc.mu.Lock()
			if bc.writer.Buffered() > 0 {
				bc.writer.Flush()
			}
			bc.mu.Unlock()
		case <-bc.stopCh:
			// Final flush before closing
			bc.Flush()
			return
		}
	}
}

// Close closes the buffered connection
func (bc *BufferedConn) Close() error {
	bc.once.Do(func() {
		close(bc.stopCh)
		bc.Flush()
	})
	return bc.conn.Close()
}

// Forward other methods to the underlying connection
func (bc *BufferedConn) Read(b []byte) (int, error) {
	return bc.conn.Read(b)
}

func (bc *BufferedConn) LocalAddr() net.Addr {
	return bc.conn.LocalAddr()
}

func (bc *BufferedConn) RemoteAddr() net.Addr {
	return bc.conn.RemoteAddr()
}

func (bc *BufferedConn) SetDeadline(t time.Time) error {
	return bc.conn.SetDeadline(t)
}

func (bc *BufferedConn) SetReadDeadline(t time.Time) error {
	return bc.conn.SetReadDeadline(t)
}

func (bc *BufferedConn) SetWriteDeadline(t time.Time) error {
	return bc.conn.SetWriteDeadline(t)
}
