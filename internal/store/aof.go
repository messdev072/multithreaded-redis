package store

import (
	"log"
	"multithreaded-redis/internal/protocol"
	"os"
	"sync"
)

type AOF struct {
	mu   sync.Mutex
	file *os.File
}

func NewAOF(path string) (*AOF, error) {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}
	return &AOF{file: f}, nil
}

func (a *AOF) Append(cmd string, args ...string) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	// Store command in RESP format
	// Create an array with the command and all arguments
	respArray := make(protocol.Array, len(args)+1)
	respArray[0] = protocol.BulkString(cmd)
	for i, arg := range args {
		respArray[i+1] = protocol.BulkString(arg)
	}

	// Encode to RESP format
	data := []byte(protocol.Encode(respArray))

	// Write to file
	if _, err := a.file.Write(data); err != nil {
		// Log error but do not disrupt main flow
		log.Printf("ERROR: Failed to write to AOF: %v", err)
	}
	return nil
}

// Sync forces a sync of the AOF file to disk
func (a *AOF) Sync() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	
	return a.file.Sync()
}

// Close closes the AOF file
func (a *AOF) Close() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	
	if a.file != nil {
		return a.file.Close()
	}
	return nil
}
