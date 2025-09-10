package store

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"multithreaded-redis/internal/protocol"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// AOFFsyncPolicy defines when to fsync the AOF file
type AOFFsyncPolicy int

const (
	AOFFsyncNever    AOFFsyncPolicy = iota // Never fsync (fastest, least safe)
	AOFFsyncAlways                         // Fsync on every write (slowest, safest)
	AOFFsyncEverySec                       // Fsync every second (balanced)
)

type AOF struct {
	mu                sync.RWMutex
	file              *os.File
	path              string
	fsyncPolicy       AOFFsyncPolicy
	lastFsync         int64 // Unix timestamp of last fsync
	commandCount      int64 // Number of commands written
	rewriteSize       int64 // Trigger rewrite when file reaches this size
	rewriteInProgress atomic.Bool
	stopCh            chan struct{}
	fsyncTicker       *time.Ticker
}

func NewAOF(path string) (*AOF, error) {
	return NewAOFWithConfig(path, AOFFsyncEverySec, 64*1024*1024) // 64MB default rewrite size
}

// NewAOFWithConfig creates an AOF with custom configuration
func NewAOFWithConfig(path string, fsyncPolicy AOFFsyncPolicy, rewriteSize int64) (*AOF, error) {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}

	aof := &AOF{
		file:        f,
		path:        path,
		fsyncPolicy: fsyncPolicy,
		lastFsync:   time.Now().Unix(),
		rewriteSize: rewriteSize,
		stopCh:      make(chan struct{}),
	}

	// Start fsync ticker for AOFFsyncEverySec policy
	if fsyncPolicy == AOFFsyncEverySec {
		aof.fsyncTicker = time.NewTicker(time.Second)
		go aof.fsyncWorker()
	}

	return aof, nil
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
		return err
	}

	// Increment command count
	atomic.AddInt64(&a.commandCount, 1)

	// Handle fsync policy
	switch a.fsyncPolicy {
	case AOFFsyncAlways:
		if err := a.file.Sync(); err != nil {
			log.Printf("ERROR: Failed to fsync AOF: %v", err)
		}
		atomic.StoreInt64(&a.lastFsync, time.Now().Unix())
	case AOFFsyncNever:
		// No fsync
	case AOFFsyncEverySec:
		// Handled by fsyncWorker
	}

	// Check if rewrite is needed
	go a.checkRewrite()

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

	// Stop fsync worker
	if a.fsyncTicker != nil {
		a.fsyncTicker.Stop()
		a.fsyncTicker = nil
	}

	// Close stop channel only once
	select {
	case <-a.stopCh:
		// Channel already closed
	default:
		close(a.stopCh)
	}

	if a.file != nil {
		// Final fsync before closing
		a.file.Sync()
		return a.file.Close()
	}
	return nil
}

// fsyncWorker handles periodic fsync for AOFFsyncEverySec policy
func (a *AOF) fsyncWorker() {
	if a.fsyncTicker == nil {
		return
	}

	for {
		select {
		case <-a.fsyncTicker.C:
			a.mu.RLock()
			if a.file != nil && a.fsyncTicker != nil {
				if err := a.file.Sync(); err != nil {
					log.Printf("ERROR: Failed to fsync AOF: %v", err)
				} else {
					atomic.StoreInt64(&a.lastFsync, time.Now().Unix())
				}
			}
			a.mu.RUnlock()
		case <-a.stopCh:
			return
		}
	}
}

// checkRewrite checks if AOF rewrite is needed and triggers it
func (a *AOF) checkRewrite() {
	if a.rewriteInProgress.Load() {
		return // Rewrite already in progress
	}

	a.mu.RLock()
	stat, err := a.file.Stat()
	a.mu.RUnlock()

	if err != nil {
		return
	}

	// Check if file size exceeds rewrite threshold
	if stat.Size() > a.rewriteSize {
		go a.rewrite()
	}
}

// rewrite performs AOF rewrite to compact the file
func (a *AOF) rewrite() {
	if !a.rewriteInProgress.CompareAndSwap(false, true) {
		return // Another rewrite is in progress
	}
	defer a.rewriteInProgress.Store(false)

	log.Printf("AOF: Starting rewrite for %s", a.path)

	// Create temporary file for new AOF
	tempPath := a.path + ".tmp." + fmt.Sprintf("%d", time.Now().Unix())
	tempFile, err := os.Create(tempPath)
	if err != nil {
		log.Printf("AOF: Failed to create temp file for rewrite: %v", err)
		return
	}
	defer tempFile.Close()

	// Load all commands from current AOF
	commands, err := a.LoadCommands()
	if err != nil {
		log.Printf("AOF: Failed to load commands for rewrite: %v", err)
		os.Remove(tempPath)
		return
	}

	// Write compacted commands to temp file
	compactedCommands := a.compactCommands(commands)
	for _, cmd := range compactedCommands {
		if len(cmd) == 0 {
			continue
		}

		// Create RESP array
		respArray := make(protocol.Array, len(cmd))
		for i, arg := range cmd {
			respArray[i] = protocol.BulkString(arg)
		}

		// Write to temp file
		data := []byte(protocol.Encode(respArray))
		if _, err := tempFile.Write(data); err != nil {
			log.Printf("AOF: Failed to write to temp file: %v", err)
			os.Remove(tempPath)
			return
		}
	}

	// Sync temp file
	if err := tempFile.Sync(); err != nil {
		log.Printf("AOF: Failed to sync temp file: %v", err)
		os.Remove(tempPath)
		return
	}

	// Close temp file
	tempFile.Close()

	// Atomic replace: close current file, rename temp file
	a.mu.Lock()
	defer a.mu.Unlock()

	oldFile := a.file
	oldFile.Close()

	// Atomic rename
	if err := os.Rename(tempPath, a.path); err != nil {
		log.Printf("AOF: Failed to rename temp file: %v", err)
		// Try to reopen old file
		if f, reopenErr := os.OpenFile(a.path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644); reopenErr == nil {
			a.file = f
		}
		os.Remove(tempPath)
		return
	}

	// Reopen the new file for appending
	newFile, err := os.OpenFile(a.path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Printf("AOF: Failed to reopen AOF after rewrite: %v", err)
		return
	}

	a.file = newFile
	atomic.StoreInt64(&a.commandCount, int64(len(compactedCommands)))

	log.Printf("AOF: Rewrite completed. Compacted %d commands to %d", len(commands), len(compactedCommands))
}

// compactCommands removes redundant commands to minimize AOF size
func (a *AOF) compactCommands(commands [][]string) [][]string {
	// This is a simple compaction strategy
	// In production, you'd want more sophisticated logic

	// Track the latest value for each key
	keyStates := make(map[string][]string)
	hashStates := make(map[string]map[string]string)
	setStates := make(map[string]map[string]struct{})
	listStates := make(map[string][]string)
	zsetStates := make(map[string]map[string]float64)
	bloomStates := make(map[string][]string)        // Track items added to bloom filters
	cmsStates := make(map[string]map[string]uint32) // Track CMS increments

	for _, cmd := range commands {
		if len(cmd) == 0 {
			continue
		}

		switch cmd[0] {
		case "SET":
			if len(cmd) >= 3 {
				keyStates[cmd[1]] = cmd
				// Clear other data structures for this key
				delete(hashStates, cmd[1])
				delete(setStates, cmd[1])
				delete(listStates, cmd[1])
				delete(zsetStates, cmd[1])
				delete(bloomStates, cmd[1])
				delete(cmsStates, cmd[1])
			}
		case "DEL":
			if len(cmd) >= 2 {
				delete(keyStates, cmd[1])
				delete(hashStates, cmd[1])
				delete(setStates, cmd[1])
				delete(listStates, cmd[1])
				delete(zsetStates, cmd[1])
				delete(bloomStates, cmd[1])
				delete(cmsStates, cmd[1])
			}
		case "HSET":
			if len(cmd) >= 4 {
				// Clear other structures for this key
				delete(keyStates, cmd[1])
				delete(setStates, cmd[1])
				delete(listStates, cmd[1])
				delete(zsetStates, cmd[1])
				delete(bloomStates, cmd[1])
				delete(cmsStates, cmd[1])
				// Set hash field
				if hashStates[cmd[1]] == nil {
					hashStates[cmd[1]] = make(map[string]string)
				}
				hashStates[cmd[1]][cmd[2]] = cmd[3]
			}
		case "HDEL":
			if len(cmd) >= 3 {
				if hashStates[cmd[1]] != nil {
					for i := 2; i < len(cmd); i++ {
						delete(hashStates[cmd[1]], cmd[i])
					}
					// Remove hash entirely if empty
					if len(hashStates[cmd[1]]) == 0 {
						delete(hashStates, cmd[1])
					}
				}
			}
		case "SADD":
			if len(cmd) >= 3 {
				// Clear other structures for this key
				delete(keyStates, cmd[1])
				delete(hashStates, cmd[1])
				delete(listStates, cmd[1])
				delete(zsetStates, cmd[1])
				delete(bloomStates, cmd[1])
				delete(cmsStates, cmd[1])
				// Add to set
				if setStates[cmd[1]] == nil {
					setStates[cmd[1]] = make(map[string]struct{})
				}
				for i := 2; i < len(cmd); i++ {
					setStates[cmd[1]][cmd[i]] = struct{}{}
				}
			}
		case "SREM":
			if len(cmd) >= 3 && setStates[cmd[1]] != nil {
				for i := 2; i < len(cmd); i++ {
					delete(setStates[cmd[1]], cmd[i])
				}
				// Remove set entirely if empty
				if len(setStates[cmd[1]]) == 0 {
					delete(setStates, cmd[1])
				}
			}
		case "SPOP":
			if len(cmd) >= 3 && setStates[cmd[1]] != nil {
				for i := 2; i < len(cmd); i++ {
					delete(setStates[cmd[1]], cmd[i])
				}
				// Remove set entirely if empty
				if len(setStates[cmd[1]]) == 0 {
					delete(setStates, cmd[1])
				}
			}
		case "LPUSH", "RPUSH":
			if len(cmd) >= 3 {
				// Clear other structures for this key
				delete(keyStates, cmd[1])
				delete(hashStates, cmd[1])
				delete(setStates, cmd[1])
				delete(zsetStates, cmd[1])
				delete(bloomStates, cmd[1])
				delete(cmsStates, cmd[1])
				// For simplicity, just recreate the list with latest state
				// In a real implementation, you'd want to track the sequence
				if listStates[cmd[1]] == nil {
					listStates[cmd[1]] = []string{}
				}
				if cmd[0] == "LPUSH" {
					// Prepend to list
					for i := len(cmd) - 1; i >= 2; i-- {
						listStates[cmd[1]] = append([]string{cmd[i]}, listStates[cmd[1]]...)
					}
				} else {
					// Append to list
					listStates[cmd[1]] = append(listStates[cmd[1]], cmd[2:]...)
				}
			}
		case "LPOP", "RPOP":
			if len(cmd) >= 2 && listStates[cmd[1]] != nil && len(listStates[cmd[1]]) > 0 {
				if cmd[0] == "LPOP" {
					listStates[cmd[1]] = listStates[cmd[1]][1:]
				} else {
					listStates[cmd[1]] = listStates[cmd[1]][:len(listStates[cmd[1]])-1]
				}
				// Remove list entirely if empty
				if len(listStates[cmd[1]]) == 0 {
					delete(listStates, cmd[1])
				}
			}
		case "ZADD":
			if len(cmd) >= 4 {
				// Clear other structures for this key
				delete(keyStates, cmd[1])
				delete(hashStates, cmd[1])
				delete(setStates, cmd[1])
				delete(listStates, cmd[1])
				delete(bloomStates, cmd[1])
				delete(cmsStates, cmd[1])
				// Add to sorted set
				if zsetStates[cmd[1]] == nil {
					zsetStates[cmd[1]] = make(map[string]float64)
				}
				// Parse score and member
				if score, err := strconv.ParseFloat(cmd[2], 64); err == nil {
					zsetStates[cmd[1]][cmd[3]] = score
				}
			}
		case "BF.ADD":
			if len(cmd) >= 3 {
				// Clear other structures for this key
				delete(keyStates, cmd[1])
				delete(hashStates, cmd[1])
				delete(setStates, cmd[1])
				delete(listStates, cmd[1])
				delete(zsetStates, cmd[1])
				delete(cmsStates, cmd[1])
				// Add to bloom filter items
				bloomStates[cmd[1]] = append(bloomStates[cmd[1]], cmd[2])
			}
		case "CMS.INCR":
			if len(cmd) >= 4 {
				// Clear other structures for this key
				delete(keyStates, cmd[1])
				delete(hashStates, cmd[1])
				delete(setStates, cmd[1])
				delete(listStates, cmd[1])
				delete(zsetStates, cmd[1])
				delete(bloomStates, cmd[1])
				// Add to CMS increments
				if cmsStates[cmd[1]] == nil {
					cmsStates[cmd[1]] = make(map[string]uint32)
				}
				if count, err := strconv.ParseUint(cmd[3], 10, 32); err == nil {
					cmsStates[cmd[1]][cmd[2]] += uint32(count)
				}
			}
		}
	}

	// Rebuild compacted command list
	var compacted [][]string

	// Add SET commands
	for _, cmd := range keyStates {
		compacted = append(compacted, cmd)
	}

	// Add HSET commands
	for key, fields := range hashStates {
		for field, value := range fields {
			compacted = append(compacted, []string{"HSET", key, field, value})
		}
	}

	// Add SADD commands for sets
	for key, members := range setStates {
		if len(members) > 0 {
			cmd := []string{"SADD", key}
			for member := range members {
				cmd = append(cmd, member)
			}
			compacted = append(compacted, cmd)
		}
	}

	// Add list commands (LPUSH for simplicity - could be optimized)
	for key, items := range listStates {
		if len(items) > 0 {
			cmd := []string{"LPUSH", key}
			cmd = append(cmd, items...)
			compacted = append(compacted, cmd)
		}
	}

	// Add ZADD commands for sorted sets
	for key, members := range zsetStates {
		for member, score := range members {
			compacted = append(compacted, []string{"ZADD", key, fmt.Sprintf("%f", score), member})
		}
	}

	// Add BF.ADD commands for bloom filters
	for key, items := range bloomStates {
		for _, item := range items {
			compacted = append(compacted, []string{"BF.ADD", key, item})
		}
	}

	// Add CMS.INCR commands for count-min sketches
	for key, itemCounts := range cmsStates {
		for item, count := range itemCounts {
			compacted = append(compacted, []string{"CMS.INCR", key, item, fmt.Sprintf("%d", count)})
		}
	}

	return compacted
}

// LoadCommands reads and parses all commands from the AOF file
// Returns a slice of commands where each command is []string{cmd, arg1, arg2, ...}
func (a *AOF) LoadCommands() ([][]string, error) {
	// Reopen file for reading
	file, err := os.Open(a.file.Name())
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var commands [][]string
	reader := bufio.NewReader(file)

	for {
		// Parse RESP messages
		resp, err := protocol.ParseRESP(reader)
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("AOF: Error parsing RESP: %v", err)
			continue // Skip malformed entries
		}

		// Extract command from RESP array
		if cmd := extractCommandFromRESP(resp); cmd != nil {
			commands = append(commands, cmd)
		}
	}

	return commands, nil
}

// extractCommandFromRESP extracts a command slice from a RESP type
func extractCommandFromRESP(resp protocol.RESPType) []string {
	// Commands should be arrays
	arr, ok := resp.(protocol.Array)
	if !ok || len(arr) == 0 {
		return nil
	}

	cmd := make([]string, len(arr))
	for i, item := range arr {
		if bs, ok := item.(protocol.BulkString); ok {
			cmd[i] = string(bs)
		} else {
			return nil // Invalid command format
		}
	}
	return cmd
}

// GetStats returns AOF statistics
func (a *AOF) GetStats() AOFStats {
	a.mu.RLock()
	defer a.mu.RUnlock()

	var size int64
	if a.file != nil {
		if stat, err := a.file.Stat(); err == nil {
			size = stat.Size()
		}
	}

	return AOFStats{
		CommandCount:      atomic.LoadInt64(&a.commandCount),
		FileSize:          size,
		LastFsync:         atomic.LoadInt64(&a.lastFsync),
		FsyncPolicy:       a.fsyncPolicy,
		RewriteInProgress: a.rewriteInProgress.Load(),
		RewriteThreshold:  a.rewriteSize,
	}
}

// TriggerRewrite manually triggers an AOF rewrite
func (a *AOF) TriggerRewrite() {
	go a.rewrite()
}

// SetRewriteThreshold sets the file size threshold for automatic rewrites
func (a *AOF) SetRewriteThreshold(size int64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.rewriteSize = size
}

// AOFStats contains statistics about the AOF
type AOFStats struct {
	CommandCount      int64
	FileSize          int64
	LastFsync         int64
	FsyncPolicy       AOFFsyncPolicy
	RewriteInProgress bool
	RewriteThreshold  int64
}
