package net

import (
	"fmt"
	"net"
	"runtime"
	"strings"
	"sync/atomic"
	"time"

	"multithreaded-redis/internal/protocol"
)

// ServerStats holds various server statistics
type ServerStats struct {
	startTime          time.Time
	commandsTotal      int64
	connectionsTotal   int64
	currentConnections int64
}

// Stats tracks server statistics
var stats ServerStats

// InitStats initializes server statistics
func InitStats() {
	stats.startTime = time.Now()
}

// IncrementCommands atomically increments the command counter
func IncrementCommands() {
	atomic.AddInt64(&stats.commandsTotal, 1)
}

// IncrementConnections atomically increments connection counters
func IncrementConnections() {
	atomic.AddInt64(&stats.connectionsTotal, 1)
	atomic.AddInt64(&stats.currentConnections, 1)
}

// DecrementConnections atomically decrements current connection counter
func DecrementConnections() {
	atomic.AddInt64(&stats.currentConnections, -1)
}

// GetUptime returns server uptime in seconds
func GetUptime() int64 {
	return int64(time.Since(stats.startTime).Seconds())
}

// GetCommandsTotal returns total commands processed
func GetCommandsTotal() int64 {
	return atomic.LoadInt64(&stats.commandsTotal)
}

// GetCurrentConnections returns current active connections
func GetCurrentConnections() int64 {
	return atomic.LoadInt64(&stats.currentConnections)
}

// GetConnectionsTotal returns total connections since start
func GetConnectionsTotal() int64 {
	return atomic.LoadInt64(&stats.connectionsTotal)
}

// GetMemoryStats returns memory usage information
func GetMemoryStats() (usedMemory int64, peakMemory int64, heapObjects int64) {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	usedMemory = int64(m.Alloc)
	peakMemory = int64(m.Sys)
	heapObjects = int64(m.HeapObjects)

	return
}

// formatInfoSection formats a section of the INFO response
func formatInfoSection(title string, fields map[string]interface{}) string {
	var result strings.Builder
	result.WriteString("# ")
	result.WriteString(title)
	result.WriteString("\r\n")

	for key, value := range fields {
		result.WriteString(fmt.Sprintf("%s:%v\r\n", key, value))
	}

	return result.String()
}

// HandleINFO handles the INFO command
func (s *Server) handleINFO(c net.Conn, args protocol.Array) {
	// Increment command counter
	IncrementCommands()

	var section string
	if len(args) > 1 {
		section = strings.ToLower(string(args[1].(protocol.BulkString)))
	} else {
		section = "all"
	}

	var result strings.Builder

	// Server section
	if section == "all" || section == "server" {
		serverInfo := map[string]interface{}{
			"redis_version":     "7.0.0-compatible",
			"redis_git_sha1":    "00000000",
			"redis_git_dirty":   0,
			"redis_build_id":    "multithreaded-redis",
			"redis_mode":        "standalone",
			"os":                runtime.GOOS,
			"arch_bits":         64,
			"multiplexing_api":  "epoll",
			"process_id":        1,
			"tcp_port":          6380,
			"uptime_in_seconds": GetUptime(),
			"uptime_in_days":    GetUptime() / 86400,
		}
		result.WriteString(formatInfoSection("Server", serverInfo))
	}

	// Clients section
	if section == "all" || section == "clients" {
		clientInfo := map[string]interface{}{
			"connected_clients":               GetCurrentConnections(),
			"client_recent_max_input_buffer":  0,
			"client_recent_max_output_buffer": 0,
			"blocked_clients":                 0,
		}
		result.WriteString(formatInfoSection("Clients", clientInfo))
	}

	// Memory section
	if section == "all" || section == "memory" {
		usedMem, peakMem, heapObjs := GetMemoryStats()

		memInfo := map[string]interface{}{
			"used_memory":             usedMem,
			"used_memory_human":       formatBytes(usedMem),
			"used_memory_rss":         peakMem,
			"used_memory_rss_human":   formatBytes(peakMem),
			"used_memory_peak":        peakMem,
			"used_memory_peak_human":  formatBytes(peakMem),
			"total_system_memory":     0,
			"maxmemory":               0,
			"mem_fragmentation_ratio": 1.0,
			"heap_objects":            heapObjs,
		}
		result.WriteString(formatInfoSection("Memory", memInfo))
	}

	// Persistence section
	if section == "all" || section == "persistence" {
		persistInfo := map[string]interface{}{
			"loading":                      0,
			"rdb_changes_since_last_save":  0,
			"rdb_bgsave_in_progress":       0,
			"rdb_last_save_time":           time.Now().Unix(),
			"rdb_last_bgsave_status":       "ok",
			"aof_enabled":                  1,
			"aof_rewrite_in_progress":      0,
			"aof_rewrite_scheduled":        0,
			"aof_last_rewrite_time_sec":    0,
			"aof_current_rewrite_time_sec": -1,
			"aof_last_bgrewrite_status":    "ok",
		}
		result.WriteString(formatInfoSection("Persistence", persistInfo))
	}

	// Stats section
	if section == "all" || section == "stats" {
		statsInfo := map[string]interface{}{
			"total_connections_received": GetConnectionsTotal(),
			"total_commands_processed":   GetCommandsTotal(),
			"instantaneous_ops_per_sec":  0, // TODO: Calculate this properly
			"total_net_input_bytes":      0,
			"total_net_output_bytes":     0,
			"rejected_connections":       0,
			"sync_full":                  0,
			"sync_partial_ok":            0,
			"sync_partial_err":           0,
			"expired_keys":               0,
			"evicted_keys":               0,
			"keyspace_hits":              0,
			"keyspace_misses":            0,
		}
		result.WriteString(formatInfoSection("Stats", statsInfo))
	}

	// CPU section
	if section == "all" || section == "cpu" {
		cpuInfo := map[string]interface{}{
			"used_cpu_sys":           0.0,
			"used_cpu_user":          0.0,
			"used_cpu_sys_children":  0.0,
			"used_cpu_user_children": 0.0,
		}
		result.WriteString(formatInfoSection("CPU", cpuInfo))
	}

	// Keyspace section
	if section == "all" || section == "keyspace" {
		keyspaceInfo := s.getKeyspaceInfo()
		if len(keyspaceInfo) > 0 {
			result.WriteString(formatInfoSection("Keyspace", keyspaceInfo))
		}
	}

	// Replication section
	if section == "all" || section == "replication" {
		replInfo := map[string]interface{}{
			"role":                "master",
			"connected_slaves":    0,
			"master_repl_offset":  0,
			"repl_backlog_active": 0,
			"repl_backlog_size":   1048576,
		}
		result.WriteString(formatInfoSection("Replication", replInfo))
	}

	response := result.String()
	if response == "" {
		response = "# Unknown section\r\n"
	}

	// Remove trailing \r\n and encode as bulk string
	response = strings.TrimSuffix(response, "\r\n")
	c.Write([]byte(protocol.Encode(protocol.BulkString(response))))
}

// getKeyspaceInfo returns keyspace statistics for each shard
func (s *Server) getKeyspaceInfo() map[string]interface{} {
	keyspaceInfo := make(map[string]interface{})

	// Get statistics from each shard
	if s.shards != nil {
		shardStats := s.shards.GetShardStats()

		totalKeys := 0
		totalExpires := 0

		for shardName, stats := range shardStats {
			totalKeys += stats.KeyCount
			totalExpires += stats.ExpiringKeys

			// Add per-shard info
			keyspaceInfo[shardName] = fmt.Sprintf("keys=%d,expires=%d",
				stats.KeyCount, stats.ExpiringKeys)
		}

		// Add total database info (Redis-style db0)
		if totalKeys > 0 {
			keyspaceInfo["db0"] = fmt.Sprintf("keys=%d,expires=%d,avg_ttl=0",
				totalKeys, totalExpires)
		}
	}

	return keyspaceInfo
}

// formatBytes formats byte count in human readable format
func formatBytes(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%dB", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f%cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}
