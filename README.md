# Multithreaded Redis Server

## Overview
This project is a high-performance, multithreaded Redis-compatible server written in Go. It is designed for maximum concurrency, efficient memory management, and robust persistence. The server supports most core Redis features, including sharding, pub/sub, AOF and RDB persistence, and advanced object pooling to minimize GC pressure.

## Key Features

### 1. Multithreaded Architecture
- Sharded key-value store with 16+ buckets for parallel access
- Each bucket uses independent RWMutex for lock contention reduction
- Optimized for thousands of concurrent clients

### 2. Network I/O Optimization
- Buffered network connections (8KB buffers, 10ms auto-flush)
- Efficient RESP protocol parsing with streaming and buffer pooling
- Low-latency response delivery for high-throughput scenarios

### 3. Advanced Memory Management
- PoolManager for object pooling (maps, slices, byte buffers, TTL entries, pub/sub messages)
- Reduces GC pressure and allocation overhead
- Pools are pointer-like to avoid slice header allocations (staticcheck SA6002 compliant)

### 4. Persistence
- **AOF (Append-Only File):**
  - Sharded AOF files for parallel write and recovery
  - Real-time logging of all write operations
  - Automatic rewrite when threshold exceeded (configurable)
  - Robust recovery: 100% command replay (SET, HSET, SADD, DEL, LPUSH, RPUSH, etc.)
- **RDB Snapshots:**
  - Periodic snapshots for fast recovery
  - Sharded dump files for scalability

### 5. Pub/Sub System
- Full support for SUBSCRIBE, UNSUBSCRIBE, and PUBLISH
- Multi-channel subscription and confirmation delivery
- High-performance message broadcasting

### 6. ACL & Authentication
- Pluggable ACL system with default user
- Command-level access control

### 7. Testing & Validation
- Comprehensive Python and Go test suite
- Performance benchmarks (240,000+ requests, 2.3x-2.8x speedup)
- Persistence, pub/sub, sharding, and ACL tests

## Directory Structure
- `cmd/server/` - Main server entry point
- `internal/net/` - Network, protocol, and handler logic
- `internal/store/` - Sharded store, persistence, pooling, ACL
- `internal/protocol/` - RESP parser and protocol utilities
- `internal/datastuctures/` - Bloom filters, count-min sketches, etc.
- `logs/` - AOF files
- `snapshots/` - RDB dump files
- `tests/` - Python and Go test scripts

## How It Works
- Start the server: `./server`
- Connect with any Redis client (Python, CLI, etc.)
- All write operations are logged to AOF and periodically snapshotted to RDB
- On restart, server replays AOF and loads RDB for full data recovery
- Pub/Sub channels support multi-channel subscriptions and fast message delivery
- Object pools minimize allocations and keep GC overhead low

## Performance Highlights
- Lock contention reduced via sharding (13,599 req/sec with 8 clients)
- Network I/O optimized for low latency
- GC pressure minimized with pointer-like pooling
- 100% persistence recovery (115,397 commands replayed flawlessly)
- Pub/Sub system supports multi-channel and high-throughput scenarios

## Future Enhancements
- **Cluster Mode:** Native support for distributed clusters and cross-node sharding
- **Lua Scripting:** Add support for EVAL and custom server-side scripts
- **Advanced Data Types:** Sorted sets, streams, geospatial, and more
- **Monitoring & Metrics:** Real-time stats, slowlog, and performance dashboards
- **TLS/SSL Support:** Secure connections for production deployments
- **Hot Backup & Restore:** Online backup and restore tools
- **Configurable Object Pools:** Dynamic pool sizing and statistics
- **Pluggable Storage Engines:** Support for alternative backends (RocksDB, Badger, etc.)
- **Improved ACL:** Role-based access, audit logging, and external auth providers

## Contributing
Pull requests and issues are welcome! See `todo.txt` for ideas and open tasks.

## License
MIT License

---

**Project Lead:** messdev072

**Special thanks to all contributors and testers!**
