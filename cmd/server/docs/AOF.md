# AOF (Append Only File) - Mandatory Persistence

## Overview

The Redis server now has **mandatory AOF (Append Only File) persistence** enabled by default. This ensures that all write operations are logged to disk for data durability and recovery.

## Features

- **Mandatory Persistence**: AOF is now required and cannot be disabled
- **Automatic Log Directory**: Creates log directory if it doesn't exist
- **Multi-Shard Support**: Each shard gets its own AOF file
- **RESP Protocol**: All commands logged in Redis RESP format
- **Thread-Safe**: Concurrent AOF writes are handled safely

## Command Line Options

```bash
./server [options]

Options:
  -addr string
        Server address to bind to (default ":6380")
  -logdir string
        Directory to store AOF log files (default "./logs")
```

## Usage Examples

### Default Configuration
```bash
# Starts server on :6380 with AOF files in ./logs/
./server
```

### Custom Port and Log Directory
```bash
# Custom port and log directory
./server -addr :7000 -logdir /var/log/redis
```

### Production Example
```bash
# Production setup with dedicated log directory
./server -addr :6379 -logdir /opt/redis/logs
```

## AOF File Structure

The server creates separate AOF files for each shard:

```
logs/
├── redis.aof.shard-0    # AOF for shard 0
└── redis.aof.shard-1    # AOF for shard 1
```

## RESP Format

All commands are stored in Redis RESP (Redis Serialization Protocol) format:

```
*3\r\n$3\r\nSET\r\n$4\r\nkey1\r\n$6\r\nvalue1\r\n
```

This represents: `SET key1 value1`

## Logged Commands

The following write commands are automatically logged to AOF:

- **SET** - String operations
- **HSET** - Hash field operations  
- **DEL** - Key deletion operations
- **[Future]** - Additional write operations as implemented

## File Management

- **Automatic Creation**: Log directory and AOF files are created automatically
- **Graceful Shutdown**: AOF files are properly closed on server shutdown
- **Error Handling**: AOF write failures are logged but don't stop the server

## Monitoring

Monitor AOF files for:
- **File Size Growth**: Indicates write activity
- **File Permissions**: Ensure writable by server process
- **Disk Space**: Monitor available disk space for log directory

## Recovery

AOF files can be used for data recovery by replaying the logged commands. The files contain all write operations in the order they were executed.

## Performance Notes

- **Non-Blocking**: AOF writes don't block command processing
- **Efficient**: Commands are written in binary RESP format
- **Concurrent**: Thread-safe operations across multiple shards
