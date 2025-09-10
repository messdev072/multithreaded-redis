# Redis Server Authentication & ACL Guide

## Overview

This Redis server implementation includes a comprehensive Access Control List (ACL) system that provides enterprise-grade authentication and authorization capabilities. The system supports user management, command-level permissions, key pattern restrictions, and role-based access control.

## Features

- **User Authentication**: Password-based authentication with SHA-256 hashing
- **Command Categories**: Fine-grained permissions for different command types (@read, @write, @admin)
- **Key Pattern Security**: Restrict user access to specific key patterns (e.g., `user:*`, `cache:*`)
- **Role-Based Access**: Predefined roles (readonly, writeonly, admin) for common use cases
- **Connection State**: Per-connection authentication state management
- **Real-time Management**: Dynamic user creation, modification, and deletion

## Quick Start

### 1. Server Configuration

Start the server with authentication enabled:

```bash
# Start with authentication required
./redis-server -require-auth -default-password mypassword

# Start with custom port and authentication
./redis-server -port 6380 -require-auth -default-password secretpass
```

**Configuration Options:**
- `-require-auth`: Enable authentication requirement for all commands
- `-default-password <pass>`: Set password for the default user
- `-port <port>`: Specify server port (default: 6379)

### 2. Default Users

When started with `-require-auth`, the server automatically creates test users:

| Username | Password | Permissions | Description |
|----------|----------|-------------|-------------|
| `default` | (from `-default-password`) | All commands, all keys | Administrative user |
| `readonly` | `readonly123` | @read commands only | Read-only access |
| `writeonly` | `writeonly123` | @write commands only | Write-only access |

## Authentication Commands

### AUTH - User Authentication

```bash
# Authenticate as default user
AUTH mypassword

# Authenticate with specific username
AUTH username password

# Example
AUTH readonly readonly123
```

**Responses:**
- `OK` - Authentication successful
- `ERR invalid username or password` - Authentication failed

### ACL Commands

All ACL commands require admin privileges:

#### ACL LIST - List All Users
```bash
ACL LIST
```
Returns array of user definitions in ACL format.

#### ACL USERS - List Usernames
```bash
ACL USERS
```
Returns array of all usernames.

#### ACL SETUSER - Create/Modify User
```bash
# Create user with password
ACL SETUSER newuser >newpassword

# Set user categories and key patterns
ACL SETUSER newuser >password +@read +@write ~cache:* ~user:*

# Enable/disable user
ACL SETUSER newuser on    # Enable
ACL SETUSER newuser off   # Disable

# Reset user (remove all permissions)
ACL SETUSER newuser reset >newpassword
```

#### ACL DELUSER - Delete User
```bash
ACL DELUSER username

# Delete multiple users
ACL DELUSER user1 user2 user3
```

**Note:** Cannot delete the currently authenticated user or the default user.

## Command Categories

The system uses Redis-compatible command categories:

### @read - Read Operations
- `GET`, `MGET`, `EXISTS`, `TTL`, `TYPE`
- `SCARD`, `SMEMBERS`, `SISMEMBER`
- `HGET`, `HGETALL`, `HKEYS`, `HVALS`, `HLEN`
- `LLEN`, `LINDEX`, `LRANGE`
- `ZCARD`, `ZRANGE`, `ZSCORE`, `ZRANK`
- `BFEXISTS`

### @write - Write Operations
- `SET`, `MSET`, `DEL`, `EXPIRE`
- `SADD`, `SREM`, `SPOP`
- `HSET`, `HDEL`, `HINCRBY`
- `LPUSH`, `RPUSH`, `LPOP`, `RPOP`
- `ZADD`, `ZREM`, `ZINCRBY`
- `BFADD`

### @admin - Administrative Operations
- `ACL` commands
- `ADDNODE`, `REMOVENODE`, `NODES` (cluster management)
- `FLUSHDB`, `FLUSHALL`

## User Management Examples

### Creating a Database Administrator
```bash
# Full access user
ACL SETUSER dbadmin >admin123 +@all ~*
```

### Creating a Read-Only Analytics User
```bash
# Read-only access to analytics data
ACL SETUSER analytics >analytics456 +@read ~analytics:* ~reports:*
```

### Creating an Application User
```bash
# Application with read/write to specific patterns
ACL SETUSER myapp >app789 +@read +@write ~app:* ~session:* ~cache:*
```

### Creating a Backup User
```bash
# Read-only access to all data for backup purposes
ACL SETUSER backup >backup321 +@read ~*
```

## Security Best Practices

### 1. Password Security
- Use strong passwords (minimum 12 characters)
- Include mixed case, numbers, and special characters
- Rotate passwords regularly
- Never use default passwords in production

### 2. Principle of Least Privilege
```bash
# Bad: Too permissive
ACL SETUSER webapp >pass +@all ~*

# Good: Minimal necessary permissions
ACL SETUSER webapp >pass +@read +@write ~webapp:* ~session:*
```

### 3. Key Pattern Restrictions
```bash
# Restrict users to their data namespace
ACL SETUSER user1 >pass +@read +@write ~user1:*
ACL SETUSER user2 >pass +@read +@write ~user2:*
```

### 4. Admin Access Control
```bash
# Separate admin user for management tasks
ACL SETUSER admin >strongpassword +@admin +@read +@write ~*
```

## Client Integration

### Python Example
```python
import redis

# Connect and authenticate
client = redis.Redis(host='localhost', port=6379)
client.auth('username', 'password')

# Use authenticated connection
client.set('key', 'value')
data = client.get('key')
```

### Go Example
```go
package main

import (
    "github.com/go-redis/redis/v8"
    "context"
)

func main() {
    client := redis.NewClient(&redis.Options{
        Addr:     "localhost:6379",
        Username: "myuser",
        Password: "mypassword",
    })
    
    ctx := context.Background()
    client.Set(ctx, "key", "value", 0)
}
```

## Error Handling

### Common Error Messages

| Error | Cause | Solution |
|-------|-------|----------|
| `NOAUTH Authentication required` | Command executed without authentication | Use `AUTH` command first |
| `ERR invalid username or password` | Wrong credentials | Check username/password |
| `ERR user is disabled` | User account disabled | Enable user with `ACL SETUSER user on` |
| `ERR command not allowed` | Insufficient command permissions | Add required category with `+@category` |
| `ERR key access denied` | Key pattern restriction | Check key patterns with `~pattern` |

### Client Error Handling
```python
import redis

try:
    client = redis.Redis(host='localhost', port=6379)
    client.auth('username', 'password')
    client.set('restricted:key', 'value')
except redis.AuthenticationError:
    print("Authentication failed")
except redis.ResponseError as e:
    if "NOAUTH" in str(e):
        print("Authentication required")
    elif "not allowed" in str(e):
        print("Permission denied")
```

## Monitoring & Debugging

### Check Current User
```bash
# After authentication, you can verify your permissions
ACL USERS  # See if your user is listed
```

### Test Permissions
```bash
# Test read permission
GET test:key

# Test write permission  
SET test:key value

# Test admin permission
ACL LIST
```

### User Status Information
```bash
# View all users and their configurations
ACL LIST

# Example output:
# user default on nopass ~* +@all
# user readonly on >readonly123 ~* +@read
# user writeonly on >writeonly123 ~* +@write
```

## Production Deployment

### 1. Initial Setup
```bash
# Start server with authentication
./redis-server -require-auth -default-password $(openssl rand -base64 32)
```

### 2. Create Production Users
```bash
# Admin user
ACL SETUSER admin >$(openssl rand -base64 24) +@all ~*

# Application users
ACL SETUSER webapp >$(openssl rand -base64 24) +@read +@write ~app:* ~session:*
ACL SETUSER cache >$(openssl rand -base64 24) +@read +@write ~cache:*

# Monitoring user
ACL SETUSER monitor >$(openssl rand -base64 24) +@read ~*

# Disable default user (optional)
ACL SETUSER default off
```

### 3. Configuration Management
- Store user configurations in secure configuration management
- Use environment variables for passwords
- Implement password rotation procedures
- Monitor authentication attempts and failures

## Troubleshooting

### Common Issues

1. **Authentication Loops**
   - Symptom: Client repeatedly fails authentication
   - Solution: Check username/password, verify user is enabled

2. **Permission Denied**
   - Symptom: Commands fail with "not allowed" errors
   - Solution: Check user's command categories and key patterns

3. **Cannot Delete User**
   - Symptom: `ACL DELUSER` fails
   - Solution: Cannot delete currently authenticated user or default user

4. **Server Won't Start with Auth**
   - Symptom: Server fails to start with `-require-auth`
   - Solution: Ensure `-default-password` is provided

### Debug Mode
```bash
# Enable verbose logging (if implemented)
./redis-server -require-auth -default-password mypass -debug
```

## API Reference

### ACL Methods (Go Internal API)

```go
type ACLManager interface {
    AuthenticateUser(username, password string) (*User, error)
    CreateUser(username string) error
    DeleteUser(username string) error
    SetUserPassword(username, password string) error
    SetUserCategories(username string, categories []string) error
    SetUserKeys(username string, keyPatterns []string) error
    EnableUser(username string) error
    DisableUser(username string) error
    ListUsers() []string
    GetUser(username string) (*User, error)
    ValidateAccess(user *User, command string, key string) error
}
```

---

## Conclusion

This authentication system provides enterprise-grade security for Redis server deployments. By following the practices outlined in this guide, you can implement secure, scalable access control that meets the needs of modern applications while maintaining the performance characteristics expected from Redis.

For additional security considerations, consider implementing network-level security (firewalls, VPNs), TLS encryption for client connections, and comprehensive audit logging.
