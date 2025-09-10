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

## Permission Patterns Guide

### Command Pattern Syntax

ACL permissions use a specific syntax to grant or deny command access:

#### Command Categories
```bash
+@read      # Allow all read commands
-@write     # Deny all write commands
+@admin     # Allow all admin commands
-@all       # Deny all commands (start with clean slate)
+@all       # Allow all commands
```

#### Individual Commands
```bash
+GET        # Allow GET command
-DEL        # Deny DEL command
+SET        # Allow SET command
-FLUSHDB    # Deny FLUSHDB command
```

#### Combined Patterns
```bash
# Start with no permissions, then add specific ones
-@all +@read +SET +DEL

# Allow all commands except dangerous ones
+@all -FLUSHDB -FLUSHALL -DEL

# Read-only with specific write permissions
+@read +SET +EXPIRE
```

### Key Pattern Syntax

Key patterns control which keys a user can access:

#### Basic Patterns
```bash
~*              # Access to all keys
~user:*         # Access to keys starting with "user:"
~cache:*        # Access to keys starting with "cache:"
~session:123    # Access only to specific key "session:123"
```

#### Wildcard Patterns
```bash
~user:*:profile     # Match user:123:profile, user:456:profile
~data:*:temp        # Match data:prod:temp, data:test:temp
~log:????-??-??     # Match log:2024-01-15, log:2024-12-31
```

#### Multiple Key Patterns
```bash
# Allow access to multiple key patterns
~user:* ~session:* ~cache:*

# Mix specific keys and patterns
~global_config ~user:* ~temp:*
```

#### Pattern Restrictions
```bash
# No key access (useful with +@admin for management-only users)
# (omit ~ patterns entirely)

# Limited to specific namespace
~myapp:*

# Multiple restricted namespaces
~app1:* ~app2:* ~shared:*
```

### Advanced Permission Examples

#### 1. Application-Specific User
```bash
# Web application user with restricted access
ACL SETUSER webapp >webpass123 -@all +@read +@write ~webapp:* ~session:*
```
**Explanation**: Deny all commands, then allow read/write operations only on keys matching `webapp:*` and `session:*`

#### 2. Database Migration User
```bash
# User for data migration with broad read access but limited write
ACL SETUSER migrator >migrate456 +@read +SET +DEL ~data:* ~backup:*
```
**Explanation**: Allow all read operations plus SET/DEL, but only on `data:*` and `backup:*` keys

#### 3. Monitoring User
```bash
# Read-only monitoring with access to metrics and logs
ACL SETUSER monitor >monitor789 +@read +INFO +PING ~metrics:* ~logs:* ~status:*
```
**Explanation**: Read operations plus server info commands, restricted to monitoring-related keys

#### 4. Cache Manager User
```bash
# User dedicated to cache management
ACL SETUSER cache_mgr >cache123 +@read +@write +EXPIRE +TTL -DEL ~cache:*
```
**Explanation**: Read/write access with expiration management but no delete permission, only for cache keys

#### 5. Admin User with Restrictions
```bash
# Admin user that cannot delete production data
ACL SETUSER safe_admin >admin789 +@all -DEL -FLUSHDB -FLUSHALL ~*
```
**Explanation**: Full admin access except for destructive operations

#### 6. Backup User
```bash
# Backup system with read-only access
ACL SETUSER backup >backup321 +@read +SCAN +KEYS ~*
```
**Explanation**: Read all data and enumerate keys for backup purposes

#### 7. Analytics User
```bash
# Analytics system with specific patterns
ACL SETUSER analytics >analytics456 +@read +SCAN ~analytics:* ~metrics:* ~events:*
```
**Explanation**: Read access limited to analytics, metrics, and events data

#### 8. Temporary Development User
```bash
# Developer with limited access to development namespace
ACL SETUSER dev_temp >devpass +@read +@write +FLUSHDB ~dev:* ~test:*
```
**Explanation**: Full read/write in development and test namespaces, can clear dev databases

### Permission Validation Patterns

#### Testing User Permissions
```bash
# Test read permission on specific key
GET user:123:profile

# Test write permission  
SET cache:temp:data "value"

# Test pattern matching
SET user:456:settings "config"  # Should work with ~user:*
SET admin:secret "data"         # Should fail if user doesn't have ~admin:*
```

#### Common Permission Combinations

| Use Case | Command Pattern | Key Pattern | Example |
|----------|----------------|-------------|---------|
| **Read-Only App** | `+@read` | `~app:*` | Web frontend reading data |
| **Write-Only Logger** | `+SET +SADD +LPUSH` | `~logs:*` | Log aggregation service |
| **Session Manager** | `+@read +@write +EXPIRE` | `~session:*` | Session storage service |
| **Cache Service** | `+@read +@write +DEL +EXPIRE` | `~cache:*` | Redis cache layer |
| **Admin Tools** | `+@all -FLUSHDB -FLUSHALL` | `~*` | Safe admin access |
| **Backup System** | `+@read +SCAN +KEYS` | `~*` | Data backup/replication |

### Permission Troubleshooting

#### Common Permission Errors
```bash
# Error: "ERR command not allowed"
# Cause: Command not in allowed categories
# Solution: Add command or category: +@read, +GET, etc.

# Error: "ERR key access denied" 
# Cause: Key doesn't match allowed patterns
# Solution: Add key pattern: ~mykey:*, ~pattern:*

# Error: "ERR user is disabled"
# Cause: User account disabled
# Solution: ACL SETUSER username on
```

#### Debugging Permission Issues
```bash
# Check user's current permissions
ACL LIST

# Test specific command/key combination
AUTH username password
GET test:key              # Test if this specific operation works
SET restricted:key value  # Test write permissions
```

#### Best Practice Patterns
```bash
# 1. Principle of Least Privilege
ACL SETUSER app >pass -@all +@read +SET +DEL ~app:*

# 2. Environment Separation  
ACL SETUSER prod_app >pass +@read +@write ~prod:*
ACL SETUSER dev_app >pass +@read +@write +FLUSHDB ~dev:*

# 3. Role-Based Access
ACL SETUSER reader >pass +@read ~*
ACL SETUSER writer >pass +@read +@write ~data:*
ACL SETUSER admin >pass +@all ~*
```

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
