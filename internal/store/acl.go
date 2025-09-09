package store

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"sync"
	"time"
)

// ACLUser represents a user in the ACL system
type ACLUser struct {
	Username     string
	PasswordHash string
	Enabled      bool
	Commands     map[string]bool // allowed commands, true = allowed, false = denied
	Categories   map[string]bool // command categories (+@read, -@write, etc.)
	Keys         []string        // key patterns this user can access
	Channels     []string        // pub/sub channels this user can access
	Flags        []string        // user flags (on, off, nopass, etc.)
	CreatedAt    time.Time
	LastLogin    time.Time
}

// ACLManager manages authentication and authorization
type ACLManager struct {
	users   map[string]*ACLUser
	mu      sync.RWMutex
	enabled bool
	logAuth bool
}

// Command categories for Redis commands
var CommandCategories = map[string][]string{
	"read": {
		"GET", "MGET", "HGET", "HGETALL", "HKEYS", "HVALS", "HMGET",
		"SISMEMBER", "SMEMBERS", "SCARD", "LRANGE", "LLEN", "LINDEX",
		"ZRANGE", "ZCARD", "ZCOUNT", "ZSCORE", "TYPE", "EXISTS",
		"TTL", "PTTL", "KEYS", "SCAN", "HSCAN", "SSCAN", "ZSCAN",
	},
	"write": {
		"SET", "MSET", "DEL", "HSET", "HMSET", "HDEL", "SADD", "SREM",
		"LPUSH", "RPUSH", "LPOP", "RPOP", "ZADD", "ZREM", "EXPIRE",
		"EXPIREAT", "PERSIST", "RENAME", "MOVE", "FLUSHDB", "FLUSHALL",
	},
	"admin": {
		"FLUSHDB", "FLUSHALL", "CONFIG", "DEBUG", "SHUTDOWN", "SAVE",
		"BGSAVE", "BGREWRITEAOF", "ACL", "AUTH", "INFO", "MONITOR",
	},
	"pubsub": {
		"PUBLISH", "SUBSCRIBE", "UNSUBSCRIBE", "PSUBSCRIBE", "PUNSUBSCRIBE",
	},
	"connection": {
		"AUTH", "QUIT", "PING", "ECHO", "SELECT", "CLIENT",
	},
}

// NewACLManager creates a new ACL manager
func NewACLManager() *ACLManager {
	acl := &ACLManager{
		users:   make(map[string]*ACLUser),
		enabled: true,
		logAuth: true,
	}

	// Create default user
	defaultUser := &ACLUser{
		Username:     "default",
		PasswordHash: "",
		Enabled:      true,
		Commands:     make(map[string]bool),
		Categories:   make(map[string]bool),
		Keys:         []string{"*"},
		Channels:     []string{"*"},
		Flags:        []string{"on", "nopass", "allcommands", "allkeys"},
		CreatedAt:    time.Now(),
	}

	// Default user has access to all commands
	for category, commands := range CommandCategories {
		defaultUser.Categories["+@"+category] = true
		for _, cmd := range commands {
			defaultUser.Commands[cmd] = true
		}
	}

	acl.users["default"] = defaultUser
	return acl
}

// hashPassword creates a SHA256 hash of the password
func (acl *ACLManager) hashPassword(password string) string {
	if password == "" {
		return ""
	}
	hash := sha256.Sum256([]byte(password))
	return hex.EncodeToString(hash[:])
}

// RequireAuthentication configures the ACL system to require authentication
// This disables the default user's nopass setting
func (acl *ACLManager) RequireAuthentication() error {
	acl.mu.Lock()
	defer acl.mu.Unlock()

	defaultUser, exists := acl.users["default"]
	if !exists {
		return fmt.Errorf("default user not found")
	}

	// Remove nopass flag and disable default user if no password set
	newFlags := make([]string, 0)
	for _, flag := range defaultUser.Flags {
		if flag != "nopass" {
			newFlags = append(newFlags, flag)
		}
	}

	// If no password is set, disable the default user
	if defaultUser.PasswordHash == "" {
		defaultUser.Enabled = false
		newFlags = append(newFlags, "off")
	}

	defaultUser.Flags = newFlags
	return nil
}

// SetDefaultUserPassword sets a password for the default user
func (acl *ACLManager) SetDefaultUserPassword(password string) error {
	acl.mu.Lock()
	defer acl.mu.Unlock()

	defaultUser, exists := acl.users["default"]
	if !exists {
		return fmt.Errorf("default user not found")
	}

	defaultUser.PasswordHash = acl.hashPassword(password)

	// Remove nopass flag and add password authentication requirement
	newFlags := make([]string, 0)
	for _, flag := range defaultUser.Flags {
		if flag != "nopass" {
			newFlags = append(newFlags, flag)
		}
	}

	defaultUser.Flags = newFlags
	defaultUser.Enabled = true

	return nil
}

// CreateUser creates a new user
func (acl *ACLManager) CreateUser(username, password string) error {
	acl.mu.Lock()
	defer acl.mu.Unlock()

	if _, exists := acl.users[username]; exists {
		return fmt.Errorf("user %s already exists", username)
	}

	user := &ACLUser{
		Username:     username,
		PasswordHash: acl.hashPassword(password),
		Enabled:      true,
		Commands:     make(map[string]bool),
		Categories:   make(map[string]bool),
		Keys:         []string{},
		Channels:     []string{},
		Flags:        []string{"on"},
		CreatedAt:    time.Now(),
	}

	if password == "" {
		user.Flags = append(user.Flags, "nopass")
	}

	acl.users[username] = user
	return nil
}

// SetUserCategories sets command categories for a user
func (acl *ACLManager) SetUserCategories(username string, categories []string) error {
	acl.mu.Lock()
	defer acl.mu.Unlock()

	user, exists := acl.users[username]
	if !exists {
		return fmt.Errorf("user %s not found", username)
	}

	// Clear existing categories
	user.Categories = make(map[string]bool)
	user.Commands = make(map[string]bool)

	// Set new categories
	for _, category := range categories {
		user.Categories[category] = true

		// Add or remove commands based on category
		if len(category) > 1 && category[0] == '+' {
			catName := category[2:] // Remove "+@"
			if commands, exists := CommandCategories[catName]; exists {
				for _, cmd := range commands {
					user.Commands[cmd] = true
				}
			}
		}
	}

	return nil
}

// SetUserKeys sets key patterns for a user
func (acl *ACLManager) SetUserKeys(username string, keys []string) error {
	acl.mu.Lock()
	defer acl.mu.Unlock()

	user, exists := acl.users[username]
	if !exists {
		return fmt.Errorf("user %s not found", username)
	}

	user.Keys = keys
	return nil
}

// DeleteUser removes a user
func (acl *ACLManager) DeleteUser(username string) error {
	acl.mu.Lock()
	defer acl.mu.Unlock()

	if username == "default" {
		return fmt.Errorf("cannot delete default user")
	}

	if _, exists := acl.users[username]; !exists {
		return fmt.Errorf("user %s does not exist", username)
	}

	delete(acl.users, username)
	return nil
}

// SetUserPassword sets or changes a user's password
func (acl *ACLManager) SetUserPassword(username, password string) error {
	acl.mu.Lock()
	defer acl.mu.Unlock()

	user, exists := acl.users[username]
	if !exists {
		return fmt.Errorf("user %s does not exist", username)
	}

	user.PasswordHash = acl.hashPassword(password)

	// Update flags
	user.Flags = removeFromSlice(user.Flags, "nopass")
	if password == "" {
		user.Flags = append(user.Flags, "nopass")
	}

	return nil
}

// AuthenticateUser verifies username and password
func (acl *ACLManager) AuthenticateUser(username, password string) (*ACLUser, error) {
	acl.mu.RLock()
	defer acl.mu.RUnlock()

	if !acl.enabled {
		return acl.users["default"], nil
	}

	user, exists := acl.users[username]
	if !exists {
		if acl.logAuth {
			// Log authentication failure
		}
		return nil, fmt.Errorf("invalid username or password")
	}

	if !user.Enabled {
		return nil, fmt.Errorf("user %s is disabled", username)
	}

	// Check password
	expectedHash := user.PasswordHash
	actualHash := acl.hashPassword(password)

	// Special case: nopass users
	if contains(user.Flags, "nopass") {
		if password != "" {
			return nil, fmt.Errorf("user %s does not require a password", username)
		}
	} else {
		if expectedHash != actualHash {
			if acl.logAuth {
				// Log authentication failure
			}
			return nil, fmt.Errorf("invalid username or password")
		}
	}

	// Update last login
	user.LastLogin = time.Now()

	if acl.logAuth {
		// Log successful authentication
	}

	return user, nil
}

// CheckCommandPermission verifies if user can execute a command
func (acl *ACLManager) CheckCommandPermission(user *ACLUser, command string) error {
	if !acl.enabled || user == nil {
		return nil
	}

	command = strings.ToUpper(command)

	// Check if user is enabled
	if !user.Enabled {
		return fmt.Errorf("user %s is disabled", user.Username)
	}

	// Check explicit command permissions first
	if allowed, exists := user.Commands[command]; exists {
		if !allowed {
			return fmt.Errorf("user %s is not allowed to execute %s", user.Username, command)
		}
		return nil
	}

	// Check category permissions
	for category, commands := range CommandCategories {
		for _, cmd := range commands {
			if cmd == command {
				// Check if category is allowed
				if allowed, exists := user.Categories["+@"+category]; exists && allowed {
					return nil
				}
				// Check if category is denied
				if denied, exists := user.Categories["-@"+category]; exists && denied {
					return fmt.Errorf("user %s is not allowed to execute %s (category -%s)", user.Username, command, category)
				}
			}
		}
	}

	// Check for allcommands flag
	if contains(user.Flags, "allcommands") {
		return nil
	}

	// Default deny
	return fmt.Errorf("user %s is not allowed to execute %s", user.Username, command)
}

// CheckKeyPermission verifies if user can access a key
func (acl *ACLManager) CheckKeyPermission(user *ACLUser, key string) error {
	if !acl.enabled || user == nil {
		return nil
	}

	// Check if user is enabled
	if !user.Enabled {
		return fmt.Errorf("user %s is disabled", user.Username)
	}

	// Check allkeys flag
	if contains(user.Flags, "allkeys") {
		return nil
	}

	// Check key patterns
	for _, pattern := range user.Keys {
		if matchPattern(pattern, key) {
			return nil
		}
	}

	return fmt.Errorf("user %s is not allowed to access key %s", user.Username, key)
}

// GrantCommand grants a command to a user
func (acl *ACLManager) GrantCommand(username, command string) error {
	acl.mu.Lock()
	defer acl.mu.Unlock()

	user, exists := acl.users[username]
	if !exists {
		return fmt.Errorf("user %s does not exist", username)
	}

	command = strings.ToUpper(command)
	user.Commands[command] = true
	return nil
}

// RevokeCommand revokes a command from a user
func (acl *ACLManager) RevokeCommand(username, command string) error {
	acl.mu.Lock()
	defer acl.mu.Unlock()

	user, exists := acl.users[username]
	if !exists {
		return fmt.Errorf("user %s does not exist", username)
	}

	command = strings.ToUpper(command)
	user.Commands[command] = false
	return nil
}

// GrantCategory grants a command category to a user
func (acl *ACLManager) GrantCategory(username, category string) error {
	acl.mu.Lock()
	defer acl.mu.Unlock()

	user, exists := acl.users[username]
	if !exists {
		return fmt.Errorf("user %s does not exist", username)
	}

	// Validate category
	if _, exists := CommandCategories[category]; !exists {
		return fmt.Errorf("unknown category: %s", category)
	}

	user.Categories["+@"+category] = true

	// Also grant individual commands in the category
	for _, cmd := range CommandCategories[category] {
		user.Commands[cmd] = true
	}

	return nil
}

// RevokeCategory revokes a command category from a user
func (acl *ACLManager) RevokeCategory(username, category string) error {
	acl.mu.Lock()
	defer acl.mu.Unlock()

	user, exists := acl.users[username]
	if !exists {
		return fmt.Errorf("user %s does not exist", username)
	}

	// Validate category
	if _, exists := CommandCategories[category]; !exists {
		return fmt.Errorf("unknown category: %s", category)
	}

	user.Categories["-@"+category] = true

	// Also revoke individual commands in the category
	for _, cmd := range CommandCategories[category] {
		user.Commands[cmd] = false
	}

	return nil
}

// AddKeyPattern adds a key pattern that the user can access
func (acl *ACLManager) AddKeyPattern(username, pattern string) error {
	acl.mu.Lock()
	defer acl.mu.Unlock()

	user, exists := acl.users[username]
	if !exists {
		return fmt.Errorf("user %s does not exist", username)
	}

	user.Keys = append(user.Keys, pattern)
	// Remove allkeys flag if specific patterns are set
	user.Flags = removeFromSlice(user.Flags, "allkeys")
	return nil
}

// ListUsers returns all usernames
func (acl *ACLManager) ListUsers() []string {
	acl.mu.RLock()
	defer acl.mu.RUnlock()

	users := make([]string, 0, len(acl.users))
	for username := range acl.users {
		users = append(users, username)
	}
	return users
}

// GetUser returns user information
func (acl *ACLManager) GetUser(username string) (*ACLUser, error) {
	acl.mu.RLock()
	defer acl.mu.RUnlock()

	user, exists := acl.users[username]
	if !exists {
		return nil, fmt.Errorf("user %s does not exist", username)
	}

	// Return a copy to prevent external modification
	userCopy := *user
	return &userCopy, nil
}

// EnableACL enables the ACL system
func (acl *ACLManager) EnableACL() {
	acl.mu.Lock()
	defer acl.mu.Unlock()
	acl.enabled = true
}

// DisableACL disables the ACL system
func (acl *ACLManager) DisableACL() {
	acl.mu.Lock()
	defer acl.mu.Unlock()
	acl.enabled = false
}

// IsEnabled returns whether ACL is enabled
func (acl *ACLManager) IsEnabled() bool {
	acl.mu.RLock()
	defer acl.mu.RUnlock()
	return acl.enabled
}

// Helper functions

func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

func removeFromSlice(slice []string, item string) []string {
	result := make([]string, 0, len(slice))
	for _, s := range slice {
		if s != item {
			result = append(result, s)
		}
	}
	return result
}

// matchPattern checks if a key matches a pattern (simplified glob matching)
func matchPattern(pattern, key string) bool {
	if pattern == "*" {
		return true
	}

	// Simple prefix matching for now
	if strings.HasSuffix(pattern, "*") {
		prefix := strings.TrimSuffix(pattern, "*")
		return strings.HasPrefix(key, prefix)
	}

	// Exact match
	return pattern == key
}
