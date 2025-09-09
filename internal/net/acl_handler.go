package net

import (
	"fmt"
	"net"
	"strings"

	"multithreaded-redis/internal/protocol"
	"multithreaded-redis/internal/store"
)

// Handle AUTH command
func (s *Server) handleAUTH(c net.Conn, args protocol.Array) {
	if len(args) < 2 || len(args) > 3 {
		c.Write([]byte(protocol.Encode(protocol.Error("ERR wrong number of arguments for 'AUTH' command"))))
		return
	}

	var username, password string

	if len(args) == 2 {
		// AUTH password (default user)
		username = "default"
		password = string(args[1].(protocol.BulkString))
	} else {
		// AUTH username password
		username = string(args[1].(protocol.BulkString))
		password = string(args[2].(protocol.BulkString))
	}

	user, err := s.acl.AuthenticateUser(username, password)
	if err != nil {
		c.Write([]byte(protocol.Encode(protocol.Error(fmt.Sprintf("ERR %s", err.Error())))))
		return
	}

	s.authenticateConnection(c, user)
	c.Write([]byte(protocol.Encode(protocol.SimpleString("OK"))))
}

// Handle ACL command
func (s *Server) handleACL(c net.Conn, args protocol.Array) {
	// Check if user has admin privileges
	if err := s.checkAuth(c, "ACL"); err != nil {
		c.Write([]byte(protocol.Encode(protocol.Error(fmt.Sprintf("ERR %s", err.Error())))))
		return
	}

	if len(args) < 2 {
		c.Write([]byte(protocol.Encode(protocol.Error("ERR wrong number of arguments for 'ACL' command"))))
		return
	}

	subCmd := strings.ToUpper(string(args[1].(protocol.BulkString)))

	switch subCmd {
	case "LIST":
		s.handleACLList(c)
	case "USERS":
		s.handleACLUsers(c)
	case "GETUSER":
		s.handleACLGetUser(c, args)
	case "SETUSER":
		s.handleACLSetUser(c, args)
	case "DELUSER":
		s.handleACLDelUser(c, args)
	case "CAT":
		s.handleACLCat(c, args)
	case "WHOAMI":
		s.handleACLWhoAmI(c)
	default:
		c.Write([]byte(protocol.Encode(protocol.Error(fmt.Sprintf("ERR Unknown ACL subcommand: %s", subCmd)))))
	}
}

// ACL LIST - List all users with their configurations
func (s *Server) handleACLList(c net.Conn) {
	users := s.acl.ListUsers()
	result := make([]protocol.RESPType, 0, len(users))

	for _, username := range users {
		user, err := s.acl.GetUser(username)
		if err != nil {
			continue
		}

		// Build user configuration string
		var config strings.Builder
		config.WriteString("user ")
		config.WriteString(username)

		// Add flags
		for _, flag := range user.Flags {
			config.WriteString(" ")
			config.WriteString(flag)
		}

		// Add password info
		if user.PasswordHash == "" {
			config.WriteString(" nopass")
		} else {
			config.WriteString(" >password")
		}

		// Add command permissions
		if len(user.Categories) > 0 {
			for category, allowed := range user.Categories {
				config.WriteString(" ")
				if allowed {
					config.WriteString(category)
				} else {
					config.WriteString("-" + strings.TrimPrefix(category, "+"))
				}
			}
		}

		// Add key patterns
		if len(user.Keys) > 0 {
			for _, pattern := range user.Keys {
				config.WriteString(" ~")
				config.WriteString(pattern)
			}
		} else {
			config.WriteString(" ~*")
		}

		result = append(result, protocol.BulkString(config.String()))
	}

	c.Write([]byte(protocol.Encode(protocol.Array(result))))
}

// ACL USERS - List all usernames
func (s *Server) handleACLUsers(c net.Conn) {
	users := s.acl.ListUsers()
	result := make([]protocol.RESPType, 0, len(users))

	for _, username := range users {
		result = append(result, protocol.BulkString(username))
	}

	c.Write([]byte(protocol.Encode(protocol.Array(result))))
}

// ACL GETUSER - Get user configuration
func (s *Server) handleACLGetUser(c net.Conn, args protocol.Array) {
	if len(args) != 3 {
		c.Write([]byte(protocol.Encode(protocol.Error("ERR wrong number of arguments for 'ACL GETUSER' command"))))
		return
	}

	username := string(args[2].(protocol.BulkString))
	user, err := s.acl.GetUser(username)
	if err != nil {
		c.Write([]byte(protocol.Encode(protocol.BulkString(nil))))
		return
	}

	result := make([]protocol.RESPType, 0)

	// Flags
	result = append(result, protocol.BulkString("flags"))
	flagsArray := make([]protocol.RESPType, 0, len(user.Flags))
	for _, flag := range user.Flags {
		flagsArray = append(flagsArray, protocol.BulkString(flag))
	}
	result = append(result, protocol.Array(flagsArray))

	// Passwords
	result = append(result, protocol.BulkString("passwords"))
	if user.PasswordHash == "" {
		result = append(result, protocol.Array([]protocol.RESPType{}))
	} else {
		result = append(result, protocol.Array([]protocol.RESPType{protocol.BulkString("password")}))
	}

	// Commands
	result = append(result, protocol.BulkString("commands"))
	commands := make([]protocol.RESPType, 0)
	for category, allowed := range user.Categories {
		if allowed {
			commands = append(commands, protocol.BulkString(category))
		}
	}
	result = append(result, protocol.Array(commands))

	// Keys
	result = append(result, protocol.BulkString("keys"))
	keysArray := make([]protocol.RESPType, 0, len(user.Keys))
	for _, pattern := range user.Keys {
		keysArray = append(keysArray, protocol.BulkString(pattern))
	}
	if len(keysArray) == 0 {
		keysArray = append(keysArray, protocol.BulkString("*"))
	}
	result = append(result, protocol.Array(keysArray))

	c.Write([]byte(protocol.Encode(protocol.Array(result))))
}

// ACL SETUSER - Create or modify user
func (s *Server) handleACLSetUser(c net.Conn, args protocol.Array) {
	if len(args) < 3 {
		c.Write([]byte(protocol.Encode(protocol.Error("ERR wrong number of arguments for 'ACL SETUSER' command"))))
		return
	}

	username := string(args[2].(protocol.BulkString))

	// Create user if doesn't exist
	_, err := s.acl.GetUser(username)
	if err != nil {
		err = s.acl.CreateUser(username, "")
		if err != nil {
			c.Write([]byte(protocol.Encode(protocol.Error(fmt.Sprintf("ERR %s", err.Error())))))
			return
		}
	}

	// Process user modifications
	for i := 3; i < len(args); i++ {
		rule := string(args[i].(protocol.BulkString))

		if strings.HasPrefix(rule, ">") {
			// Set password
			password := strings.TrimPrefix(rule, ">")
			err = s.acl.SetUserPassword(username, password)
		} else if strings.HasPrefix(rule, "+@") {
			// Grant category
			category := strings.TrimPrefix(rule, "+@")
			err = s.acl.GrantCategory(username, category)
		} else if strings.HasPrefix(rule, "-@") {
			// Revoke category
			category := strings.TrimPrefix(rule, "-@")
			err = s.acl.RevokeCategory(username, category)
		} else if strings.HasPrefix(rule, "+") {
			// Grant command
			command := strings.TrimPrefix(rule, "+")
			err = s.acl.GrantCommand(username, command)
		} else if strings.HasPrefix(rule, "-") {
			// Revoke command
			command := strings.TrimPrefix(rule, "-")
			err = s.acl.RevokeCommand(username, command)
		} else if strings.HasPrefix(rule, "~") {
			// Add key pattern
			pattern := strings.TrimPrefix(rule, "~")
			err = s.acl.AddKeyPattern(username, pattern)
		}

		if err != nil {
			c.Write([]byte(protocol.Encode(protocol.Error(fmt.Sprintf("ERR %s", err.Error())))))
			return
		}
	}

	c.Write([]byte(protocol.Encode(protocol.SimpleString("OK"))))
}

// ACL DELUSER - Delete user
func (s *Server) handleACLDelUser(c net.Conn, args protocol.Array) {
	if len(args) < 3 {
		c.Write([]byte(protocol.Encode(protocol.Error("ERR wrong number of arguments for 'ACL DELUSER' command"))))
		return
	}

	deleted := 0
	for i := 2; i < len(args); i++ {
		username := string(args[i].(protocol.BulkString))
		err := s.acl.DeleteUser(username)
		if err == nil {
			deleted++
		}
	}

	c.Write([]byte(protocol.Encode(protocol.Integer(deleted))))
}

// ACL CAT - List command categories
func (s *Server) handleACLCat(c net.Conn, args protocol.Array) {
	if len(args) == 2 {
		// List all categories
		categories := make([]protocol.RESPType, 0)
		for category := range store.CommandCategories {
			categories = append(categories, protocol.BulkString(category))
		}
		c.Write([]byte(protocol.Encode(protocol.Array(categories))))
	} else if len(args) == 3 {
		// List commands in category
		commands, exists := store.CommandCategories[string(args[2].(protocol.BulkString))]
		if !exists {
			c.Write([]byte(protocol.Encode(protocol.Error("ERR Unknown category"))))
			return
		}

		result := make([]protocol.RESPType, 0, len(commands))
		for _, cmd := range commands {
			result = append(result, protocol.BulkString(cmd))
		}
		c.Write([]byte(protocol.Encode(protocol.Array(result))))
	} else {
		c.Write([]byte(protocol.Encode(protocol.Error("ERR wrong number of arguments for 'ACL CAT' command"))))
	}
}

// ACL WHOAMI - Get current user
func (s *Server) handleACLWhoAmI(c net.Conn) {
	s.mu.RLock()
	state, exists := s.connStates[c]
	s.mu.RUnlock()

	if !exists {
		c.Write([]byte(protocol.Encode(protocol.BulkString("unknown"))))
		return
	}

	state.mu.RLock()
	username := state.user.Username
	state.mu.RUnlock()

	c.Write([]byte(protocol.Encode(protocol.BulkString(username))))
}
