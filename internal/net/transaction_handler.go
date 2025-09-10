package net

import (
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
	"time"

	"multithreaded-redis/internal/protocol"
)

// isInTransaction checks if the connection is currently in a transaction
func (s *Server) isInTransaction(c net.Conn) bool {
	s.mu.RLock()
	state, exists := s.connStates[c]
	s.mu.RUnlock()

	if !exists {
		return false
	}

	state.txMu.Lock()
	inTx := state.inTransaction
	state.txMu.Unlock()

	return inTx
}

// queueTransactionCommand adds a command to the transaction queue
func (s *Server) queueTransactionCommand(c net.Conn, args protocol.Array) {
	s.mu.RLock()
	state, exists := s.connStates[c]
	s.mu.RUnlock()

	if !exists {
		c.Write([]byte(protocol.Encode(protocol.Error("ERR connection state not found"))))
		return
	}

	state.txMu.Lock()
	defer state.txMu.Unlock()

	if !state.inTransaction {
		c.Write([]byte(protocol.Encode(protocol.Error("ERR command requires MULTI"))))
		return
	}

	// Queue the command
	state.txQueue = append(state.txQueue, args)
	c.Write([]byte(protocol.Encode(protocol.SimpleString("QUEUED"))))
}

// handleMulti starts a transaction
func (s *Server) handleMulti(c net.Conn, args protocol.Array) {
	if len(args) != 1 {
		c.Write([]byte(protocol.Encode(protocol.Error("ERR wrong number of arguments for 'MULTI' command"))))
		return
	}

	s.mu.RLock()
	state, exists := s.connStates[c]
	s.mu.RUnlock()

	if !exists {
		// Create connection state if it doesn't exist
		s.mu.Lock()
		state = &ConnectionState{
			channels: make(map[string]bool),
		}
		s.connStates[c] = state
		s.mu.Unlock()
	}

	state.txMu.Lock()
	defer state.txMu.Unlock()

	if state.inTransaction {
		c.Write([]byte(protocol.Encode(protocol.Error("ERR MULTI calls can not be nested"))))
		return
	}

	state.inTransaction = true
	state.txQueue = make([]protocol.Array, 0)
	c.Write([]byte(protocol.Encode(protocol.SimpleString("OK"))))

	log.Printf("Started transaction for connection")
}

// handleExec executes all queued commands in a transaction
func (s *Server) handleExec(c net.Conn, args protocol.Array) {
	if len(args) != 1 {
		c.Write([]byte(protocol.Encode(protocol.Error("ERR wrong number of arguments for 'EXEC' command"))))
		return
	}

	s.mu.RLock()
	state, exists := s.connStates[c]
	s.mu.RUnlock()

	if !exists {
		c.Write([]byte(protocol.Encode(protocol.Error("ERR EXEC without MULTI"))))
		return
	}

	state.txMu.Lock()
	defer state.txMu.Unlock()

	if !state.inTransaction {
		c.Write([]byte(protocol.Encode(protocol.Error("ERR EXEC without MULTI"))))
		return
	}

	// Execute all queued commands atomically
	results := make([]interface{}, len(state.txQueue))

	log.Printf("Executing transaction with %d commands", len(state.txQueue))

	// Execute each command and collect results
	for i, cmdArgs := range state.txQueue {
		if len(cmdArgs) == 0 {
			results[i] = protocol.Error("ERR empty command")
			continue
		}

		cmdName := strings.ToUpper(string(cmdArgs[0].(protocol.BulkString)))
		result := s.executeTransactionCommand(cmdName, cmdArgs)
		results[i] = result
	}

	// Clear transaction state
	state.inTransaction = false
	state.txQueue = nil

	// Convert results to protocol.Array
	protocolResults := make(protocol.Array, len(results))
	for i, result := range results {
		protocolResults[i] = result
	}

	// Send results as array
	c.Write([]byte(protocol.Encode(protocolResults)))
}

// handleDiscard discards all queued commands in a transaction
func (s *Server) handleDiscard(c net.Conn, args protocol.Array) {
	if len(args) != 1 {
		c.Write([]byte(protocol.Encode(protocol.Error("ERR wrong number of arguments for 'DISCARD' command"))))
		return
	}

	s.mu.RLock()
	state, exists := s.connStates[c]
	s.mu.RUnlock()

	if !exists {
		c.Write([]byte(protocol.Encode(protocol.Error("ERR DISCARD without MULTI"))))
		return
	}

	state.txMu.Lock()
	defer state.txMu.Unlock()

	if !state.inTransaction {
		c.Write([]byte(protocol.Encode(protocol.Error("ERR DISCARD without MULTI"))))
		return
	}

	// Clear transaction state
	state.inTransaction = false
	state.txQueue = nil

	c.Write([]byte(protocol.Encode(protocol.SimpleString("OK"))))
	log.Printf("Discarded transaction for connection")
}

// executeTransactionCommand executes a single command within a transaction context
func (s *Server) executeTransactionCommand(cmdName string, args protocol.Array) interface{} {
	// Execute command directly on store operations
	switch cmdName {
	case "SET":
		return s.executeTransactionSET(args)
	case "GET":
		return s.executeTransactionGET(args)
	case "DEL":
		return s.executeTransactionDEL(args)
	case "EXISTS":
		return s.executeTransactionEXISTS(args)
	case "EXPIRE":
		return s.executeTransactionEXPIRE(args)
	case "TTL":
		return s.executeTransactionTTL(args)
	case "SADD":
		return s.executeTransactionSADD(args)
	case "SREM":
		return s.executeTransactionSREM(args)
	case "SMEMBERS":
		return s.executeTransactionSMEMBERS(args)
	case "SISMEMBER":
		return s.executeTransactionSISMEMBER(args)
	case "SCARD":
		return s.executeTransactionSCARD(args)
	case "SPOP":
		return s.executeTransactionSPOP(args)
	case "HSET":
		return s.executeTransactionHSET(args)
	case "HGET":
		return s.executeTransactionHGET(args)
	case "HDEL":
		return s.executeTransactionHDEL(args)
	case "HGETALL":
		return s.executeTransactionHGETALL(args)
	case "LPUSH":
		return s.executeTransactionLPUSH(args)
	case "RPUSH":
		return s.executeTransactionRPUSH(args)
	case "LPOP":
		return s.executeTransactionLPOP(args)
	case "RPOP":
		return s.executeTransactionRPOP(args)
	case "LLEN":
		return s.executeTransactionLLEN(args)
	case "LRANGE":
		return s.executeTransactionLRANGE(args)
	case "ZADD":
		return s.executeTransactionZADD(args)
	case "ZSCORE":
		return s.executeTransactionZSCORE(args)
	case "ZCARD":
		return s.executeTransactionZCARD(args)
	case "ZRANK":
		return s.executeTransactionZRANK(args)
	case "ZRANGE":
		return s.executeTransactionZRANGE(args)
	case "BFADD":
		return s.executeTransactionBFADD(args)
	case "BFEXISTS":
		return s.executeTransactionBFEXISTS(args)
	default:
		return protocol.Error(fmt.Sprintf("ERR unknown command '%s'", cmdName))
	}
}

// Transaction command implementations - using SharedStore Execute method
func (s *Server) executeTransactionSET(args protocol.Array) interface{} {
	if len(args) < 3 {
		return protocol.Error("ERR wrong number of arguments for 'SET' command")
	}

	key := string(args[1].(protocol.BulkString))
	value := string(args[2].(protocol.BulkString))

	// Handle optional EX parameter
	var expire time.Duration = 0
	if len(args) >= 5 {
		if strings.ToUpper(string(args[3].(protocol.BulkString))) == "EX" {
			if seconds, err := strconv.Atoi(string(args[4].(protocol.BulkString))); err == nil {
				expire = time.Duration(seconds) * time.Second
			}
		}
	}

	result := s.shards.Execute("SET", key, value, expire.String())
	if err, isErr := result.(error); isErr {
		return protocol.Error(fmt.Sprintf("ERR %s", err.Error()))
	}
	return protocol.SimpleString("OK")
}

func (s *Server) executeTransactionGET(args protocol.Array) interface{} {
	if len(args) != 2 {
		return protocol.Error("ERR wrong number of arguments for 'GET' command")
	}

	key := string(args[1].(protocol.BulkString))
	result := s.shards.Execute("GET", key)

	if result == nil {
		return protocol.BulkString(nil) // null bulk string
	}
	if bytes, ok := result.([]byte); ok {
		return protocol.BulkString(bytes)
	}
	return protocol.BulkString(nil)
}

func (s *Server) executeTransactionDEL(args protocol.Array) interface{} {
	if len(args) < 2 {
		return protocol.Error("ERR wrong number of arguments for 'DEL' command")
	}

	count := 0
	for i := 1; i < len(args); i++ {
		key := string(args[i].(protocol.BulkString))
		result := s.shards.Execute("DEL", key)
		if deleted, ok := result.(bool); ok && deleted {
			count++
		}
	}
	return protocol.Integer(count)
}

func (s *Server) executeTransactionEXISTS(args protocol.Array) interface{} {
	if len(args) < 2 {
		return protocol.Error("ERR wrong number of arguments for 'EXISTS' command")
	}

	count := 0
	for i := 1; i < len(args); i++ {
		key := string(args[i].(protocol.BulkString))
		result := s.shards.Execute("GET", key)
		if result != nil {
			count++
		}
	}
	return protocol.Integer(count)
}

func (s *Server) executeTransactionEXPIRE(args protocol.Array) interface{} {
	if len(args) != 3 {
		return protocol.Error("ERR wrong number of arguments for 'EXPIRE' command")
	}

	key := string(args[1].(protocol.BulkString))
	seconds := string(args[2].(protocol.BulkString))

	result := s.shards.Execute("EXPIRE", key, seconds)
	if _, isErr := result.(error); isErr {
		return protocol.Integer(0)
	}
	return protocol.Integer(1)
}

func (s *Server) executeTransactionTTL(args protocol.Array) interface{} {
	if len(args) != 2 {
		return protocol.Error("ERR wrong number of arguments for 'TTL' command")
	}

	key := string(args[1].(protocol.BulkString))
	result := s.shards.Execute("TTL", key)
	if ttl, ok := result.(int64); ok {
		return protocol.Integer(ttl)
	}
	return protocol.Integer(-2)
}

// Set operations for transactions
func (s *Server) executeTransactionSADD(args protocol.Array) interface{} {
	if len(args) < 3 {
		return protocol.Error("ERR wrong number of arguments for 'SADD' command")
	}

	key := string(args[1].(protocol.BulkString))
	members := make([]string, len(args)-2)
	for i := 2; i < len(args); i++ {
		members[i-2] = string(args[i].(protocol.BulkString))
	}

	result := s.shards.Execute("SADD", key, members...)
	if count, ok := result.(int); ok {
		return protocol.Integer(count)
	}
	return protocol.Integer(0)
}

func (s *Server) executeTransactionSREM(args protocol.Array) interface{} {
	if len(args) < 3 {
		return protocol.Error("ERR wrong number of arguments for 'SREM' command")
	}

	key := string(args[1].(protocol.BulkString))
	members := make([]string, len(args)-2)
	for i := 2; i < len(args); i++ {
		members[i-2] = string(args[i].(protocol.BulkString))
	}

	result := s.shards.Execute("SREM", key, members...)
	if count, ok := result.(int); ok {
		return protocol.Integer(count)
	}
	return protocol.Integer(0)
}

func (s *Server) executeTransactionSMEMBERS(args protocol.Array) interface{} {
	if len(args) != 2 {
		return protocol.Error("ERR wrong number of arguments for 'SMEMBERS' command")
	}

	key := string(args[1].(protocol.BulkString))
	result := s.shards.Execute("SMEMBERS", key)
	if members, ok := result.([]string); ok {
		protocolArray := make(protocol.Array, len(members))
		for i, member := range members {
			protocolArray[i] = protocol.BulkString(member)
		}
		return protocolArray
	}
	return protocol.Array{}
}

func (s *Server) executeTransactionSISMEMBER(args protocol.Array) interface{} {
	if len(args) != 3 {
		return protocol.Error("ERR wrong number of arguments for 'SISMEMBER' command")
	}

	key := string(args[1].(protocol.BulkString))
	member := string(args[2].(protocol.BulkString))

	result := s.shards.Execute("SISMEMBER", key, member)
	if isMember, ok := result.(bool); ok && isMember {
		return protocol.Integer(1)
	}
	return protocol.Integer(0)
}

func (s *Server) executeTransactionSCARD(args protocol.Array) interface{} {
	if len(args) != 2 {
		return protocol.Error("ERR wrong number of arguments for 'SCARD' command")
	}

	key := string(args[1].(protocol.BulkString))
	result := s.shards.Execute("SCARD", key)
	if count, ok := result.(int); ok {
		return protocol.Integer(count)
	}
	return protocol.Integer(0)
}

func (s *Server) executeTransactionSPOP(args protocol.Array) interface{} {
	if len(args) < 2 || len(args) > 3 {
		return protocol.Error("ERR wrong number of arguments for 'SPOP' command")
	}

	key := string(args[1].(protocol.BulkString))
	count := "1"
	if len(args) == 3 {
		count = string(args[2].(protocol.BulkString))
	}

	result := s.shards.Execute("SPOP", key, count)
	if members, ok := result.([]string); ok {
		if len(members) == 0 {
			return protocol.BulkString(nil)
		}
		if len(args) == 2 { // Single member
			return protocol.BulkString(members[0])
		}
		// Multiple members
		protocolArray := make(protocol.Array, len(members))
		for i, member := range members {
			protocolArray[i] = protocol.BulkString(member)
		}
		return protocolArray
	}
	return protocol.BulkString(nil)
}

// Hash operations for transactions
func (s *Server) executeTransactionHSET(args protocol.Array) interface{} {
	if len(args) < 4 || len(args)%2 != 0 {
		return protocol.Error("ERR wrong number of arguments for 'HSET' command")
	}

	key := string(args[1].(protocol.BulkString))
	count := 0
	for i := 2; i < len(args); i += 2 {
		field := string(args[i].(protocol.BulkString))
		value := string(args[i+1].(protocol.BulkString))
		result := s.shards.Execute("HSET", key, field, value)
		if added, ok := result.(int); ok {
			count += added
		}
	}
	return protocol.Integer(count)
}

func (s *Server) executeTransactionHGET(args protocol.Array) interface{} {
	if len(args) != 3 {
		return protocol.Error("ERR wrong number of arguments for 'HGET' command")
	}

	key := string(args[1].(protocol.BulkString))
	field := string(args[2].(protocol.BulkString))

	result := s.shards.Execute("HGET", key, field)
	if value, ok := result.(string); ok {
		return protocol.BulkString(value)
	}
	return protocol.BulkString(nil)
}

func (s *Server) executeTransactionHDEL(args protocol.Array) interface{} {
	if len(args) < 3 {
		return protocol.Error("ERR wrong number of arguments for 'HDEL' command")
	}

	key := string(args[1].(protocol.BulkString))
	fields := make([]string, len(args)-2)
	for i := 2; i < len(args); i++ {
		fields[i-2] = string(args[i].(protocol.BulkString))
	}

	result := s.shards.Execute("HDEL", key, fields...)
	if count, ok := result.(int); ok {
		return protocol.Integer(count)
	}
	return protocol.Integer(0)
}

func (s *Server) executeTransactionHGETALL(args protocol.Array) interface{} {
	if len(args) != 2 {
		return protocol.Error("ERR wrong number of arguments for 'HGETALL' command")
	}

	key := string(args[1].(protocol.BulkString))
	result := s.shards.Execute("HGETALL", key)

	if hash, ok := result.(map[string]string); ok {
		protocolArray := make(protocol.Array, len(hash)*2)
		i := 0
		for field, value := range hash {
			protocolArray[i] = protocol.BulkString(field)
			protocolArray[i+1] = protocol.BulkString(value)
			i += 2
		}
		return protocolArray
	}
	return protocol.Array{}
}

// List operations for transactions
func (s *Server) executeTransactionLPUSH(args protocol.Array) interface{} {
	if len(args) < 3 {
		return protocol.Error("ERR wrong number of arguments for 'LPUSH' command")
	}

	key := string(args[1].(protocol.BulkString))
	values := make([]string, len(args)-2)
	for i := 2; i < len(args); i++ {
		values[i-2] = string(args[i].(protocol.BulkString))
	}

	result := s.shards.Execute("LPUSH", key, values...)
	if length, ok := result.(int); ok {
		return protocol.Integer(length)
	}
	return protocol.Integer(0)
}

func (s *Server) executeTransactionRPUSH(args protocol.Array) interface{} {
	if len(args) < 3 {
		return protocol.Error("ERR wrong number of arguments for 'RPUSH' command")
	}

	key := string(args[1].(protocol.BulkString))
	values := make([]string, len(args)-2)
	for i := 2; i < len(args); i++ {
		values[i-2] = string(args[i].(protocol.BulkString))
	}

	result := s.shards.Execute("RPUSH", key, values...)
	if length, ok := result.(int); ok {
		return protocol.Integer(length)
	}
	return protocol.Integer(0)
}

func (s *Server) executeTransactionLPOP(args protocol.Array) interface{} {
	if len(args) < 2 || len(args) > 3 {
		return protocol.Error("ERR wrong number of arguments for 'LPOP' command")
	}

	key := string(args[1].(protocol.BulkString))
	count := "1"
	if len(args) == 3 {
		count = string(args[2].(protocol.BulkString))
	}

	result := s.shards.Execute("LPOP", key, count)
	if values, ok := result.([]string); ok {
		if len(values) == 0 {
			return protocol.BulkString(nil)
		}
		if len(args) == 2 { // Single value
			return protocol.BulkString(values[0])
		}
		// Multiple values
		protocolArray := make(protocol.Array, len(values))
		for i, value := range values {
			protocolArray[i] = protocol.BulkString(value)
		}
		return protocolArray
	}
	return protocol.BulkString(nil)
}

func (s *Server) executeTransactionRPOP(args protocol.Array) interface{} {
	if len(args) < 2 || len(args) > 3 {
		return protocol.Error("ERR wrong number of arguments for 'RPOP' command")
	}

	key := string(args[1].(protocol.BulkString))
	count := "1"
	if len(args) == 3 {
		count = string(args[2].(protocol.BulkString))
	}

	result := s.shards.Execute("RPOP", key, count)
	if values, ok := result.([]string); ok {
		if len(values) == 0 {
			return protocol.BulkString(nil)
		}
		if len(args) == 2 { // Single value
			return protocol.BulkString(values[0])
		}
		// Multiple values
		protocolArray := make(protocol.Array, len(values))
		for i, value := range values {
			protocolArray[i] = protocol.BulkString(value)
		}
		return protocolArray
	}
	return protocol.BulkString(nil)
}

func (s *Server) executeTransactionLLEN(args protocol.Array) interface{} {
	if len(args) != 2 {
		return protocol.Error("ERR wrong number of arguments for 'LLEN' command")
	}

	key := string(args[1].(protocol.BulkString))
	result := s.shards.Execute("LLEN", key)
	if length, ok := result.(int); ok {
		return protocol.Integer(length)
	}
	return protocol.Integer(0)
}

func (s *Server) executeTransactionLRANGE(args protocol.Array) interface{} {
	if len(args) != 4 {
		return protocol.Error("ERR wrong number of arguments for 'LRANGE' command")
	}

	key := string(args[1].(protocol.BulkString))
	start := string(args[2].(protocol.BulkString))
	stop := string(args[3].(protocol.BulkString))

	result := s.shards.Execute("LRANGE", key, start, stop)
	if values, ok := result.([]string); ok {
		protocolArray := make(protocol.Array, len(values))
		for i, value := range values {
			protocolArray[i] = protocol.BulkString(value)
		}
		return protocolArray
	}
	return protocol.Array{}
}

// Sorted Set operations for transactions
func (s *Server) executeTransactionZADD(args protocol.Array) interface{} {
	if len(args) < 4 || len(args)%2 != 0 {
		return protocol.Error("ERR wrong number of arguments for 'ZADD' command")
	}

	key := string(args[1].(protocol.BulkString))
	count := 0
	for i := 2; i < len(args); i += 2 {
		score := string(args[i].(protocol.BulkString))
		member := string(args[i+1].(protocol.BulkString))
		result := s.shards.Execute("ZADD", key, score, member)
		if added, ok := result.(int); ok {
			count += added
		}
	}
	return protocol.Integer(count)
}

func (s *Server) executeTransactionZSCORE(args protocol.Array) interface{} {
	if len(args) != 3 {
		return protocol.Error("ERR wrong number of arguments for 'ZSCORE' command")
	}

	key := string(args[1].(protocol.BulkString))
	member := string(args[2].(protocol.BulkString))

	result := s.shards.Execute("ZSCORE", key, member)
	if score, ok := result.(float64); ok {
		return protocol.BulkString(fmt.Sprintf("%f", score))
	}
	return protocol.BulkString(nil)
}

func (s *Server) executeTransactionZCARD(args protocol.Array) interface{} {
	if len(args) != 2 {
		return protocol.Error("ERR wrong number of arguments for 'ZCARD' command")
	}

	key := string(args[1].(protocol.BulkString))
	result := s.shards.Execute("ZCARD", key)
	if count, ok := result.(int); ok {
		return protocol.Integer(count)
	}
	return protocol.Integer(0)
}

func (s *Server) executeTransactionZRANK(args protocol.Array) interface{} {
	if len(args) != 3 {
		return protocol.Error("ERR wrong number of arguments for 'ZRANK' command")
	}

	key := string(args[1].(protocol.BulkString))
	member := string(args[2].(protocol.BulkString))

	result := s.shards.Execute("ZRANK", key, member)
	if rank, ok := result.(int); ok {
		return protocol.Integer(rank)
	}
	return protocol.BulkString(nil)
}

func (s *Server) executeTransactionZRANGE(args protocol.Array) interface{} {
	if len(args) < 4 {
		return protocol.Error("ERR wrong number of arguments for 'ZRANGE' command")
	}

	key := string(args[1].(protocol.BulkString))
	start := string(args[2].(protocol.BulkString))
	stop := string(args[3].(protocol.BulkString))

	executeArgs := []string{start, stop}
	if len(args) >= 5 && strings.ToUpper(string(args[4].(protocol.BulkString))) == "WITHSCORES" {
		executeArgs = append(executeArgs, "WITHSCORES")
	}

	result := s.shards.Execute("ZRANGE", key, executeArgs...)
	if members, ok := result.([]string); ok {
		protocolArray := make(protocol.Array, len(members))
		for i, member := range members {
			protocolArray[i] = protocol.BulkString(member)
		}
		return protocolArray
	}
	return protocol.Array{}
}

// Bloom Filter operations for transactions
func (s *Server) executeTransactionBFADD(args protocol.Array) interface{} {
	if len(args) != 3 {
		return protocol.Error("ERR wrong number of arguments for 'BFADD' command")
	}

	key := string(args[1].(protocol.BulkString))
	item := string(args[2].(protocol.BulkString))

	result := s.shards.Execute("BFADD", key, item)
	if added, ok := result.(bool); ok && added {
		return protocol.Integer(1)
	}
	return protocol.Integer(0)
}

func (s *Server) executeTransactionBFEXISTS(args protocol.Array) interface{} {
	if len(args) != 3 {
		return protocol.Error("ERR wrong number of arguments for 'BFEXISTS' command")
	}

	key := string(args[1].(protocol.BulkString))
	item := string(args[2].(protocol.BulkString))

	result := s.shards.Execute("BFEXISTS", key, item)
	if exists, ok := result.(bool); ok && exists {
		return protocol.Integer(1)
	}
	return protocol.Integer(0)
}
