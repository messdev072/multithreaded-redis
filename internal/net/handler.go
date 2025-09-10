package net

import (
	"context"
	"fmt"
	"log"
	"multithreaded-redis/internal/protocol"
	"multithreaded-redis/internal/store"
	"net"
	"strconv"
	"time"
)

// Handle SET command with optional expiration
func (s *Server) handleSET(c net.Conn, args protocol.Array) {
	if len(args) < 3 {
		c.Write([]byte(protocol.Encode(protocol.Error("ERR wrong number of arguments for 'SET' command"))))
		return
	}

	key, _ := args[1].(protocol.BulkString)

	// Check authentication and permissions
	if err := s.checkAuth(c, "SET", string(key)); err != nil {
		c.Write([]byte(protocol.Encode(protocol.Error(fmt.Sprintf("ERR %s", err.Error())))))
		return
	}

	val, _ := args[2].(protocol.BulkString)

	expire := time.Duration(0)

	//Optional EX argument
	if len(args) == 5 {
		opt, _ := args[3].(protocol.BulkString)
		if string(opt) == "EX" {
			secs, err := strconv.Atoi(string(args[4].(protocol.BulkString)))
			if err != nil {
				c.Write([]byte(protocol.Encode(protocol.Error("ERR invalid expire time in 'SET' command"))))
				return
			}
			expire = time.Duration(secs) * time.Second
		}
	}

	s.shards.Set(string(key), []byte(val), expire)
	c.Write([]byte(protocol.Encode(protocol.SimpleString("OK"))))
}

// Handle GET command
func (s *Server) handleGET(c net.Conn, args protocol.Array) {
	if len(args) != 2 {
		c.Write([]byte(protocol.Encode(protocol.Error("ERR wrong number of arguments for 'GET' command"))))
		return
	}
	key, _ := args[1].(protocol.BulkString)

	// Check authentication and permissions
	if err := s.checkAuth(c, "GET", string(key)); err != nil {
		c.Write([]byte(protocol.Encode(protocol.Error(fmt.Sprintf("ERR %s", err.Error())))))
		return
	}

	val, ok := s.shards.Get(string(key))
	if !ok {
		c.Write([]byte(protocol.Encode(protocol.BulkString(nil))))
		return
	}
	c.Write([]byte(protocol.Encode(protocol.BulkString(val))))
}

// Handle EXISTS command
func (s *Server) handleExists(c net.Conn, args protocol.Array) {
	if len(args) < 2 {
		c.Write([]byte(protocol.Encode(protocol.Error("ERR wrong number of arguments for 'EXISTS' command"))))
		return
	}

	count := 0
	for i := 1; i < len(args); i++ {
		key, ok := args[i].(protocol.BulkString)
		if !ok {
			continue
		}

		// Check authentication and permissions
		if err := s.checkAuth(c, "EXISTS", string(key)); err != nil {
			c.Write([]byte(protocol.Encode(protocol.Error(fmt.Sprintf("ERR %s", err.Error())))))
			return
		}

		_, exists := s.shards.Get(string(key))
		if exists {
			count++
		}
	}

	c.Write([]byte(protocol.Encode(protocol.Integer(count))))
}

// Handle DEL command
func (s *Server) handleDel(c net.Conn, args protocol.Array) {
	if len(args) < 2 {
		c.Write([]byte(protocol.Encode(protocol.Error("ERR wrong number of arguments for 'DEL' command"))))
		return
	}

	// Check authentication and permissions for all keys
	for i := 1; i < len(args); i++ {
		key, ok := args[i].(protocol.BulkString)
		if !ok {
			continue
		}
		if err := s.checkAuth(c, "DEL", string(key)); err != nil {
			c.Write([]byte(protocol.Encode(protocol.Error(fmt.Sprintf("ERR %s", err.Error())))))
			return
		}
	}

	deleted := 0
	for i := 1; i < len(args); i++ {
		key, ok := args[i].(protocol.BulkString)
		if !ok {
			continue
		}
		res := s.shards.Execute("DEL", string(key))
		if b, ok := res.(bool); ok && b {
			deleted++
		}
	}
	c.Write([]byte(protocol.Encode(protocol.Integer(deleted))))
}

// Handle TTL command
func (s *Server) handleTTL(c net.Conn, args protocol.Array) {
	if len(args) != 2 {
		c.Write([]byte(protocol.Encode(protocol.Error("ERR wrong number of arguments for 'TTL' command"))))
		return
	}
	key, _ := args[1].(protocol.BulkString)

	// Check authentication and permissions
	if err := s.checkAuth(c, "TTL", string(key)); err != nil {
		c.Write([]byte(protocol.Encode(protocol.Error(fmt.Sprintf("ERR %s", err.Error())))))
		return
	}

	res := s.shards.Execute("TTL", string(key))
	if ttl, ok := res.(int64); ok {
		c.Write([]byte(protocol.Encode(protocol.Integer(ttl))))
	} else {
		c.Write([]byte(protocol.Encode(protocol.Integer(-2))))
	}
}
func (s *Server) handleSAdd(c net.Conn, args protocol.Array) {
	if len(args) < 3 {
		c.Write([]byte(protocol.Encode(protocol.Error("ERR wrong number of arguments for 'SADD' command"))))
		return
	}
	key := string(args[1].(protocol.BulkString))

	// Check authentication and permissions
	if err := s.checkAuth(c, "SADD", key); err != nil {
		c.Write([]byte(protocol.Encode(protocol.Error(fmt.Sprintf("ERR %s", err.Error())))))
		return
	}

	members := []string{}
	for i := 2; i < len(args); i++ {
		members = append(members, string(args[i].(protocol.BulkString)))
	}
	res := s.shards.Execute("SADD", key, members...)
	if added, ok := res.(int); ok {
		c.Write([]byte(protocol.Encode(protocol.Integer(added))))
	} else {
		c.Write([]byte(protocol.Encode(protocol.Integer(0))))
	}
}

func (s *Server) handleSRem(c net.Conn, args protocol.Array) {
	if len(args) < 3 {
		c.Write([]byte(protocol.Encode(protocol.Error("ERR wrong number of arguments for 'SREM' command"))))
		return
	}
	key := string(args[1].(protocol.BulkString))

	// Check authentication and permissions
	if err := s.checkAuth(c, "SREM", key); err != nil {
		c.Write([]byte(protocol.Encode(protocol.Error(fmt.Sprintf("ERR %s", err.Error())))))
		return
	}

	members := []string{}
	for i := 2; i < len(args); i++ {
		members = append(members, string(args[i].(protocol.BulkString)))
	}
	res := s.shards.Execute("SREM", key, members...)
	if removed, ok := res.(int); ok {
		c.Write([]byte(protocol.Encode(protocol.Integer(removed))))
	} else {
		c.Write([]byte(protocol.Encode(protocol.Integer(0))))
	}
}

func (s *Server) handleSMembers(c net.Conn, args protocol.Array) {
	if len(args) != 2 {
		c.Write([]byte(protocol.Encode(protocol.Error("ERR wrong number of arguments for 'SMEMBERS' command"))))
		return
	}
	key := string(args[1].(protocol.BulkString))

	// Check authentication and permissions
	if err := s.checkAuth(c, "SMEMBERS", key); err != nil {
		c.Write([]byte(protocol.Encode(protocol.Error(fmt.Sprintf("ERR %s", err.Error())))))
		return
	}

	res := s.shards.Execute("SMEMBERS", key)
	members, _ := res.([]string)
	arr := make([]protocol.RESPType, 0, len(members))
	for _, m := range members {
		arr = append(arr, protocol.BulkString(m))
	}
	c.Write([]byte(protocol.Encode(protocol.Array(arr))))
}

func (s *Server) handleSCard(c net.Conn, args protocol.Array) {
	if len(args) != 2 {
		c.Write([]byte(protocol.Encode(protocol.Error("ERR wrong number of arguments for 'SCARD' command"))))
		return
	}
	key := string(args[1].(protocol.BulkString))

	// Check authentication and permissions
	if err := s.checkAuth(c, "SCARD", key); err != nil {
		c.Write([]byte(protocol.Encode(protocol.Error(fmt.Sprintf("ERR %s", err.Error())))))
		return
	}

	res := s.shards.Execute("SCARD", key)
	if card, ok := res.(int); ok {
		c.Write([]byte(protocol.Encode(protocol.Integer(card))))
	} else {
		c.Write([]byte(protocol.Encode(protocol.Integer(0))))
	}
}

func (s *Server) handleSIsMember(c net.Conn, args protocol.Array) {
	if len(args) != 3 {
		c.Write([]byte(protocol.Encode(protocol.Error("ERR wrong number of argumments for 'SIMEMBER' command"))))
		return
	}
	key := string(args[1].(protocol.BulkString))
	member := string(args[2].(protocol.BulkString))

	// Check authentication and permissions
	if err := s.checkAuth(c, "SISMEMBER", key); err != nil {
		c.Write([]byte(protocol.Encode(protocol.Error(fmt.Sprintf("ERR %s", err.Error())))))
		return
	}

	res := s.shards.Execute("SISMEMBER", key, member)
	if ok, _ := res.(bool); ok {
		c.Write([]byte(protocol.Encode(protocol.Integer(1))))
	} else {
		c.Write([]byte(protocol.Encode(protocol.Integer(0))))
	}
}

func (s *Server) handleSUnion(c net.Conn, args protocol.Array) {
	if len(args) < 2 {
		c.Write([]byte(protocol.Encode(protocol.Error("ERR wrong number of arguments for 'SUNION' command"))))
		return
	}
	keys := make([]string, 0, len(args)-1)
	for _, a := range args[1:] {
		keys = append(keys, string(a.(protocol.BulkString)))
	}

	// Check authentication and permissions for all keys
	for _, key := range keys {
		if err := s.checkAuth(c, "SUNION", key); err != nil {
			c.Write([]byte(protocol.Encode(protocol.Error(fmt.Sprintf("ERR %s", err.Error())))))
			return
		}
	}

	res := s.shards.Execute("SUNION", keys[0], keys...)
	result, _ := res.([]string)
	arr := make([]protocol.RESPType, 0, len(result))
	for _, v := range result {
		arr = append(arr, protocol.BulkString(v))
	}
	c.Write([]byte(protocol.Encode(protocol.Array(arr))))
}

func (s *Server) handleSInter(c net.Conn, args protocol.Array) {
	if len(args) < 2 {
		c.Write([]byte(protocol.Encode(protocol.Error("ERR wrong number of arguments for 'SINTER' command"))))
		return
	}

	keys := make([]string, 0, len(args)-1)
	for _, a := range args[1:] {
		keys = append(keys, string(a.(protocol.BulkString)))
	}

	// Check authentication and permissions for all keys
	for _, key := range keys {
		if err := s.checkAuth(c, "SINTER", key); err != nil {
			c.Write([]byte(protocol.Encode(protocol.Error(fmt.Sprintf("ERR %s", err.Error())))))
			return
		}
	}

	res := s.shards.Execute("SINTER", keys[0], keys...)
	result, _ := res.([]string)
	arr := make([]protocol.RESPType, 0, len(result))
	for _, v := range result {
		arr = append(arr, protocol.BulkString(v))
	}
	c.Write([]byte(protocol.Encode(protocol.Array(arr))))
}

func (s *Server) handleSDiff(c net.Conn, args protocol.Array) {
	if len(args) < 2 {
		c.Write([]byte(protocol.Encode(protocol.Error("ERR wrong number of arguments for 'SDIFF' command"))))
		return
	}

	keys := make([]string, 0, len(args)-1)
	for _, a := range args[1:] {
		keys = append(keys, string(a.(protocol.BulkString)))
	}

	// Check authentication and permissions for all keys
	for _, key := range keys {
		if err := s.checkAuth(c, "SDIFF", key); err != nil {
			c.Write([]byte(protocol.Encode(protocol.Error(fmt.Sprintf("ERR %s", err.Error())))))
			return
		}
	}

	res := s.shards.Execute("SDIFF", keys[0], keys...)
	result, _ := res.([]string)
	arr := make([]protocol.RESPType, 0, len(result))
	for _, v := range result {
		arr = append(arr, protocol.BulkString(v))
	}
	c.Write([]byte(protocol.Encode(protocol.Array(arr))))
}

func (s *Server) handleSPop(c net.Conn, args protocol.Array) {
	if len(args) < 2 || len(args) > 3 {
		c.Write([]byte(protocol.Encode(protocol.Error("ERR wrong number of arguments for 'SPOP' command"))))
		return
	}
	key := string(args[1].(protocol.BulkString))

	// Check authentication and permissions
	if err := s.checkAuth(c, "SPOP", key); err != nil {
		c.Write([]byte(protocol.Encode(protocol.Error(fmt.Sprintf("ERR %s", err.Error())))))
		return
	}

	count := 1
	if len(args) == 3 {
		n, err := strconv.Atoi(string(args[2].(protocol.BulkString)))
		if err != nil || n < 0 {
			c.Write([]byte(protocol.Encode(protocol.Error("ERR value is not an integer or out of range"))))
			return
		}
		count = n
	}

	res := s.shards.Execute("SPOP", key, fmt.Sprintf("%d", count))
	result, _ := res.([]string)
	if result == nil {
		c.Write([]byte(protocol.Encode(protocol.Error("ERR null"))))
		return
	}

	if count == 1 {
		c.Write([]byte(protocol.Encode(protocol.BulkString(result[0]))))
	} else {
		arr := make([]protocol.RESPType, len(result))
		for i, v := range result {
			arr[i] = protocol.BulkString(v)
		}
		c.Write([]byte(protocol.Encode(protocol.Array(arr))))
	}
}

func (s *Server) handleSRandMember(c net.Conn, args protocol.Array) {
	if len(args) < 2 {
		c.Write([]byte(protocol.Encode(protocol.Error("ERR wrong number of arguments for 'SRANDMEMBER' command"))))
		return
	}
	key := string(args[1].(protocol.BulkString))

	// Check authentication and permissions
	if err := s.checkAuth(c, "SRANDMEMBER", key); err != nil {
		c.Write([]byte(protocol.Encode(protocol.Error(fmt.Sprintf("ERR %s", err.Error())))))
		return
	}

	count := 0

	if len(args) > 2 {
		n, err := strconv.Atoi(string(args[2].(protocol.BulkString)))
		if err != nil {
			c.Write([]byte(protocol.Encode(protocol.Error("ERR value is not an integer or out of range"))))
			return
		}
		count = n
	}

	res := s.shards.Execute("SRANDMEMBER", key, fmt.Sprintf("%d", count))
	result, _ := res.([]string)
	if result == nil {
		c.Write([]byte(protocol.Encode(protocol.Array(nil))))
		return
	}

	if count == 0 {
		//single value
		c.Write([]byte(protocol.Encode(protocol.BulkString(result[0]))))
		return
	}

	// array response
	arr := make(protocol.Array, 0, len(result))
	for _, v := range result {
		arr = append(arr, protocol.BulkString(v))
	}
	c.Write([]byte(protocol.Encode(arr)))
}

func (s *Server) handleHSet(c net.Conn, args protocol.Array) {
	if len(args) < 4 {
		c.Write([]byte(protocol.Encode(protocol.Error("ERR wrong number of arguments for 'HSET' command"))))
		return
	}

	key := string(args[1].(protocol.BulkString))
	field := string(args[2].(protocol.BulkString))
	value := string(args[3].(protocol.BulkString))

	// Check authentication and permissions
	if err := s.checkAuth(c, "HSET", key); err != nil {
		c.Write([]byte(protocol.Encode(protocol.Error(fmt.Sprintf("ERR %s", err.Error())))))
		return
	}

	res := s.shards.Execute("HSET", key, field, value)
	if n, ok := res.(int); ok {
		c.Write([]byte(protocol.Encode(protocol.Integer(n))))
	} else {
		c.Write([]byte(protocol.Encode(protocol.Integer(0))))
	}
}

func (s *Server) handleHGet(c net.Conn, args protocol.Array) {
	if len(args) < 3 {
		c.Write([]byte(protocol.Encode(protocol.Error("ERR wrong number of arguments for 'HGET' command"))))
		return
	}

	key := string(args[1].(protocol.BulkString))
	field := string(args[2].(protocol.BulkString))

	// Check authentication and permissions
	if err := s.checkAuth(c, "HGET", key); err != nil {
		c.Write([]byte(protocol.Encode(protocol.Error(fmt.Sprintf("ERR %s", err.Error())))))
		return
	}

	res := s.shards.Execute("HGET", key, field)
	val, ok := res.(string)
	if !ok {
		c.Write([]byte(protocol.Encode(protocol.BulkString(nil))))
		return
	}
	c.Write([]byte(protocol.Encode(protocol.BulkString(val))))
}

func (s *Server) handleHDel(c net.Conn, args protocol.Array) {
	if len(args) < 3 {
		c.Write([]byte(protocol.Encode(protocol.Error("ERR wrong number of arguments for 'HDEL' command"))))
		return
	}

	key := string(args[1].(protocol.BulkString))

	// Check authentication and permissions
	if err := s.checkAuth(c, "HDEL", key); err != nil {
		c.Write([]byte(protocol.Encode(protocol.Error(fmt.Sprintf("ERR %s", err.Error())))))
		return
	}

	fields := make([]string, 0, len(args)-2)
	for _, a := range args[2:] {
		fields = append(fields, string(a.(protocol.BulkString)))
	}

	res := s.shards.Execute("HDEL", key, fields...)
	deleted, _ := res.(int)
	c.Write([]byte(protocol.Encode(protocol.Integer(deleted))))
}

func (s *Server) handleHGetAll(c net.Conn, args protocol.Array) {
	if len(args) != 2 {
		c.Write([]byte(protocol.Encode(protocol.Error("ERR wrong number of arguments for 'HGETALL' command"))))
		return
	}

	key := string(args[1].(protocol.BulkString))

	// Check authentication and permissions
	if err := s.checkAuth(c, "HGETALL", key); err != nil {
		c.Write([]byte(protocol.Encode(protocol.Error(fmt.Sprintf("ERR %s", err.Error())))))
		return
	}

	res := s.shards.Execute("HGETALL", key)
	result, _ := res.(map[string]string)

	if result == nil {
		// Redis returns empty array for non-existing or non-hash key
		c.Write([]byte(protocol.Encode(protocol.Array{})))
		return
	}

	arr := make(protocol.Array, 0, len(result)*2)
	for field, val := range result {
		arr = append(arr, protocol.BulkString(field), protocol.BulkString(val))
	}

	c.Write([]byte(protocol.Encode(arr)))
}

// CMS.INCR key item count
func (s *Server) handleCMSIncr(c net.Conn, args protocol.Array) {
	if len(args) != 4 {
		c.Write([]byte(protocol.Encode(protocol.Error("ERR wrong number of arguments for 'CMSINCR'"))))
		return
	}

	key := string(args[1].(protocol.BulkString))
	item := string(args[2].(protocol.BulkString))
	countStr := string(args[3].(protocol.BulkString))

	// Check authentication and permissions
	if err := s.checkAuth(c, "CMSINCR", key); err != nil {
		c.Write([]byte(protocol.Encode(protocol.Error(fmt.Sprintf("ERR %s", err.Error())))))
		return
	}

	count, err := strconv.Atoi(countStr)
	if err != nil {
		c.Write([]byte(protocol.Encode(protocol.Error("ERR invalid count"))))
		return
	}

	s.shards.Execute("CMSINCR", key, item, fmt.Sprintf("%d", count))
	c.Write([]byte(protocol.Encode(protocol.SimpleString("OK"))))
}

// CMS.QUERY key item
func (s *Server) handleCMSQuery(c net.Conn, args protocol.Array) {
	if len(args) != 3 {
		c.Write([]byte(protocol.Encode(protocol.Error("ERR wrong number of arguments for 'CMSQUERY'"))))
		return
	}

	key := string(args[1].(protocol.BulkString))
	item := string(args[2].(protocol.BulkString))

	// Check authentication and permissions
	if err := s.checkAuth(c, "CMSQUERY", key); err != nil {
		c.Write([]byte(protocol.Encode(protocol.Error(fmt.Sprintf("ERR %s", err.Error())))))
		return
	}

	res := s.shards.Execute("CMSQUERY", key, item)
	count, _ := res.(uint32)
	c.Write([]byte(protocol.Encode(protocol.Integer(count))))
}

// LPUSH key value [value ...]
func (s *Server) handleLPush(c net.Conn, args protocol.Array) {
	if len(args) < 3 {
		c.Write([]byte(protocol.Encode(protocol.Error("ERR wrong number of arguments for 'LPUSH' command"))))
		return
	}
	key := string(args[1].(protocol.BulkString))

	// Check authentication and permissions
	if err := s.checkAuth(c, "LPUSH", key); err != nil {
		c.Write([]byte(protocol.Encode(protocol.Error(fmt.Sprintf("ERR %s", err.Error())))))
		return
	}

	values := []string{}
	for i := 2; i < len(args); i++ {
		values = append(values, string(args[i].(protocol.BulkString)))
	}

	res := s.shards.Execute("LPUSH", key, values...)
	newLen, _ := res.(int)
	c.Write([]byte(protocol.Encode(protocol.Integer(newLen))))
}

// RPUSH key value [value ...]
func (s *Server) handleRPush(c net.Conn, args protocol.Array) {
	if len(args) < 3 {
		c.Write([]byte(protocol.Encode(protocol.Error("ERR wrong number of arguments for 'RPUSH' command"))))
		return
	}
	key := string(args[1].(protocol.BulkString))

	// Check authentication and permissions
	if err := s.checkAuth(c, "RPUSH", key); err != nil {
		c.Write([]byte(protocol.Encode(protocol.Error(fmt.Sprintf("ERR %s", err.Error())))))
		return
	}

	values := []string{}
	for i := 2; i < len(args); i++ {
		values = append(values, string(args[i].(protocol.BulkString)))
	}

	res := s.shards.Execute("RPUSH", key, values...)
	newLen, _ := res.(int)
	c.Write([]byte(protocol.Encode(protocol.Integer(newLen))))
}

// LPOP key
func (s *Server) handleLPop(c net.Conn, args protocol.Array) {
	if len(args) != 2 {
		c.Write([]byte(protocol.Encode(protocol.Error("ERR wrong number of arguments for 'LPOP' command"))))
		return
	}
	key := string(args[1].(protocol.BulkString))

	// Check authentication and permissions
	if err := s.checkAuth(c, "LPOP", key); err != nil {
		c.Write([]byte(protocol.Encode(protocol.Error(fmt.Sprintf("ERR %s", err.Error())))))
		return
	}

	res := s.shards.Execute("LPOP", key)
	val, ok := res.(string)
	if !ok {
		c.Write([]byte(protocol.Encode(protocol.BulkString(nil))))
		return
	}

	c.Write([]byte(protocol.Encode(protocol.BulkString(val))))
}

// RPOP key
func (s *Server) handleRPop(c net.Conn, args protocol.Array) {
	if len(args) != 2 {
		c.Write([]byte(protocol.Encode(protocol.Error("ERR wrong number of arguments for 'RPOP' command"))))
		return
	}
	key := string(args[1].(protocol.BulkString))

	// Check authentication and permissions
	if err := s.checkAuth(c, "RPOP", key); err != nil {
		c.Write([]byte(protocol.Encode(protocol.Error(fmt.Sprintf("ERR %s", err.Error())))))
		return
	}

	res := s.shards.Execute("RPOP", key)
	val, ok := res.(string)
	if !ok {
		c.Write([]byte(protocol.Encode(protocol.BulkString(nil))))
		return
	}

	c.Write([]byte(protocol.Encode(protocol.BulkString(val))))
}

// LLEN key
func (s *Server) handleLLen(c net.Conn, args protocol.Array) {
	if len(args) != 2 {
		c.Write([]byte(protocol.Encode(protocol.Error("ERR wrong number of arguments for 'LLEN' command"))))
		return
	}
	key := string(args[1].(protocol.BulkString))

	// Check authentication and permissions
	if err := s.checkAuth(c, "LLEN", key); err != nil {
		c.Write([]byte(protocol.Encode(protocol.Error(fmt.Sprintf("ERR %s", err.Error())))))
		return
	}

	res := s.shards.Execute("LLEN", key)
	length, _ := res.(int)
	c.Write([]byte(protocol.Encode(protocol.Integer(length))))
}

// LRANGE key start stop
func (s *Server) handleLRange(c net.Conn, args protocol.Array) {
	if len(args) != 4 {
		c.Write([]byte(protocol.Encode(protocol.Error("ERR wrong number of arguments for 'LRANGE' command"))))
		return
	}
	key := string(args[1].(protocol.BulkString))
	startStr := string(args[2].(protocol.BulkString))
	stopStr := string(args[3].(protocol.BulkString))

	// Check authentication and permissions
	if err := s.checkAuth(c, "LRANGE", key); err != nil {
		c.Write([]byte(protocol.Encode(protocol.Error(fmt.Sprintf("ERR %s", err.Error())))))
		return
	}

	start, err1 := strconv.Atoi(startStr)
	stop, err2 := strconv.Atoi(stopStr)
	if err1 != nil || err2 != nil {
		c.Write([]byte(protocol.Encode(protocol.Error("ERR invalid start or stop index"))))
		return
	}

	res := s.shards.Execute("LRANGE", key, fmt.Sprintf("%d", start), fmt.Sprintf("%d", stop))
	result, _ := res.([]string)
	arr := make(protocol.Array, 0, len(result))
	for _, v := range result {
		arr = append(arr, protocol.BulkString(v))
	}

	c.Write([]byte(protocol.Encode(arr)))
}

// ZADD key score member [score member ...]
func (s *Server) handleZAdd(c net.Conn, args protocol.Array) {
	if len(args) < 3 {
		c.Write([]byte(protocol.Encode(protocol.Error("ERR wrong number of arguments for 'ZADD' command"))))
		return
	}
	key, _ := args[1].(protocol.BulkString)

	// Check authentication and permissions
	if err := s.checkAuth(c, "ZADD", string(key)); err != nil {
		c.Write([]byte(protocol.Encode(protocol.Error(fmt.Sprintf("ERR %s", err.Error())))))
		return
	}

	members := make(map[string]float64)
	for i := 2; i+1 < len(args); i += 2 {
		scoreStr, _ := args[i].(protocol.BulkString)
		member, _ := args[i+1].(protocol.BulkString)
		score, err := strconv.ParseFloat(string(scoreStr), 64)
		if err != nil {
			c.Write([]byte(protocol.Encode(protocol.Error("ERR invalid score for 'ZADD'"))))
			return
		}
		members[string(member)] = score
	}
	// Convert protocol.Array to []string for members
	memberArgs := []string{}
	for i := 2; i < len(args); i++ {
		memberArgs = append(memberArgs, string(args[i].(protocol.BulkString)))
	}
	res := s.shards.Execute("ZADD", string(key), memberArgs...)
	added, _ := res.(int)
	c.Write([]byte(protocol.Encode(protocol.Integer(added))))
}

// ZSCORE key member
func (s *Server) handleZScore(c net.Conn, args protocol.Array) {
	if len(args) != 3 {
		c.Write([]byte(protocol.Encode(protocol.Error("ERR wrong number of arguments for 'ZSCORE' command"))))
		return
	}
	key, _ := args[1].(protocol.BulkString)
	member, _ := args[2].(protocol.BulkString)

	// Check authentication and permissions
	if err := s.checkAuth(c, "ZSCORE", string(key)); err != nil {
		c.Write([]byte(protocol.Encode(protocol.Error(fmt.Sprintf("ERR %s", err.Error())))))
		return
	}

	res := s.shards.Execute("ZSCORE", string(key), string(member))
	score, ok := res.(float64)
	if !ok {
		c.Write([]byte(protocol.Encode(protocol.BulkString(nil))))
		return
	}
	c.Write([]byte(protocol.Encode(protocol.BulkString(fmt.Sprintf("%f", score)))))
}

// ZCARD key
func (s *Server) handleZCard(c net.Conn, args protocol.Array) {
	if len(args) != 2 {
		c.Write([]byte(protocol.Encode(protocol.Error("ERR wrong number of arguments for 'ZCARD' command"))))
		return
	}
	key, _ := args[1].(protocol.BulkString)

	// Check authentication and permissions
	if err := s.checkAuth(c, "ZCARD", string(key)); err != nil {
		c.Write([]byte(protocol.Encode(protocol.Error(fmt.Sprintf("ERR %s", err.Error())))))
		return
	}

	res := s.shards.Execute("ZCARD", string(key))
	count, _ := res.(int)
	c.Write([]byte(protocol.Encode(protocol.Integer(count))))
}

// ZRANK key member
func (s *Server) handleZRank(c net.Conn, args protocol.Array) {
	if len(args) != 3 {
		c.Write([]byte(protocol.Encode(protocol.Error("ERR wrong number of arguments for 'ZRANK' command"))))
		return
	}
	key, _ := args[1].(protocol.BulkString)
	member, _ := args[2].(protocol.BulkString)

	// Check authentication and permissions
	if err := s.checkAuth(c, "ZRANK", string(key)); err != nil {
		c.Write([]byte(protocol.Encode(protocol.Error(fmt.Sprintf("ERR %s", err.Error())))))
		return
	}

	res := s.shards.Execute("ZRANK", string(key), string(member))
	rank, ok := res.(int)
	if !ok {
		c.Write([]byte(protocol.Encode(protocol.BulkString(nil))))
		return
	}
	c.Write([]byte(protocol.Encode(protocol.Integer(rank))))
}

// ZRANGE key start stop [WITHSCORES]
func (s *Server) handleZRange(c net.Conn, args protocol.Array) {
	if len(args) < 4 {
		c.Write([]byte(protocol.Encode(protocol.Error("ERR wrong number of arguments for 'ZRANGE' command"))))
		return
	}
	key, _ := args[1].(protocol.BulkString)

	// Check authentication and permissions
	if err := s.checkAuth(c, "ZRANGE", string(key)); err != nil {
		c.Write([]byte(protocol.Encode(protocol.Error(fmt.Sprintf("ERR %s", err.Error())))))
		return
	}

	start, err1 := strconv.Atoi(string(args[2].(protocol.BulkString)))
	stop, err2 := strconv.Atoi(string(args[3].(protocol.BulkString)))
	withScores := false
	if len(args) > 4 && len(args) == 5 {
		if bs, ok := args[4].(protocol.BulkString); ok && (string(bs) == "WITHSCORES" || string(bs) == "withscores") {
			withScores = true
		}
	}
	if err1 != nil || err2 != nil {
		c.Write([]byte(protocol.Encode(protocol.Error("ERR invalid start/stop for 'ZRANGE'"))))
		return
	}
	res := s.shards.Execute("ZRANGE", string(key), fmt.Sprintf("%d", start), fmt.Sprintf("%d", stop), fmt.Sprintf("%t", withScores))
	result, _ := res.([]string)
	if result == nil {
		c.Write([]byte(protocol.Encode(protocol.BulkString(nil))))
		return
	}
	arr := make(protocol.Array, len(result))
	for i, v := range result {
		arr[i] = protocol.BulkString(v)
	}
	c.Write([]byte(protocol.Encode(arr)))
}

// BF.ADD key item
func (s *Server) handleBFAdd(c net.Conn, args protocol.Array) {
	if len(args) != 3 {
		c.Write([]byte(protocol.Encode(protocol.Error("ERR wrong number of arguments for 'BFADD' command (expected key m k item)"))))
		return
	}
	key, _ := args[1].(protocol.BulkString)
	item, _ := args[2].(protocol.BulkString)

	// Check authentication and permissions
	if err := s.checkAuth(c, "BFADD", string(key)); err != nil {
		c.Write([]byte(protocol.Encode(protocol.Error(fmt.Sprintf("ERR %s", err.Error())))))
		return
	}

	res := s.shards.Execute("BFADD", string(key), string(item))
	ok, _ := res.(bool)
	if ok {
		c.Write([]byte(protocol.Encode(protocol.Integer(1))))
	} else {
		c.Write([]byte(protocol.Encode(protocol.Integer(0))))
	}
}

// Handler for BFEXISTS: BFEXISTS key item
func (s *Server) handleBFExists(c net.Conn, args protocol.Array) {
	if len(args) != 3 {
		c.Write([]byte(protocol.Encode(protocol.Error("ERR wrong number of arguments for 'BFEXISTS' command (expected key item)"))))
		return
	}
	key, _ := args[1].(protocol.BulkString)
	item, _ := args[2].(protocol.BulkString)

	// Check authentication and permissions
	if err := s.checkAuth(c, "BFEXISTS", string(key)); err != nil {
		c.Write([]byte(protocol.Encode(protocol.Error(fmt.Sprintf("ERR %s", err.Error())))))
		return
	}

	res := s.shards.Execute("BFEXISTS", string(key), string(item))
	ok, _ := res.(bool)
	if ok {
		c.Write([]byte(protocol.Encode(protocol.Integer(1))))
	} else {
		c.Write([]byte(protocol.Encode(protocol.Integer(0))))
	}
}

func (s *Server) handleAddNode(c net.Conn, args protocol.Array) {
	if len(args) != 2 {
		c.Write([]byte(protocol.Encode(protocol.Error("ERR wrong number of arguments for 'ADDNODE' command (expected key)"))))
		return
	}
	key, _ := args[1].(protocol.BulkString)
	nodeID := string(key)

	// Check authentication and permissions (admin command)
	if err := s.checkAuth(c, "ADDNODE"); err != nil {
		c.Write([]byte(protocol.Encode(protocol.Error(fmt.Sprintf("ERR %s", err.Error())))))
		return
	}

	log.Printf("DEBUG: Handling ADDNODE command with key: %s", nodeID)

	// Create and add the new shard
	newShard := store.NewShard(store.NewStore())
	if err := s.shards.AddNode(nodeID, newShard); err != nil {
		log.Printf("ERROR: Failed to add node %s: %v", nodeID, err)
		c.Write([]byte(protocol.Encode(protocol.Error(fmt.Sprintf("ERR failed to add node: %v", err)))))
		return
	}

	// Start migration in background
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		if err := s.shards.BackgroundMigrateTo(ctx, nodeID, 10); err != nil {
			log.Printf("ERROR: Background migration for node %s failed: %v", nodeID, err)
		} else {
			log.Printf("DEBUG: %s - Background migration completed successfully", nodeID)
		}
	}()

	c.Write([]byte(protocol.Encode(protocol.SimpleString("OK"))))
}

func (s *Server) handleRemoveNode(c net.Conn, args protocol.Array) {
	if len(args) != 2 {
		c.Write([]byte(protocol.Encode(protocol.Error("ERR wrong number of arguments for 'REMOVENODE' command (expected key)"))))
		return
	}
	key, _ := args[1].(protocol.BulkString)
	nodeID := string(key)

	// Check authentication and permissions (admin command)
	if err := s.checkAuth(c, "REMOVENODE"); err != nil {
		c.Write([]byte(protocol.Encode(protocol.Error(fmt.Sprintf("ERR %s", err.Error())))))
		return
	}

	log.Printf("DEBUG: Handling REMOVENODE command for node: %s", nodeID)

	// Check if the node exists
	if _, exists := s.shards.GetShardByNodeID(nodeID); !exists {
		log.Printf("ERROR: Node %s does not exist", nodeID)
		c.Write([]byte(protocol.Encode(protocol.Error(fmt.Sprintf("ERR node %s does not exist", nodeID)))))
		return
	}

	// Before removing the node, we need to migrate its data to other nodes
	if shard, ok := s.shards.GetShardByNodeID(nodeID); ok {
		// Get all keys from the node that's being removed
		keys := shard.Store.ScanKeys(-1) // Get all keys
		log.Printf("DEBUG: Node %s has %d keys to migrate before removal", nodeID, len(keys))

		// Migrate each key to other nodes
		if len(keys) > 0 {
			// FIRST: Remove the node from hash ring so GetNodeForKey works correctly
			s.shards.RemoveNodeFromRing(nodeID)
			log.Printf("DEBUG: Removed node %s from hash ring", nodeID)

			// Group keys by their target nodes based on updated hash ring
			keysByTargetNode := make(map[string][]string)

			for _, key := range keys {
				// Hash key to determine which remaining node it should go to
				targetNode, ok := s.shards.GetNodeForKey(key)
				if !ok {
					log.Printf("WARNING: Could not determine target node for key %s", key)
					continue
				}

				// Skip if the target is the node being removed (shouldn't happen after removal from ring)
				if targetNode == nodeID {
					log.Printf("WARNING: Key %s still maps to removed node %s", key, nodeID)
					continue
				}

				keysByTargetNode[targetNode] = append(keysByTargetNode[targetNode], key)
			}

			log.Printf("DEBUG: Keys distribution for migration: %v", keysByTargetNode)

			// Migrate keys to their respective target nodes in batches
			totalMigrated := 0
			for targetNode, keysToMigrate := range keysByTargetNode {
				if len(keysToMigrate) == 0 {
					continue
				}

				log.Printf("DEBUG: Migrating %d keys from %s to %s", len(keysToMigrate), nodeID, targetNode)

				// Get target shard
				targetShard, ok := s.shards.GetShardByNodeID(targetNode)
				if !ok {
					log.Printf("ERROR: Target shard %s not found", targetNode)
					continue
				}

				// Migrate keys in batch to this target node
				migratedCount := s.shards.MigrateKeysBatch(shard, targetShard, keysToMigrate, nodeID, targetNode)
				totalMigrated += migratedCount
				log.Printf("DEBUG: Successfully migrated %d keys from %s to %s", migratedCount, nodeID, targetNode)
			}

			log.Printf("DEBUG: Total keys migrated from %s: %d/%d", nodeID, totalMigrated, len(keys))
		} else {
			// No keys to migrate, just remove from ring
			s.shards.RemoveNodeFromRing(nodeID)
			log.Printf("DEBUG: Removed node %s from hash ring (no keys to migrate)", nodeID)
		}

		// FINALLY: Remove the shard itself
		s.shards.RemoveShardOnly(nodeID)
	} else {
		// Node not found, just remove from ring if it exists
		s.shards.RemoveNodeFromRing(nodeID)
	}
	log.Printf("DEBUG: Successfully removed node %s", nodeID)

	c.Write([]byte(protocol.Encode(protocol.SimpleString("OK"))))
}

// Handle PUBLISH command: PUBLISH channel message
func (s *Server) handlePublish(c net.Conn, args protocol.Array) {
	if len(args) != 3 {
		c.Write([]byte(protocol.Encode(protocol.Error("ERR wrong number of arguments for 'PUBLISH' command"))))
		return
	}

	channel := string(args[1].(protocol.BulkString))
	message := string(args[2].(protocol.BulkString))

	// Check authentication and permissions for publish command
	if err := s.checkAuth(c, "PUBLISH"); err != nil {
		c.Write([]byte(protocol.Encode(protocol.Error(fmt.Sprintf("ERR %s", err.Error())))))
		return
	}

	log.Printf("DEBUG: Publishing message to channel %s: %s", channel, message)
	count := s.pubsub.Publish(channel, message)

	c.Write([]byte(protocol.Encode(protocol.Integer(count))))
}

// Handle SUBSCRIBE command: SUBSCRIBE channel [channel ...]
func (s *Server) handleSubscribe(c net.Conn, args protocol.Array) {
	if len(args) < 2 {
		c.Write([]byte(protocol.Encode(protocol.Error("ERR wrong number of arguments for 'SUBSCRIBE' command"))))
		return
	}

	channels := make([]string, 0, len(args)-1)
	for i := 1; i < len(args); i++ {
		channels = append(channels, string(args[i].(protocol.BulkString)))
	}

	// Check authentication and permissions for subscribe command
	if err := s.checkAuth(c, "SUBSCRIBE"); err != nil {
		c.Write([]byte(protocol.Encode(protocol.Error(fmt.Sprintf("ERR %s", err.Error())))))
		return
	}

	log.Printf("DEBUG: Subscribing to channels: %v", channels)

	// Get or create connection state
	s.mu.Lock()
	state, exists := s.connStates[c]
	if !exists {
		state = &ConnectionState{
			channels: make(map[string]bool),
		}
		s.connStates[c] = state
	}

	// Initialize message channel if not already done
	if state.msgCh == nil {
		state.msgCh = make(chan store.PubSubMessage, 100)
	}
	s.mu.Unlock()

	// Subscribe to all channels
	s.pubsub.Subscribe(channels, state.msgCh)

	// Update connection state
	state.mu.Lock()
	totalSubscriptions := len(state.channels)
	for _, channel := range channels {
		if !state.channels[channel] {
			state.channels[channel] = true
			totalSubscriptions++
		}
	}
	state.mu.Unlock()

	// Send subscription confirmations
	currentCount := totalSubscriptions - len(channels) + 1
	for _, channel := range channels {
		// Send subscribe confirmation: ["subscribe", channel, num_subscriptions]
		response := protocol.Array{
			protocol.BulkString("subscribe"),
			protocol.BulkString(channel),
			protocol.Integer(currentCount),
		}
		c.Write([]byte(protocol.Encode(response)))
		currentCount++
	}

	// Start message listener if this is the first subscription for this connection
	state.mu.RLock()
	isFirstSubscription := len(state.channels) == len(channels)
	state.mu.RUnlock()

	if isFirstSubscription {
		// Enter subscription mode - listen for messages
		go func() {
			for {
				select {
				case message, ok := <-state.msgCh:
					if !ok {
						return // Channel closed
					}

					// Send message to client: ["message", channel, message]
					response := protocol.Array{
						protocol.BulkString("message"),
						protocol.BulkString(message.Channel),
						protocol.BulkString(message.Message),
					}
					if _, err := c.Write([]byte(protocol.Encode(response))); err != nil {
						log.Printf("Failed to send message to subscriber: %v", err)
						return
					}
				case <-s.stopCh:
					return // Server shutting down
				}
			}
		}()
	}
}

// Handle UNSUBSCRIBE command: UNSUBSCRIBE [channel [channel ...]]
func (s *Server) handleUnsubscribe(c net.Conn, args protocol.Array) {
	// Check authentication and permissions for unsubscribe command
	if err := s.checkAuth(c, "UNSUBSCRIBE"); err != nil {
		c.Write([]byte(protocol.Encode(protocol.Error(fmt.Sprintf("ERR %s", err.Error())))))
		return
	}

	// Get connection state
	s.mu.RLock()
	state, exists := s.connStates[c]
	s.mu.RUnlock()

	if !exists || state.msgCh == nil {
		// Connection is not in subscription mode
		c.Write([]byte(protocol.Encode(protocol.Error("ERR connection is not subscribed"))))
		return
	}

	var channelsToUnsubscribe []string

	if len(args) == 1 {
		// Unsubscribe from all channels
		log.Printf("DEBUG: Unsubscribing from all channels")

		state.mu.RLock()
		channelsToUnsubscribe = make([]string, 0, len(state.channels))
		for channel := range state.channels {
			channelsToUnsubscribe = append(channelsToUnsubscribe, channel)
		}
		state.mu.RUnlock()
	} else {
		// Unsubscribe from specific channels
		channelsToUnsubscribe = make([]string, 0, len(args)-1)
		for i := 1; i < len(args); i++ {
			channel := string(args[i].(protocol.BulkString))

			// Only include channels we're actually subscribed to
			state.mu.RLock()
			if state.channels[channel] {
				channelsToUnsubscribe = append(channelsToUnsubscribe, channel)
			}
			state.mu.RUnlock()
		}
		log.Printf("DEBUG: Unsubscribing from channels: %v", channelsToUnsubscribe)
	}

	if len(channelsToUnsubscribe) == 0 {
		// No channels to unsubscribe from
		log.Printf("DEBUG: No channels to unsubscribe from")
		return
	}

	// Use the improved Unsubscribe method that returns actually removed channels
	removedChannels := s.pubsub.Unsubscribe(channelsToUnsubscribe, state.msgCh)

	log.Printf("DEBUG: Actually unsubscribed from channels: %v", removedChannels)

	// Update connection state
	state.mu.Lock()
	for _, channel := range removedChannels {
		delete(state.channels, channel)
	}
	remainingSubscriptions := len(state.channels)
	state.mu.Unlock()

	// Send unsubscribe confirmations for channels that were actually removed
	currentCount := remainingSubscriptions + len(removedChannels)
	for _, channel := range removedChannels {
		currentCount--
		response := protocol.Array{
			protocol.BulkString("unsubscribe"),
			protocol.BulkString(channel),
			protocol.Integer(currentCount), // remaining subscription count
		}
		c.Write([]byte(protocol.Encode(response)))
	}

	// If we unsubscribed from all channels, close the message channel
	if remainingSubscriptions == 0 {
		close(state.msgCh)
		state.msgCh = nil
	}
}
