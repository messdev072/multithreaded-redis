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
	val, ok := s.shards.Get(string(key))
	if !ok {
		c.Write([]byte(protocol.Encode(protocol.BulkString(nil))))
		return
	}
	c.Write([]byte(protocol.Encode(protocol.BulkString(val))))
}

// Handle DEL command
func (s *Server) handleDel(c net.Conn, args protocol.Array) {
	if len(args) < 2 {
		c.Write([]byte(protocol.Encode(protocol.Error("ERR wrong number of arguments for 'DEL' command"))))
		return
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
		c.Write([]byte(protocol.Encode(protocol.Error("ERR wrong number of arguments for 'SCARD' command"))))
	}
	key := string(args[1].(protocol.BulkString))
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
	}
	key := string(args[1].(protocol.BulkString))
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
	res := s.shards.Execute("ZSCORE", string(key), string(member))
	score, ok := res.(float64)
	if !ok {
		c.Write([]byte(protocol.Encode(protocol.BulkString(nil))))
		return
	}
	c.Write([]byte(protocol.Encode(protocol.BulkString(fmt.Sprintf("%f", score)))))
}

// ZSCORE key member
func (s *Server) handleZCard(c net.Conn, args protocol.Array) {
	if len(args) != 2 {
		c.Write([]byte(protocol.Encode(protocol.Error("ERR wrong number of arguments for 'ZCARD' command"))))
		return
	}
	key, _ := args[1].(protocol.BulkString)
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

	log.Printf("DEBUG: Subscribing to channels: %v", channels)

	// Create a channel for this subscription
	msgCh := make(chan store.PubSubMessage, 100) // Buffer to prevent blocking

	// Subscribe to all channels
	s.pubsub.Subscribe(channels, msgCh)

	// Send subscription confirmations
	for i, channel := range channels {
		// Send subscribe confirmation: ["subscribe", channel, num_subscriptions]
		response := protocol.Array{
			protocol.BulkString("subscribe"),
			protocol.BulkString(channel),
			protocol.Integer(i + 1), // subscription count
		}
		c.Write([]byte(protocol.Encode(response)))
	}

	// Enter subscription mode - listen for messages
	go func() {
		defer func() {
			// Cleanup: unsubscribe from all channels when connection closes
			s.pubsub.Unsubscribe(channels, msgCh)
			close(msgCh)
		}()

		for {
			select {
			case message, ok := <-msgCh:
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

// Handle UNSUBSCRIBE command: UNSUBSCRIBE [channel [channel ...]]
func (s *Server) handleUnsubscribe(c net.Conn, args protocol.Array) {
	// For now, we'll implement a simple version that doesn't track individual connection subscriptions
	// In a full implementation, you'd need to track which channels each connection is subscribed to

	if len(args) == 1 {
		// Unsubscribe from all channels - not implemented in this simple version
		c.Write([]byte(protocol.Encode(protocol.SimpleString("OK"))))
		return
	}

	channels := make([]string, 0, len(args)-1)
	for i := 1; i < len(args); i++ {
		channels = append(channels, string(args[i].(protocol.BulkString)))
	}

	log.Printf("DEBUG: Unsubscribing from channels: %v", channels)

	// Send unsubscribe confirmations
	for i, channel := range channels {
		response := protocol.Array{
			protocol.BulkString("unsubscribe"),
			protocol.BulkString(channel),
			protocol.Integer(len(channels) - i - 1), // remaining subscription count
		}
		c.Write([]byte(protocol.Encode(response)))
	}

	c.Write([]byte(protocol.Encode(protocol.SimpleString("OK"))))
}
