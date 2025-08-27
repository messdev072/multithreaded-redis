package net

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"strconv"
	"time"

	"multithreaded-redis/internal/protocol"
	"multithreaded-redis/internal/shard"
)

type Server struct {
	addr   string
	shards *shard.SharedStore
}

func NewServer(addr string) *Server {
	s := &Server{
		addr:   addr,
		shards: shard.NewSharedStore(8), // 8 shards, adjust as needed
	}

	// Kick off active expiration for each shard
	for i := 0; i < s.shards.Count(); i++ {
		shard := s.shards.Shards()[i]
		if shard != nil {
			shard.Store.StartCleaner(20, 100000*time.Millisecond)
		}
	}
	return s
}

func (s *Server) Start() error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("failed to start server: %w", err)
	}
	defer ln.Close()
	log.Printf("Server started on %s", s.addr)
	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Printf("failed to accept connection: %v", err)
			continue
		}
		go s.handleConn(conn)
	}
}

// handleConn processes incoming connections and RESP commands
func (s *Server) handleConn(c net.Conn) {
	defer c.Close()
	r := bufio.NewReader(c)

	for {
		resp, err := protocol.ParseRESP(r)
		if err != nil {
			log.Printf("failed to parse RESP: %v", err)
			return
		}
		log.Printf("Received RESP: %v", resp)

		//Handle command
		switch v := resp.(type) {
		case protocol.Array:
			if len(v) == 0 {
				c.Write([]byte(protocol.Encode(protocol.Error("ERR Empty command"))))
				continue
			}
			cmd, ok := v[0].(protocol.BulkString)
			if !ok {
				c.Write([]byte(protocol.Encode(protocol.Error("ERR Invalid command type"))))
				continue
			}

			switch string(cmd) {
			case "PING":
				c.Write([]byte(protocol.Encode(protocol.SimpleString("PONG"))))
			case "SET":
				s.handleSET(c, v)
			case "GET":
				s.handleGET(c, v)
			case "DEL":
				s.handleDel(c, v)
			case "TTL":
				s.handleTTL(c, v)
			case "SADD":
				s.handleSAdd(c, v)
			case "SREM":
				s.handleSRem(c, v)
			case "SMEMBERS":
				s.handleSMembers(c, v)
			case "SCARD":
				s.handleSCard(c, v)
			case "SPOP":
				s.handleSPop(c, v)
			case "SUNION":
				s.handleSUnion(c, v)
			case "SINTER":
				s.handleSInter(c, v)
			case "SDIFF":
				s.handleSDiff(c, v)
			case "SISMEMBER":
				s.handleSIsMember(c, v)
			case "SRANDMEMBER":
				s.handleSRandMember(c, v)
			case "HSET":
				s.handleHSet(c, v)
			case "HGET":
				s.handleHGet(c, v)
			case "HDEL":
				s.handleHDel(c, v)
			case "HGETALL":
				s.handleHGetAll(c, v)
			case "CMS.INCR":
				s.handleCMSIncr(c, v)
			case "CMS.QUERY":
				s.handleCMSQuery(c, v)
			case "LPUSH":
				s.handleLPush(c, v)
			case "RPUSH":
				s.handleRPush(c, v)
			case "LPOP":
				s.handleLPop(c, v)
			case "RPOP":
				s.handleRPop(c, v)
			case "LLEN":
				s.handleLLen(c, v)
			case "LRANGE":
				s.handleLRange(c, v)
			case "ZADD":
				s.handleZAdd(c, v)
			case "ZSCORE":
				s.handleZScore(c, v)
			case "ZCARD":
				s.handleZCard(c, v)
			case "ZRANK":
				s.handleZRank(c, v)
			case "ZRANGE":
				s.handleZRange(c, v)
			case "BFADD":
				s.handleBFAdd(c, v)
			case "BFEXISTS":
				s.handleBFExists(c, v)
			default:
				c.Write([]byte(protocol.Encode(protocol.Error("ERR Unknown command"))))
			}
		default:
			c.Write([]byte(protocol.Encode(protocol.Error("ERR Invalid request"))))
		}
	}
}

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
		c.Write([]byte(protocol.Encode(protocol.Error("ERR wrong number of arguments for 'CMS.INCR'"))))
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
		c.Write([]byte(protocol.Encode(protocol.Error("ERR wrong number of arguments for 'CMS.QUERY'"))))
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
