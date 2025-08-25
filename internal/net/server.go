package net

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"strconv"
	"time"

	"multithreaded-redis/internal/protocol"
	"multithreaded-redis/internal/store"
)

type Server struct {
	addr  string
	store *store.Store
}

func NewServer(addr string) *Server {
	s := &Server{
		addr:  addr,
		store: store.NewStore(),
	}

	// Kick of active expiration
	s.store.StartCleaner(20, 100000*time.Millisecond)
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

	s.store.Set(string(key), []byte(val), expire)
	c.Write([]byte(protocol.Encode(protocol.SimpleString("OK"))))
}

// Handle GET command
func (s *Server) handleGET(c net.Conn, args protocol.Array) {
	if len(args) != 2 {
		c.Write([]byte(protocol.Encode(protocol.Error("ERR wrong number of arguments for 'GET' command"))))
		return
	}

	key, _ := args[1].(protocol.BulkString)
	val, ok := s.store.Get(string(key))
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
		if s.store.Delete(string(key)) {
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
	ttl := s.store.TTL(string(key))

	c.Write([]byte(protocol.Encode(protocol.Integer(ttl))))
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

	added := s.store.SAdd(key, members...)
	c.Write([]byte(protocol.Encode(protocol.Integer(added))))
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

	removed := s.store.SRem(key, members...)
	c.Write([]byte(protocol.Encode(protocol.Integer(removed))))
}

func (s *Server) handleSMembers(c net.Conn, args protocol.Array) {
	if len(args) != 2 {
		c.Write([]byte(protocol.Encode(protocol.Error("ERR wrong number of arguments for 'SMEMBERS' command"))))
	}
	key := string(args[1].(protocol.BulkString))

	members := s.store.SMembers(key)
	arr := []protocol.RESPType{}
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
	card := s.store.SCard(key)
	c.Write([]byte(protocol.Encode(protocol.Integer(card))))
}

func (s *Server) handleSIsMember(c net.Conn, args protocol.Array) {
	if len(args) != 3 {
		c.Write([]byte(protocol.Encode(protocol.Error("ERR wrong number of argumments for 'SIMEMBER' command"))))
		return
	}
	key := string(args[1].(protocol.BulkString))
	member := string(args[2].(protocol.BulkString))

	ok := s.store.SIsMember(key, member)
	if ok {
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

	result := s.store.SUnion(keys...)
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

	result := s.store.SInter(keys...)
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

	result := s.store.SDiff(keys...)
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

	result := s.store.SPop(key, count)
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

	result := s.store.SRandMember(key, count)
	if result == nil {
		c.Write([]byte(protocol.Encode(protocol.Array(nil))))
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

	res := s.store.HSet(key, field, value)
	c.Write([]byte(protocol.Encode(protocol.Integer(res))))
}

func (s *Server) handleHGet(c net.Conn, args protocol.Array) {
	if len(args) < 3 {
		c.Write([]byte(protocol.Encode(protocol.Error("ERR wrong number of arguments for 'HGET' command"))))
		return
	}

	key := string(args[1].(protocol.BulkString))
	field := string(args[2].(protocol.BulkString))

	val, ok := s.store.HGet(key, field)
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

	deleted := s.store.HDel(key, fields...)
	c.Write([]byte(protocol.Encode(protocol.Integer(deleted))))
}

func (s *Server) handleHGetAll(c net.Conn, args protocol.Array) {
	if len(args) != 2 {
		c.Write([]byte(protocol.Encode(protocol.Error("ERR wrong number of arguments for 'HGETALL' command"))))
		return
	}

	key := string(args[1].(protocol.BulkString))
	result := s.store.HGetAll(key)

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

	s.store.CMSIncr(key, item, uint32(count))
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

	count := s.store.CMSQuery(key, item)
	c.Write([]byte(protocol.Encode(protocol.Integer(count))))
}
