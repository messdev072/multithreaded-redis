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
	return &Server{
		addr:  addr,
		store: store.NewStore(),
	}
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
