package net

import (
	"bufio"
	"fmt"
	"log"
	"net"

	"multithreaded-redis/internal/protocol"
)

type Server struct {
	addr string
}

func NewServer(addr string) *Server {
	return &Server{addr: addr}
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
			if len(v) > 0 {
				if cmd, ok := v[0].(protocol.BulkString); ok {
					switch string(cmd) {
					case "PING":
						c.Write([]byte(protocol.Encode(protocol.SimpleString("PONG"))))
					default:
						c.Write([]byte(protocol.Encode(protocol.Error("ERR Unknown command"))))
					}
				}
			}
		default:
			c.Write([]byte(protocol.Encode(protocol.Error("ERR Invalid request"))))
		}
	}
}
