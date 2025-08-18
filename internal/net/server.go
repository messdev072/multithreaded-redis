package net

import (
	"fmt"
	"log"
	"net"
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
	buf := make([]byte, 1024)

	for {
		n, err := c.Read(buf)
		if err != nil {
			log.Printf("Connection closed : %v", err)
			return
		}
		input := string(buf[:n])
		log.Printf("Received: %s", input)

		// Echo back for now (until RESP parser is in)
		_, _ = c.Write([]byte(fmt.Sprintf("Echo: %s", input)))
	}
}
