package main

import (
	"log"
	"multithreaded-redis/internal/net"
)

func main() {
	s := net.NewServer(":6380")
	if err := s.Start(); err != nil {
		log.Fatalf("Error starting server: %v", err)
	}
}
