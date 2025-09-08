package main

import (
	"context"
	"flag"
	"log"
	"multithreaded-redis/internal/net"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"
)

func main() {
	// Parse command line flags
	var (
		addr   = flag.String("addr", ":6380", "Server address to bind to")
		logDir = flag.String("logdir", "./logs", "Directory to store AOF log files")
	)
	flag.Parse()

	// Enable immediate logging
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	// Create log directory if it doesn't exist
	if err := os.MkdirAll(*logDir, 0755); err != nil {
		log.Fatalf("Failed to create log directory %s: %v", *logDir, err)
	}

	// AOF is now mandatory - create path in log directory
	aofPath := filepath.Join(*logDir, "redis.aof")
	log.Printf("Starting server with AOF enabled: %s", aofPath)

	s, err := net.NewServer(*addr, aofPath)
	if err != nil {
		log.Fatalf("Error creating server: %v", err)
	}

	if err := s.Start(); err != nil {
		log.Fatalf("Error starting server: %v", err)
	}
	log.Printf("Server started and ready for commands")

	//gracefully shutdown on SIGINT or SIGTERM
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	<-ctx.Done()
	stop()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := s.Shutdown(shutdownCtx); err != nil {
		log.Printf("graceful shutdown timeout: %v", err)
	} else {
		log.Println("Server shut down gracefully")
	}
}
