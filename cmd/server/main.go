package main

import (
	"context"
	"log"
	"multithreaded-redis/internal/net"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	// Enable immediate logging
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	
	s := net.NewServer(":6380")
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
