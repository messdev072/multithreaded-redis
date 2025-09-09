package main

import (
	"context"
	"flag"
	"log"
	"multithreaded-redis/internal/net"
	"multithreaded-redis/internal/store"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"
)

func main() {
	// Parse command line flags
	var (
		addr         = flag.String("addr", ":6380", "Server address to bind to")
		logDir       = flag.String("logdir", "./logs", "Directory to store AOF log files")
		rdbDir       = flag.String("rdbdir", "./snapshots", "Directory to store RDB snapshot files")
		fsyncPolicy  = flag.String("fsync", "everysec", "AOF fsync policy: never, always, everysec")
		rewriteSize  = flag.Int64("aof-rewrite-size", 64*1024*1024, "AOF rewrite threshold in bytes (64MB default)")
		saveInterval = flag.Int("save-interval", 900, "RDB save interval in seconds (15 minutes default, 0 to disable)")
		enableRDB    = flag.Bool("enable-rdb", true, "Enable RDB snapshots")
	)
	flag.Parse()

	// Parse fsync policy
	var aofFsyncPolicy store.AOFFsyncPolicy
	switch *fsyncPolicy {
	case "never":
		aofFsyncPolicy = store.AOFFsyncNever
	case "always":
		aofFsyncPolicy = store.AOFFsyncAlways
	case "everysec":
		aofFsyncPolicy = store.AOFFsyncEverySec
	default:
		log.Fatalf("Invalid fsync policy: %s (must be: never, always, everysec)", *fsyncPolicy)
	}

	// Enable immediate logging
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	// Create log directory if it doesn't exist
	if err := os.MkdirAll(*logDir, 0755); err != nil {
		log.Fatalf("Failed to create log directory %s: %v", *logDir, err)
	}

	// Create RDB directory if RDB is enabled
	var rdbPath string
	if *enableRDB {
		if err := os.MkdirAll(*rdbDir, 0755); err != nil {
			log.Fatalf("Failed to create RDB directory %s: %v", *rdbDir, err)
		}
		rdbPath = filepath.Join(*rdbDir, "dump.rdb")
	}

	// AOF is now mandatory - create path in log directory
	aofPath := filepath.Join(*logDir, "redis.aof")
	log.Printf("Starting server with AOF enabled: %s", aofPath)
	log.Printf("AOF fsync policy: %s, rewrite threshold: %d bytes", *fsyncPolicy, *rewriteSize)

	if *enableRDB {
		log.Printf("RDB snapshots enabled: %s, save interval: %d seconds", rdbPath, *saveInterval)
	} else {
		log.Printf("RDB snapshots disabled")
	}

	var s *net.Server
	var err error

	if *enableRDB {
		s, err = net.NewServerWithAOFAndRDB(*addr, aofPath, rdbPath, aofFsyncPolicy, *rewriteSize, time.Duration(*saveInterval)*time.Second)
	} else {
		s, err = net.NewServerWithAOFConfig(*addr, aofPath, aofFsyncPolicy, *rewriteSize)
	}
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
