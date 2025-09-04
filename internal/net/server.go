package net

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"multithreaded-redis/internal/protocol"
	"multithreaded-redis/internal/store"
)

type Server struct {
	addr   string
	shards *store.SharedStore
	ln     net.Listener

	// connection management
	mu    sync.Mutex
	conns map[net.Conn]struct{}
	wg    sync.WaitGroup

	// lifecycle management
	stopOnce sync.Once
	stopCh   chan struct{}

	// debugging flags
	debug bool
}

func (s *Server) logDebug(format string, args ...interface{}) {
	if s.debug {
		log.Printf(format, args...)
	}
}

func NewServer(addr string) *Server {
	sharedStore := store.NewSharedStore(2) // 2 replicas for consistent hashing

	// Create and add 2 shards
	numShards := 2
	for i := 0; i < numShards; i++ {
		st := store.NewStore()
		// Start cleaner for each store
		st.StartCleaner(20, 100000*time.Millisecond)
		shard := store.NewShard(st)
		nodeID := fmt.Sprintf("shard-%d", i)
		sharedStore.AddNode(nodeID, shard)
	}

	s := &Server{
		addr:     addr,
		shards:   sharedStore,
		conns:    make(map[net.Conn]struct{}),
		stopCh:   make(chan struct{}),
		mu:       sync.Mutex{},
		wg:       sync.WaitGroup{},
		stopOnce: sync.Once{},
		debug:    true,
	}

	return s
}

func (s *Server) Start() error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("failed to start server: %w", err)
	}
	s.ln = ln

	log.Printf("Server started on %s", s.addr)
	go s.acceptLoop()
	return nil
}

func (s *Server) acceptLoop() {
	for {
		conn, err := s.ln.Accept()
		if err != nil {
			select {
			case <-s.stopCh:
				// Server is shutting down
				return
			default:
				log.Printf("failed to accept connection: %v", err)
				continue
			}
		}
		s.mu.Lock()
		s.conns[conn] = struct{}{}
		s.mu.Unlock()

		s.wg.Add(1)
		go s.handleConn(conn)
	}
}

// Shutdown order:
// 1) stop accepting new connections
// 2) close current connections to unblock handlers
// 3) wait for handlers to finish
// 4) shutdown shards (drain + stop)
func (s *Server) Shutdown(ctx context.Context) error {
	var retErr error
	s.stopOnce.Do(func() {
		close(s.stopCh)
		if s.ln != nil {
			s.ln.Close()
		}

		// Close all active connections
		s.mu.Lock()
		for c := range s.conns {
			c.Close()
		}
		s.mu.Unlock()

		// Wait for all handlers to finish or context timeout
		doneCh := make(chan struct{})
		go func() {
			s.wg.Wait()
			close(doneCh)
		}()

		select {
		case <-doneCh:
			// All handlers finished
		case <-ctx.Done():
			retErr = ctx.Err()
		}

		// Shutdown shards
		if err := s.shards.Shutdown(ctx); err != nil && retErr == nil {
			retErr = err
		}
	})
	return retErr
}

// handleConn processes incoming connections and RESP commands
func (s *Server) handleConn(c net.Conn) {
	defer func() {
		s.mu.Lock()
		delete(s.conns, c)
		s.mu.Unlock()
		c.Close()
		s.wg.Done()
	}()
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

			cmdStr := string(cmd)
			log.Printf("Received command: %s with args: %v", cmdStr, v)

			switch cmdStr {
			case "PING":
				log.Printf("Handling PING command")
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
			case "CMSINCR":
				s.handleCMSIncr(c, v)
			case "CMSQUERY":
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
			case "ADDNODE":
				log.Printf("Handling ADDNODE command with key: %s", string(v[1].(protocol.BulkString)))
				s.handleAddNode(c, v)
			case "REMOVENODE":
				s.handleRemoveNode(c, v)
			default:
				c.Write([]byte(protocol.Encode(protocol.Error("ERR Unknown command"))))
			}
		default:
			c.Write([]byte(protocol.Encode(protocol.Error("ERR Invalid request"))))
		}
	}
}
