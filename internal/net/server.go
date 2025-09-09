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

type ConnectionState struct {
	msgCh       chan store.PubSubMessage
	channels    map[string]bool // tracks which channels this connection is subscribed to
	mu          sync.RWMutex
	user        *store.ACLUser  // authenticated user for this connection
	authenticated bool           // whether connection is authenticated
}

type Server struct {
	addr   string
	shards *store.SharedStore
	pubsub *store.PubSub
	acl    *store.ACLManager  // ACL authentication and authorization
	ln     net.Listener

	// connection management
	mu         sync.RWMutex
	conns      map[net.Conn]struct{}
	connStates map[net.Conn]*ConnectionState // tracks subscription state per connection
	wg         sync.WaitGroup

	// lifecycle management
	stopOnce sync.Once
	stopCh   chan struct{}

	// debugging flags
	debug bool
}

// NewServer creates a new server with AOF persistence (AOF is now mandatory)
func NewServer(addr, aofPath string) (*Server, error) {
	return NewServerWithAOF(addr, aofPath)
}

// NewServerWithAOF creates a new server with AOF persistence enabled
func NewServerWithAOF(addr, aofPath string) (*Server, error) {
	sharedStore := store.NewSharedStore(2) // 2 replicas for consistent hashing

	// Create and add 2 shards with AOF enabled
	numShards := 2
	for i := 0; i < numShards; i++ {
		// Create AOF path for each shard
		shardAOFPath := fmt.Sprintf("%s.shard-%d", aofPath, i)
		st, err := store.NewStoreWithAOF(shardAOFPath)
		if err != nil {
			return nil, fmt.Errorf("failed to create store with AOF for shard %d: %v", i, err)
		}

		// Load existing data from AOF file
		if err := st.LoadFromAOF(); err != nil {
			log.Printf("WARNING: Failed to load AOF for shard %d: %v", i, err)
			// Continue anyway - empty store is valid
		}

		// Start cleaner for each store
		st.StartCleaner(20, 100000*time.Millisecond)
		shard := store.NewShard(st)
		nodeID := fmt.Sprintf("shard-%d", i)
		sharedStore.AddNode(nodeID, shard)
	}

	s := &Server{
		addr:       addr,
		shards:     sharedStore,
		pubsub:     store.NewPubSub(),
		acl:        store.NewACLManager(),
		conns:      make(map[net.Conn]struct{}),
		connStates: make(map[net.Conn]*ConnectionState),
		stopCh:     make(chan struct{}),
		mu:         sync.RWMutex{},
		wg:         sync.WaitGroup{},
		stopOnce:   sync.Once{},
		debug:      true,
	}

	return s, nil
}

// NewServerWithAOFConfig creates a new server with AOF persistence and custom config
func NewServerWithAOFConfig(addr, aofPath string, fsyncPolicy store.AOFFsyncPolicy, rewriteSize int64) (*Server, error) {
	sharedStore := store.NewSharedStore(2) // 2 replicas for consistent hashing

	// Create and add 2 shards with AOF enabled
	numShards := 2
	for i := 0; i < numShards; i++ {
		// Create AOF path for each shard
		shardAOFPath := fmt.Sprintf("%s.shard-%d", aofPath, i)
		st, err := store.NewStoreWithAOFConfig(shardAOFPath, fsyncPolicy, rewriteSize)
		if err != nil {
			return nil, fmt.Errorf("failed to create store with AOF config for shard %d: %v", i, err)
		}

		// Load existing data from AOF file
		if err := st.LoadFromAOF(); err != nil {
			log.Printf("WARNING: Failed to load AOF for shard %d: %v", i, err)
			// Continue anyway - empty store is valid
		}

		// Start cleaner for each store
		st.StartCleaner(20, 100000*time.Millisecond)
		shard := store.NewShard(st)
		nodeID := fmt.Sprintf("shard-%d", i)
		sharedStore.AddNode(nodeID, shard)
	}

	s := &Server{
		addr:       addr,
		shards:     sharedStore,
		pubsub:     store.NewPubSub(),
		acl:        store.NewACLManager(),
		conns:      make(map[net.Conn]struct{}),
		connStates: make(map[net.Conn]*ConnectionState),
		stopCh:     make(chan struct{}),
		mu:         sync.RWMutex{},
		wg:         sync.WaitGroup{},
		stopOnce:   sync.Once{},
		debug:      true,
	}

	return s, nil
}

// NewServerWithAOFAndRDB creates a new server with both AOF and RDB persistence
func NewServerWithAOFAndRDB(addr, aofPath, rdbPath string, fsyncPolicy store.AOFFsyncPolicy, rewriteSize int64, saveInterval time.Duration) (*Server, error) {
	sharedStore := store.NewSharedStore(2) // 2 replicas for consistent hashing

	// Create and add 2 shards with both AOF and RDB enabled
	numShards := 2
	for i := 0; i < numShards; i++ {
		// Create AOF and RDB paths for each shard
		shardAOFPath := fmt.Sprintf("%s.shard-%d", aofPath, i)
		shardRDBPath := fmt.Sprintf("%s.shard-%d", rdbPath, i)

		st, err := store.NewStoreWithAOFAndRDB(shardAOFPath, shardRDBPath, fsyncPolicy, rewriteSize)
		if err != nil {
			return nil, fmt.Errorf("failed to create store with AOF and RDB for shard %d: %v", i, err)
		}

		// Load existing data from both RDB and AOF
		if err := st.LoadFromPersistence(); err != nil {
			log.Printf("WARNING: Failed to load persistence for shard %d: %v", i, err)
			// Continue anyway - empty store is valid
		}

		// Start cleaner for each store
		st.StartCleaner(20, 100000*time.Millisecond)

		// Start periodic RDB saves if interval > 0
		if saveInterval > 0 {
			go func(store *store.Store, interval time.Duration, shardID int) {
				ticker := time.NewTicker(interval)
				defer ticker.Stop()

				for range ticker.C {
					log.Printf("RDB: Starting periodic save for shard %d", shardID)
					if err := store.SaveRDBSnapshot(); err != nil {
						log.Printf("RDB: Failed to save snapshot for shard %d: %v", shardID, err)
					} else {
						log.Printf("RDB: Successfully saved snapshot for shard %d", shardID)
					}
				}
			}(st, saveInterval, i)
		}

		shard := store.NewShard(st)
		nodeID := fmt.Sprintf("shard-%d", i)
		sharedStore.AddNode(nodeID, shard)
	}

	s := &Server{
		addr:       addr,
		shards:     sharedStore,
		pubsub:     store.NewPubSub(),
		acl:        store.NewACLManager(),
		conns:      make(map[net.Conn]struct{}),
		connStates: make(map[net.Conn]*ConnectionState),
		stopCh:     make(chan struct{}),
		mu:         sync.RWMutex{},
		wg:         sync.WaitGroup{},
		stopOnce:   sync.Once{},
		debug:      true,
	}

	return s, nil
}

// getDefaultUser returns the default user from ACL manager
func (s *Server) getDefaultUser() *store.ACLUser {
	user, _ := s.acl.GetUser("default")
	return user
}

// checkAuth checks if connection is authenticated and has permission for command
func (s *Server) checkAuth(c net.Conn, command string, keys ...string) error {
	s.mu.RLock()
	state, exists := s.connStates[c]
	s.mu.RUnlock()

	if !exists {
		return fmt.Errorf("connection state not found")
	}

	state.mu.RLock()
	user := state.user
	authenticated := state.authenticated
	state.mu.RUnlock()

	// Check if connection is authenticated
	if !authenticated {
		return fmt.Errorf("NOAUTH Authentication required")
	}

	// Check command permission
	if err := s.acl.CheckCommandPermission(user, command); err != nil {
		return err
	}

	// Check key permissions for commands that access keys
	for _, key := range keys {
		if err := s.acl.CheckKeyPermission(user, key); err != nil {
			return err
		}
	}

	return nil
}

// authenticateConnection sets the authenticated user for a connection
func (s *Server) authenticateConnection(c net.Conn, user *store.ACLUser) {
	s.mu.RLock()
	state, exists := s.connStates[c]
	s.mu.RUnlock()

	if exists {
		state.mu.Lock()
		state.user = user
		state.authenticated = true
		state.mu.Unlock()
	}
}

// ConfigureACL configures the ACL system with the provided settings
func (s *Server) ConfigureACL(requireAuth bool, defaultPassword string) error {
	if requireAuth {
		if err := s.acl.RequireAuthentication(); err != nil {
			return fmt.Errorf("failed to require authentication: %v", err)
		}
	}
	
	if defaultPassword != "" {
		if err := s.acl.SetDefaultUserPassword(defaultPassword); err != nil {
			return fmt.Errorf("failed to set default password: %v", err)
		}
	}
	
	return nil
}

// GetACLManager returns the ACL manager
func (s *Server) GetACLManager() *store.ACLManager {
	return s.acl
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
		s.connStates[conn] = &ConnectionState{
			channels:      make(map[string]bool),
			user:          s.getDefaultUser(),
			authenticated: true, // Default user is automatically authenticated (nopass)
		}
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
		// Cleanup connection state and unsubscribe from all channels
		if state, exists := s.connStates[c]; exists {
			if state.msgCh != nil {
				// Get all subscribed channels
				state.mu.RLock()
				channels := make([]string, 0, len(state.channels))
				for channel := range state.channels {
					channels = append(channels, channel)
				}
				state.mu.RUnlock()

				// Unsubscribe from all channels
				if len(channels) > 0 {
					s.pubsub.Unsubscribe(channels, state.msgCh)
				}
				close(state.msgCh)
			}
			delete(s.connStates, c)
		}
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
			case "AUTH":
				s.handleAUTH(c, v)
			case "ACL":
				s.handleACL(c, v)
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
			case "SUBSCRIBE":
				s.handleSubscribe(c, v)
			case "UNSUBSCRIBE":
				s.handleUnsubscribe(c, v)
			case "PUBLISH":
				s.handlePublish(c, v)
			default:
				c.Write([]byte(protocol.Encode(protocol.Error("ERR Unknown command"))))
			}
		default:
			c.Write([]byte(protocol.Encode(protocol.Error("ERR Invalid request"))))
		}
	}
}
