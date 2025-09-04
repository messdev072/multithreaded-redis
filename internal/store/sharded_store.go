package store

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"
)

type SharedStore struct {
	mu         sync.RWMutex
	ring       *HashRing
	nodeShards map[string]*Shard // map nodeID to Shard
	// optional : local cached mapping for pickShard faster path
}

func NewSharedStore(replicas int) *SharedStore {
	ss := &SharedStore{
		ring:       NewHashRing(replicas),
		nodeShards: make(map[string]*Shard),
	}

	return ss
}

func (ss *SharedStore) AddNode(nodeID string, sh *Shard) error {
	ss.mu.Lock()
	// Check for existing node under lock
	if _, ok := ss.nodeShards[nodeID]; ok {
		ss.mu.Unlock()
		log.Printf("WARNING: Node %s already exists, ignoring add request", nodeID)
		return fmt.Errorf("node %s already exists", nodeID)
	}

	// Set up the new shard
	sh.nodeID = nodeID
	sh.parent = ss
	ss.nodeShards[nodeID] = sh
	ss.ring.AddNode(nodeID)
	log.Printf("DEBUG: %s - Added node to ring with %d replicas", nodeID, ss.ring.replicas)

	// Start the shard worker before waiting for ready
	go sh.Run()
	ss.mu.Unlock() // Release lock before waiting

	// Wait for shard to be ready with timeout
	ready := make(chan interface{}, 1)
	sh.inbox <- ShardRequest{
		Command: "_INTERNAL_READY",
		Reply:   ready,
	}

	select {
	case <-ready:
		log.Printf("DEBUG: %s - Node worker is ready", nodeID)
		return nil
	case <-time.After(5 * time.Second):
		// Clean up if shard doesn't become ready
		ss.mu.Lock()
		delete(ss.nodeShards, nodeID)
		ss.ring.RemoveNode(nodeID)
		ss.mu.Unlock()
		log.Printf("ERROR: %s - Node worker failed to become ready", nodeID)
		return fmt.Errorf("node %s failed to become ready", nodeID)
	}
}

func (ss *SharedStore) RemoveNode(nodeID string) {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	if sh, ok := ss.nodeShards[nodeID]; ok {
		// signal shard to stop accepting new requests, drain via its Run() quit handling
		close(sh.quit)
		delete(ss.nodeShards, nodeID)

	}
	ss.ring.RemoveNode(nodeID)
}

// Internal ultility: getShardForKey (by ring)
func (ss *SharedStore) getShardForKey(key string, command string) (*Shard, bool) {
	nodeID, ok := ss.ring.GetNode(key)
	if !ok {
		log.Printf("DEBUG: %s - Hash ring could not determine target node", key)
		// For SET-like operations, hash to any available shard
		if command == "SET" || command == "HSET" || command == "SADD" ||
			command == "ZADD" || command == "LPUSH" || command == "RPUSH" {
			ss.mu.RLock()
			defer ss.mu.RUnlock()

			// Get all available nodes
			nodes := make([]string, 0, len(ss.nodeShards))
			for node := range ss.nodeShards {
				nodes = append(nodes, node)
			}

			if len(nodes) > 0 {
				// Hash to a consistent node
				hash := ss.ring.hashStr(key)
				nodeID = nodes[hash%uint32(len(nodes))]
				sh, exists := ss.nodeShards[nodeID]
				if exists {
					log.Printf("DEBUG: %s - Hash ring assigned to node %s for SET-like operation", key, nodeID)
					return sh, true
				}
			}
		}
		return nil, false
	}

	log.Printf("DEBUG: %s - Hash ring maps to node %s", key, nodeID)

	ss.mu.RLock()
	defer ss.mu.RUnlock()
	sh, ok := ss.nodeShards[nodeID]
	if ok {
		log.Printf("DEBUG: %s - Found shard for node %s", key, nodeID)
	} else {
		log.Printf("DEBUG: %s - No shard found for node %s", key, nodeID)
	}
	return sh, ok
}

func (ss *SharedStore) getShardByNodeID(nodeID string) (*Shard, bool) {
	ss.mu.RLock()
	defer ss.mu.RUnlock()
	sh, ok := ss.nodeShards[nodeID]
	return sh, ok
}

func (ss *SharedStore) Execute(cmd string, key string, args ...string) interface{} {
	req := ShardRequest{
		Command: cmd,
		Key:     key,
		Args:    args,
		Reply:   make(chan interface{}, 1),
	}
	log.Printf("DEBUG: %s - Executing %s command", key, cmd)

	shard, ok := ss.getShardForKey(key, cmd)
	if !ok {
		log.Printf("DEBUG: %s - No shard available for command %s", key, cmd)
		return fmt.Errorf("no shard available for key %s", key)
	}

	log.Printf("DEBUG: %s - Sending %s command to shard %s", key, cmd, shard.nodeID)
	shard.inbox <- req
	resp := <-req.Reply
	log.Printf("DEBUG: %s - Got response type %T from shard %s", key, resp, shard.nodeID)
	return resp
}

func (ss *SharedStore) Set(key string, val []byte, expire time.Duration) error {
	resp := ss.Execute("SET", key, string(val), expire.String())
	if err, isErr := resp.(error); isErr {
		return err
	}
	if str, ok := resp.(string); ok && str == "OK" {
		return nil
	}
	return fmt.Errorf("unexpected response: %v", resp)
}

func (ss *SharedStore) Get(key string) ([]byte, bool) {
	resp := ss.Execute("GET", key)
	if resp == nil {
		log.Printf("DEBUG: %s - No value found", key)
		return nil, false
	}

	if byteVal, ok := resp.([]byte); ok {
		log.Printf("DEBUG: %s - Found value: %q", key, string(byteVal))
		return byteVal, true
	}

	log.Printf("DEBUG: %s - Unexpected response type: %T", key, resp)
	return nil, false
}

func (ss *SharedStore) Shutdown(ctx context.Context) error {
	ss.mu.RLock()
	shards := make([]*Shard, 0, len(ss.nodeShards))
	for _, shard := range ss.nodeShards {
		shards = append(shards, shard)
	}
	ss.mu.RUnlock()

	for _, shard := range shards {
		close(shard.quit)
	}
	for _, shard := range shards {
		select {
		case <-shard.done:
			// Shard shut down gracefully
		case <-ctx.Done():
			return ctx.Err() // Timeout or cancellation
		}
	}
	return nil
}
