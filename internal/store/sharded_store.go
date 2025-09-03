package store

import (
	"context"
	"fmt"
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

func (ss *SharedStore) AddNode(nodeID string, sh *Shard) {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	if _, ok := ss.nodeShards[nodeID]; ok {
		return
	}
	sh.nodeID = nodeID
	sh.parent = ss
	ss.nodeShards[nodeID] = sh
	ss.ring.AddNode(nodeID)
	go sh.Run()
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
func (ss *SharedStore) getShardForKey(key string) (*Shard, bool) {
	nodeID, ok := ss.ring.GetNode(key)
	if !ok {
		return nil, false
	}

	ss.mu.RLock()
	defer ss.mu.RUnlock()
	sh, ok := ss.nodeShards[nodeID]
	return sh, ok
}

func (ss *SharedStore) getShardByNodeID(nodeID string) (*Shard, bool) {
	ss.mu.RLock()
	defer ss.mu.RUnlock()
	sh, ok := ss.nodeShards[nodeID]
	return sh, ok
}

func (ss *SharedStore) Set(key string, val []byte, expire time.Duration) error {
	req := ShardRequest{
		Command: "SET",
		Key:     key,
		Args:    []string{string(val), expire.String()},
		Reply:   make(chan interface{}, 1),
	}
	shard, ok := ss.getShardForKey(key)
	if !ok {
		return fmt.Errorf("no shard available for key %s", key)
	}
	shard.inbox <- req
	resp := <-req.Reply
	if err, isErr := resp.(error); isErr {
		return err
	}
	return nil
}

func (ss *SharedStore) Get(key string) ([]byte, bool) {
	req := ShardRequest{
		Command: "GET",
		Key:     key,
		Reply:   make(chan interface{}, 1),
	}
	shard, ok := ss.getShardForKey(key)
	if !ok {
		return nil, false
	}
	shard.inbox <- req
	val := <-req.Reply
	if val == nil {
		return nil, false
	}
	if byteVal, ok := val.([]byte); ok {
		return byteVal, true
	}
	return nil, false
}

func (ss *SharedStore) Execute(cmd string, key string, args ...string) interface{} {
	req := ShardRequest{
		Command: cmd,
		Key:     key,
		Args:    args,
		Reply:   make(chan interface{}, 1),
	}
	shard, ok := ss.getShardForKey(key)
	if !ok {
		return nil
	}
	shard.inbox <- req
	return <-req.Reply
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
