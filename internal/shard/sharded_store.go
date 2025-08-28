package shard

import (
	"context"
	"hash/fnv"
	"multithreaded-redis/internal/store"
	"time"
)

type SharedStore struct {
	shards []*Shard
	count  int
}

// Count returns the number of shards
func (ss *SharedStore) Count() int {
	return ss.count
}

// Shards returns the slice of shards
func (ss *SharedStore) Shards() []*Shard {
	return ss.shards
}

func NewSharedStore(numShards int) *SharedStore {
	ss := &SharedStore{
		shards: make([]*Shard, numShards),
		count:  numShards,
	}

	for i := 0; i < numShards; i++ {
		sh := NewShard(store.NewStore())
		ss.shards[i] = sh
		go sh.Run()
	}
	return ss
}

func (ss *SharedStore) getShard(key string) *Shard {
	hash := fnv.New32a()
	hash.Write([]byte(key))
	idx := int(hash.Sum32()) % ss.count
	return ss.shards[idx]
}

func (ss *SharedStore) Set(key string, val []byte, expire time.Duration) {
	req := ShardRequest{
		Command: "SET",
		Key:     key,
		Args:    []string{string(val), expire.String()},
		Reply:   make(chan interface{}, 1),
	}
	ss.getShard(key).inbox <- req
	<-req.Reply
}

func (ss *SharedStore) Get(key string) ([]byte, bool) {
	req := ShardRequest{
		Command: "GET",
		Key:     key,
		Reply:   make(chan interface{}, 1),
	}
	ss.getShard(key).inbox <- req
	val := <-req.Reply
	if val == nil {
		return nil, false
	}
	return val.([]byte), true
}

func (ss *SharedStore) Execute(cmd string, key string, args ...string) interface{} {
	req := ShardRequest{
		Command: cmd,
		Key:     key,
		Args:    args,
		Reply:   make(chan interface{}, 1),
	}
	ss.getShard(key).inbox <- req
	return <-req.Reply
}

func (ss *SharedStore) Shutdown(ctx context.Context) error {
	for _, shard := range ss.shards {
		close(shard.quit)
	}
	for _, shard := range ss.shards {
		select {
		case <-shard.done:
			// Shard shut down gracefully
		case <-ctx.Done():
			return ctx.Err() // Timeout or cancellation
		}
	}
	return nil
}
