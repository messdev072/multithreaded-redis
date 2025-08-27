package shard

import (
	"fmt"
	"hash/fnv"
	"multithreaded-redis/internal/store"
	"strings"
	"time"
)

type Shard struct {
	Store *store.Store
	inbox chan ShardRequest
}

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

type ShardRequest struct {
	Command string
	Key     string
	Args    []string
	Reply   chan interface{}
}

func NewSharedStore(numShards int) *SharedStore {
	ss := &SharedStore{
		count: numShards,
	}
	ss.shards = make([]*Shard, numShards)
	for i := 0; i < numShards; i++ {
		shard := &Shard{
			Store: store.NewStore(),
			inbox: make(chan ShardRequest, 100),
		}
		ss.shards[i] = shard
		go shard.Run()
	}
	return ss
}

func (ss *SharedStore) getShard(key string) *Shard {
	hash := fnv.New32a()
	hash.Write([]byte(key))
	return ss.shards[uint(hash.Sum32())%uint(ss.count)]
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

func (s *Shard) Run() {
	for req := range s.inbox {
		switch strings.ToUpper(req.Command) {
		case "SET":
			if len(req.Args) < 1 {
				req.Reply <- fmt.Errorf("SET requires at least 1 argument")
				continue
			}
			val := []byte(req.Args[0])
			var expire time.Duration
			if len(req.Args) >= 2 {
				dur, err := time.ParseDuration(req.Args[1])
				if err != nil {
					req.Reply <- fmt.Errorf("invalid duration: %v", err)
					continue
				}
				expire = dur
			}
			s.Store.Set(req.Key, val, expire)
			req.Reply <- "OK"
		case "GET":
			val, found := s.Store.Get(req.Key)
			if !found {
				req.Reply <- nil
			} else {
				req.Reply <- val
			}
		case "DEL":
			deleted := s.Store.Delete(req.Key)
			req.Reply <- deleted
		case "SADD":
			if len(req.Args) < 1 {
				req.Reply <- 0
				continue
			}
			added := s.Store.SAdd(req.Key, req.Args...)
			req.Reply <- added
		case "SREM":
			if len(req.Args) < 1 {
				req.Reply <- 0
				continue
			}
			removed := s.Store.SRem(req.Key, req.Args...)
			req.Reply <- removed
		case "SMEMBERS":
			members := s.Store.SMembers(req.Key)
			req.Reply <- members
		case "SCARD":
			card := s.Store.SCard(req.Key)
			req.Reply <- card
		case "SISMEMBER":
			if len(req.Args) < 1 {
				req.Reply <- false
				continue
			}
			ok := s.Store.SIsMember(req.Key, req.Args[0])
			req.Reply <- ok
		case "SUNION":
			members := s.Store.SUnion(append([]string{req.Key}, req.Args...)...)
			req.Reply <- members
		case "SINTER":
			members := s.Store.SInter(append([]string{req.Key}, req.Args...)...)
			req.Reply <- members
		case "SDIFF":
			members := s.Store.SDiff(append([]string{req.Key}, req.Args...)...)
			req.Reply <- members
		case "SPOP":
			count := 1
			if len(req.Args) >= 1 {
				fmt.Sscanf(req.Args[0], "%d", &count)
			}
			members := s.Store.SPop(req.Key, count)
			req.Reply <- members
		case "SRANDMEMBER":
			count := 0
			if len(req.Args) >= 1 {
				fmt.Sscanf(req.Args[0], "%d", &count)
			}
			members := s.Store.SRandMember(req.Key, count)
			req.Reply <- members
		case "HSET":
			if len(req.Args) < 2 {
				req.Reply <- 0
				continue
			}
			n := s.Store.HSet(req.Key, req.Args[0], req.Args[1])
			req.Reply <- n
		case "HGET":
			if len(req.Args) < 1 {
				req.Reply <- ""
				continue
			}
			val, _ := s.Store.HGet(req.Key, req.Args[0])
			req.Reply <- val
		case "HDEL":
			if len(req.Args) < 1 {
				req.Reply <- 0
				continue
			}
			deleted := s.Store.HDel(req.Key, req.Args...)
			req.Reply <- deleted
		case "HGETALL":
			result := s.Store.HGetAll(req.Key)
			req.Reply <- result
		case "CMSINCR":
			if len(req.Args) < 2 {
				req.Reply <- nil
				continue
			}
			var count uint32
			fmt.Sscanf(req.Args[1], "%d", &count)
			s.Store.CMSIncr(req.Key, req.Args[0], count)
			req.Reply <- true
		case "CMSQUERY":
			if len(req.Args) < 1 {
				req.Reply <- uint32(0)
				continue
			}
			count := s.Store.CMSQuery(req.Key, req.Args[0])
			req.Reply <- count
		case "LPUSH":
			if len(req.Args) < 1 {
				req.Reply <- -1
				continue
			}
			newLen := s.Store.LPush(req.Key, req.Args...)
			req.Reply <- newLen
		case "RPUSH":
			if len(req.Args) < 1 {
				req.Reply <- -1
				continue
			}
			newLen := s.Store.RPush(req.Key, req.Args...)
			req.Reply <- newLen
		case "LPOP":
			val, _ := s.Store.LPop(req.Key)
			req.Reply <- val
		case "RPOP":
			val, _ := s.Store.RPop(req.Key)
			req.Reply <- val
		case "LLEN":
			length := s.Store.LLen(req.Key)
			req.Reply <- length
		case "LRANGE":
			if len(req.Args) < 2 {
				req.Reply <- nil
				continue
			}
			var start, stop int
			fmt.Sscanf(req.Args[0], "%d", &start)
			fmt.Sscanf(req.Args[1], "%d", &stop)
			result := s.Store.LRange(req.Key, start, stop)
			req.Reply <- result
		case "ZADD":
			if len(req.Args) < 2 || len(req.Args)%2 != 0 {
				req.Reply <- -1
				continue
			}
			members := make(map[string]float64)
			for i := 0; i < len(req.Args); i += 2 {
				score := 0.0
				fmt.Sscanf(req.Args[i], "%f", &score)
				members[req.Args[i+1]] = score
			}
			added := s.Store.ZAdd(req.Key, members)
			req.Reply <- added
		case "ZSCORE":
			if len(req.Args) < 1 {
				req.Reply <- 0.0
				continue
			}
			score, _ := s.Store.ZScore(req.Key, req.Args[0])
			req.Reply <- score
		case "ZCARD":
			count := s.Store.ZCard(req.Key)
			req.Reply <- count
		case "ZRANK":
			if len(req.Args) < 1 {
				req.Reply <- -1
				continue
			}
			rank, _ := s.Store.ZRank(req.Key, req.Args[0])
			req.Reply <- rank
		case "ZRANGE":
			if len(req.Args) < 2 {
				req.Reply <- nil
				continue
			}
			var start, stop int
			fmt.Sscanf(req.Args[0], "%d", &start)
			fmt.Sscanf(req.Args[1], "%d", &stop)
			withScores := false
			if len(req.Args) > 2 && strings.ToUpper(req.Args[2]) == "WITHSCORES" {
				withScores = true
			}
			result := s.Store.ZRange(req.Key, start, stop, withScores)
			req.Reply <- result
		case "BFADD":
			if len(req.Args) < 1 {
				req.Reply <- false
				continue
			}
			ok := s.Store.BFAdd(req.Key, req.Args[0])
			req.Reply <- ok
		case "BFEXISTS":
			if len(req.Args) < 1 {
				req.Reply <- false
				continue
			}
			ok := s.Store.BFExists(req.Key, req.Args[0])
			req.Reply <- ok
		default:
			req.Reply <- fmt.Errorf("unknown command: %s", req.Command)
		}
	}
}
