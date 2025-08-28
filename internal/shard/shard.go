package shard

import (
	"fmt"
	"multithreaded-redis/internal/store"
	"strings"
	"time"
)

type Shard struct {
	Store *store.Store
	inbox chan ShardRequest
	quit  chan struct{}
	done  chan struct{}
}

type ShardRequest struct {
	Command string
	Key     string
	Args    []string
	Reply   chan interface{}
}

func NewShard(s *store.Store) *Shard {
	shard := &Shard{
		Store: s,
		inbox: make(chan ShardRequest, 100),
		quit:  make(chan struct{}),
		done:  make(chan struct{}),
	}
	return shard
}

func (s *Shard) Run() {
	defer close(s.done)
	for {
		select {
		case req, ok := <-s.inbox:
			if !ok {
				//Inbox channel closed, exit the goroutine
				return
			}
			s.handle(req)
		case <-s.quit:
			// Drain remaining requests before exiting
			for {
				select {
				case req := <-s.inbox:
					s.handle(req)
				default:
					return
				}
			}
		}
	}
}

func (s *Shard) handle(req ShardRequest) {
	cmd := strings.ToUpper(req.Command)

	switch cmd {
	case "SET":
		if len(req.Args) < 1 {
			req.Reply <- fmt.Errorf("SET requires at least 1 argument")
			return
		}
		val := []byte(req.Args[0])
		var expire time.Duration
		if len(req.Args) >= 2 {
			dur, err := time.ParseDuration(req.Args[1])
			if err != nil {
				req.Reply <- fmt.Errorf("invalid duration: %v", err)
				return
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
			return
		}
		added := s.Store.SAdd(req.Key, req.Args...)
		req.Reply <- added
	case "SREM":
		if len(req.Args) < 1 {
			req.Reply <- 0
			return
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
			return
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
			return
		}
		n := s.Store.HSet(req.Key, req.Args[0], req.Args[1])
		req.Reply <- n
	case "HGET":
		if len(req.Args) < 1 {
			req.Reply <- ""
			return
		}
		val, _ := s.Store.HGet(req.Key, req.Args[0])
		req.Reply <- val
	case "HDEL":
		if len(req.Args) < 1 {
			req.Reply <- 0
			return
		}
		deleted := s.Store.HDel(req.Key, req.Args...)
		req.Reply <- deleted
	case "HGETALL":
		result := s.Store.HGetAll(req.Key)
		req.Reply <- result
	case "CMSINCR":
		if len(req.Args) < 2 {
			req.Reply <- nil
			return
		}
		var count uint32
		fmt.Sscanf(req.Args[1], "%d", &count)
		s.Store.CMSIncr(req.Key, req.Args[0], count)
		req.Reply <- true
	case "CMSQUERY":
		if len(req.Args) < 1 {
			req.Reply <- uint32(0)
			return
		}
		count := s.Store.CMSQuery(req.Key, req.Args[0])
		req.Reply <- count
	case "LPUSH":
		if len(req.Args) < 1 {
			req.Reply <- -1
			return
		}
		newLen := s.Store.LPush(req.Key, req.Args...)
		req.Reply <- newLen
	case "RPUSH":
		if len(req.Args) < 1 {
			req.Reply <- -1
			return
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
			return
		}
		var start, stop int
		fmt.Sscanf(req.Args[0], "%d", &start)
		fmt.Sscanf(req.Args[1], "%d", &stop)
		result := s.Store.LRange(req.Key, start, stop)
		req.Reply <- result
	case "ZADD":
		if len(req.Args) < 2 || len(req.Args)%2 != 0 {
			req.Reply <- -1
			return
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
			return
		}
		score, _ := s.Store.ZScore(req.Key, req.Args[0])
		req.Reply <- score
	case "ZCARD":
		count := s.Store.ZCard(req.Key)
		req.Reply <- count
	case "ZRANK":
		if len(req.Args) < 1 {
			req.Reply <- -1
			return
		}
		rank, _ := s.Store.ZRank(req.Key, req.Args[0])
		req.Reply <- rank
	case "ZRANGE":
		if len(req.Args) < 2 {
			req.Reply <- nil
			return
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
			return
		}
		ok := s.Store.BFAdd(req.Key, req.Args[0])
		req.Reply <- ok
	case "BFEXISTS":
		if len(req.Args) < 1 {
			req.Reply <- false
			return
		}
		ok := s.Store.BFExists(req.Key, req.Args[0])
		req.Reply <- ok
	default:
		req.Reply <- fmt.Errorf("unknown command: %s", req.Command)
	}
}
