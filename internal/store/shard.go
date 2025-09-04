package store

import (
	"fmt"
	"log"
	"strings"
	"time"
)

type Shard struct {
	Store  *Store
	inbox  chan ShardRequest
	quit   chan struct{}
	done   chan struct{}
	nodeID string
	parent *SharedStore
}

type ShardRequest struct {
	Command  string
	Key      string
	Args     []string
	Reply    chan interface{}
	internal bool // mark interbal ops
	Payload  interface{}
}

type KeyDump struct {
	Key        string
	ValueType  int
	ValueBytes []byte    // serialized value OR we can pass typed fields (choose what's easier for you)
	TTL        time.Time // zero => no TTL
}

func NewShard(s *Store) *Shard {
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

	// Signal that we're ready to process requests
	ready := make(chan interface{}, 1)
	ready <- struct{}{}
	s.inbox <- ShardRequest{
		Command: "_INTERNAL_READY",
		Reply:   ready,
	}
	<-ready

	for {
		select {
		case req := <-s.inbox:
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
	//check if key should live on this shard (ring authoritative)
	if s.parent != nil && !req.internal {
		targetNode, _ := s.parent.ring.GetNode(req.Key)
		if targetNode != "" && targetNode != s.nodeID {
			//forward request to the correct shard
			if dest, ok := s.parent.getShardByNodeID(targetNode); ok {
				//forward : keep req but make sure Reply exists
				if req.Reply == nil {
					// if no reply expected, create a temp chan to avoid blocking
					req.Reply = make(chan interface{}, 1)
				}
				dest.inbox <- req
				//wait for resp and return to original caller
				resp := <-req.Reply
				//write back to reply if this was external
				if req.Reply != nil {
					req.Reply <- resp
				}
				return
			} else {
				// destination not found : return MOVED-like error
				if req.Reply != nil {
					req.Reply <- fmt.Errorf("MOVED: key %s should be on node %s", req.Key, targetNode)
				}
				return
			}
		}
	}

	cmd := strings.ToUpper(req.Command)
	log.Printf("DEBUG: %s - Processing %s command in shard %s", req.Key, cmd, s.nodeID)

	switch cmd {
	case "SET":
		if len(req.Args) < 1 {
			log.Printf("ERROR: %s - SET command missing value argument", req.Key)
			req.Reply <- fmt.Errorf("SET requires at least 1 argument")
			return
		}
		val := []byte(req.Args[0])
		var expire time.Duration
		if len(req.Args) >= 2 {
			dur, err := time.ParseDuration(req.Args[1])
			if err != nil {
				log.Printf("ERROR: %s - Invalid expiration duration: %v", req.Key, err)
				req.Reply <- fmt.Errorf("invalid duration: %v", err)
				return
			}
			expire = dur
		}
		expireStr := ""
		if expire > 0 {
			expireStr = fmt.Sprintf(" and expiration %v", expire)
		}
		log.Printf("DEBUG: %s - Setting value with length %d bytes%s",
			req.Key, len(val), expireStr)
		s.Store.Set(req.Key, val, expire)
		log.Printf("DEBUG: %s - Successfully set value", req.Key)
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
	case "DUMPKEY":
		// internal API : return KeyDump or nil
		val, ok := s.Store.getRaw(req.Key)
		if !ok {
			log.Printf("DEBUG: %s - Not found in shard during DUMPKEY", req.Key)
			if req.Reply != nil {
				req.Reply <- nil
			}
			return
		}

		// Log value details based on type
		switch val.Type {
		case StringType:
			log.Printf("DEBUG: %s - Found in source shard with type=STRING, data=%q", req.Key, string(val.Data))
		case SetType:
			log.Printf("DEBUG: %s - Found in source shard with type=SET, members=%d", req.Key, len(val.Set))
		case HashType:
			log.Printf("DEBUG: %s - Found in source shard with type=HASH, fields=%d", req.Key, len(val.Hash))
		case CMSType:
			if val.CMS != nil {
				log.Printf("DEBUG: %s - Found in source shard with type=CMS, width=%d, depth=%d",
					req.Key, val.CMS.Width, val.CMS.Depth)
			} else {
				log.Printf("DEBUG: %s - Found in source shard with type=CMS but CMS is nil", req.Key)
			}
		default:
			log.Printf("DEBUG: %s - Found in source shard with type=%d", req.Key, val.Type)
		}

		valueBytes := s.Store.serializeValue(val)
		if valueBytes == nil {
			log.Printf("ERROR: %s - Failed to serialize value", req.Key)
			if req.Reply != nil {
				req.Reply <- nil
			}
			return
		}

		kd := KeyDump{
			Key:        req.Key,
			ValueType:  int(val.Type),
			ValueBytes: valueBytes,
			TTL:        s.Store.getExpirationTime(req.Key),
		}

		log.Printf("DEBUG: %s - Dumped value: type=%d, size=%d bytes",
			req.Key, kd.ValueType, len(kd.ValueBytes))

		if req.Reply != nil {
			req.Reply <- kd
		}
		return
	case "MIGRATE_RESTORE":
		// expecting Payload to be KeyDump
		kd, ok := req.Payload.(KeyDump)
		if !ok {
			log.Printf("DEBUG: %s - Bad payload type for MIGRATE_RESTORE: %T", req.Key, req.Payload)
			if req.Reply != nil {
				req.Reply <- fmt.Errorf("bad payload")
			}
			return
		}
		log.Printf("DEBUG: %s - Starting restore with type=%d, size=%d bytes",
			kd.Key, kd.ValueType, len(kd.ValueBytes))

		// restore into s.store preserving TTL
		if err := s.Store.restoreFromDump(kd); err != nil {
			log.Printf("ERROR: %s - Failed to restore: %v", kd.Key, err)
			if req.Reply != nil {
				req.Reply <- err
			}
			return
		}
		log.Printf("DEBUG: %s - Successfully restored", kd.Key)
		if req.Reply != nil {
			req.Reply <- true
		}
		return
	case "MIGRATE_DELETE":
		deleted := s.Store.Delete(req.Key)
		if req.Reply != nil {
			req.Reply <- deleted
		}
		return
	default:
		req.Reply <- fmt.Errorf("unknown command: %s", req.Command)
	}
}
