package store

import (
	"fmt"
	"hash/fnv"
	"log"
	"sort"
	"strings"
	"time"

	"multithreaded-redis/internal/datastuctures"
)

type ValueType int

const (
	StringType ValueType = iota
	SetType
	HashType
	CMSType
	ListType
	ZSetType
	BFType
)

type Value struct {
	Type       ValueType
	Data       []byte                        // for strings
	Set        map[string]struct{}           // for sets
	Hash       map[string]string             // for hashes
	CMS        *datastuctures.CountMinSketch // for Count-Min Sketch
	List       []string
	ZSet       map[string]float64
	BF         *datastuctures.BloomFilter // for Bloom Filter
	Expiration int64                      // Unix timestamp in seconds; 0 means no expiration
	LastAccess int64                      // Unix timestamp in seconds
}

const (
	// Number of buckets for sharding within a store
	NumBuckets = 16
)

// hash returns a bucket index for the given key
func hash(key string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(key))
	return h.Sum32() % NumBuckets
}

type Store struct {
	buckets [NumBuckets]*Bucket
	ttlKeys []string // for random sampling - kept for compatibility
	aof     *AOF     // Append Only File for persistence
	rdb     *RDB     // Redis Database snapshots
}

// getBucket returns the bucket for a given key
func (s *Store) getBucket(key string) *Bucket {
	return s.buckets[hash(key)]
}

// StoreStats contains basic statistics about a store
type StoreStats struct {
	KeyCount     int
	ExpiringKeys int
	StringKeys   int
	SetKeys      int
	HashKeys     int
	ListKeys     int
	ZSetKeys     int
	BFKeys       int
	CMSKeys      int
}

func NewStore() *Store {
	store := &Store{}
	for i := range store.buckets {
		store.buckets[i] = NewBucket()
	}
	return store
}

// NewStoreWithAOF creates a new Store with AOF persistence enabled
func NewStoreWithAOF(aofPath string) (*Store, error) {
	aof, err := NewAOF(aofPath)
	if err != nil {
		return nil, err
	}

	store := &Store{aof: aof}
	for i := range store.buckets {
		store.buckets[i] = NewBucket()
	}
	return store, nil
}

// NewStoreWithAOFConfig creates a new Store with AOF persistence and custom config
func NewStoreWithAOFConfig(aofPath string, fsyncPolicy AOFFsyncPolicy, rewriteSize int64) (*Store, error) {
	aof, err := NewAOFWithConfig(aofPath, fsyncPolicy, rewriteSize)
	if err != nil {
		return nil, err
	}

	store := &Store{aof: aof}
	for i := range store.buckets {
		store.buckets[i] = NewBucket()
	}
	return store, nil
}

// NewStoreWithAOFAndRDB creates a new Store with both AOF and RDB persistence
func NewStoreWithAOFAndRDB(aofPath, rdbPath string, fsyncPolicy AOFFsyncPolicy, rewriteSize int64) (*Store, error) {
	aof, err := NewAOFWithConfig(aofPath, fsyncPolicy, rewriteSize)
	if err != nil {
		return nil, err
	}

	rdb, err := NewRDB(rdbPath)
	if err != nil {
		return nil, err
	}

	store := &Store{aof: aof, rdb: rdb}
	for i := range store.buckets {
		store.buckets[i] = NewBucket()
	}
	return store, nil
}

// logToAOF appends a command to the AOF if AOF is enabled
func (s *Store) logToAOF(cmd string, args ...string) {
	if s.aof != nil {
		err := s.aof.Append(cmd, args...)
		if err != nil {
			log.Printf("ERROR: Failed to write to AOF: %v", err)
		}
	}
}

func (s *Store) Set(key string, val []byte, expire time.Duration) {
	expiration := int64(0)
	if expire > 0 {
		expiration = time.Now().Add(expire).Unix()
	}

	value := Value{
		Type:       StringType,
		Data:       val,
		Expiration: expiration,
		LastAccess: time.Now().UnixNano(),
	}

	bucket := s.getBucket(key)
	bucket.Set(key, value)

	// Log to AOF
	s.logToAOF("SET", key, string(val))
}

func (s *Store) Get(key string) ([]byte, bool) {
	bucket := s.getBucket(key)
	val, ok := bucket.Get(key)
	if !ok {
		log.Printf("DEBUG: %s - Not found in store", key)
		return nil, false
	}

	switch val.Type {
	case StringType:
		log.Printf("DEBUG: %s - Found string value with data %q", key, string(val.Data))
	case SetType:
		log.Printf("DEBUG: %s - Found set with %d members", key, len(val.Set))
	case HashType:
		log.Printf("DEBUG: %s - Found hash with %d fields", key, len(val.Hash))
	case CMSType:
		if val.CMS != nil {
			log.Printf("DEBUG: %s - Found CMS with width=%d, depth=%d", key, val.CMS.Width, val.CMS.Depth)
		} else {
			log.Printf("DEBUG: %s - Found CMS but it is nil", key)
		}
	default:
		log.Printf("DEBUG: %s - Found value of type %d", key, val.Type)
	}

	if val.Type != StringType {
		log.Printf("WARNING: %s - Incorrect type in store: got %d, expected %d (StringType)",
			key, val.Type, StringType)
		return nil, false
	}

	// For string values, check that we have data
	if len(val.Data) == 0 {
		log.Printf("WARNING: %s - Found with StringType but empty data", key)
		return nil, false
	}

	// Update last access time
	val.LastAccess = time.Now().UnixNano()
	bucket.Set(key, val)

	return val.Data, true
}

func (s *Store) Delete(key string) bool {
	bucket := s.getBucket(key)
	exists := bucket.Delete(key)

	if exists {
		// Log to AOF
		s.logToAOF("DEL", key)
	}

	return exists
}

func (s *Store) TTL(key string) int64 {
	bucket := s.getBucket(key)
	val, ok := bucket.Get(key)
	if !ok {
		return -2 // key does not exist
	}

	if val.Expiration == 0 {
		return -1 // no expiration
	}

	ttl := val.Expiration - time.Now().Unix()
	if ttl <= 0 {
		return -2 // already expired
	}
	return ttl
}

func (s *Store) PTTL(key string) int64 {
	bucket := s.getBucket(key)
	val, ok := bucket.Get(key)
	if !ok {
		return -2 // key does not exist
	}

	if val.Expiration == 0 {
		return -1 // no expiration
	}

	ttl := (val.Expiration - time.Now().Unix()) * 1000
	if ttl <= 0 {
		return -2 // already expired
	}
	return ttl
}

// StartCleaner starts background cleanup of expired keys
func (s *Store) StartCleaner(sampleSize int, interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for range ticker.C {
			// Clean expired keys from all buckets
			for i := range s.buckets {
				s.buckets[i].CleanupExpired()
			}
		}
	}()
}

// EvictOne evicts one key using approximate LRU from all buckets
func (s *Store) EvictOne() bool {
	sampleSize := 5
	var oldestBucket *Bucket
	var oldestKey string
	var oldestTime int64 = time.Now().UnixNano()

	// Sample keys from all buckets
	for i := range s.buckets {
		bucket := s.buckets[i]
		keys := bucket.Keys()

		// Sample from this bucket
		for j := 0; j < sampleSize && j < len(keys); j++ {
			key := keys[j]
			val, ok := bucket.Get(key)
			if !ok {
				continue
			}
			if val.LastAccess < oldestTime {
				oldestTime = val.LastAccess
				oldestKey = key
				oldestBucket = bucket
			}
		}
	}

	if oldestBucket != nil && oldestKey != "" {
		return oldestBucket.Delete(oldestKey)
	}
	return false
}

// ScanKeys returns all keys from all buckets
func (s *Store) ScanKeys(batchSize int) []string {
	var allKeys []string

	for i := range s.buckets {
		keys := s.buckets[i].Keys()
		allKeys = append(allKeys, keys...)
	}

	// return at most batchSize keys
	if batchSize <= 0 || len(allKeys) <= batchSize {
		return allKeys
	}
	return allKeys[:batchSize]
}

// GetStats returns statistics aggregated from all buckets
func (s *Store) GetStats() StoreStats {
	stats := StoreStats{}

	for i := range s.buckets {
		bucketStats := s.buckets[i].GetStats()
		stats.KeyCount += bucketStats.KeyCount
		stats.ExpiringKeys += bucketStats.ExpiringKeys
		stats.StringKeys += bucketStats.StringKeys
		stats.SetKeys += bucketStats.SetKeys
		stats.HashKeys += bucketStats.HashKeys
		stats.ListKeys += bucketStats.ListKeys
		stats.ZSetKeys += bucketStats.ZSetKeys
		stats.BFKeys += bucketStats.BFKeys
		stats.CMSKeys += bucketStats.CMSKeys
	}

	return stats
}

// Close closes the store and AOF file if enabled
func (s *Store) Close() error {
	if s.aof != nil {
		return s.aof.Close()
	}
	return nil
}

// These methods would need to be implemented for full Redis compatibility
// For now, I'll include basic implementations for the most important ones

func (s *Store) SAdd(key string, members ...string) int {
	bucket := s.getBucket(key)
	val, ok := bucket.Get(key)
	if !ok {
		val = Value{Type: SetType, Set: make(map[string]struct{})}
	}

	if val.Type != SetType {
		return 0
	}

	added := 0
	for _, m := range members {
		if _, exists := val.Set[m]; !exists {
			val.Set[m] = struct{}{}
			added++
		}
	}

	val.LastAccess = time.Now().UnixNano()
	bucket.Set(key, val)

	if added > 0 {
		s.logToAOF("SADD", append([]string{key}, members...)...)
	}

	return added
}

func (s *Store) SRem(key string, members ...string) int {
	bucket := s.getBucket(key)
	val, ok := bucket.Get(key)
	if !ok || val.Type != SetType {
		return 0
	}

	removed := 0
	for _, m := range members {
		if _, exists := val.Set[m]; exists {
			delete(val.Set, m)
			removed++
		}
	}

	val.LastAccess = time.Now().UnixNano()
	bucket.Set(key, val)

	if removed > 0 {
		s.logToAOF("SREM", append([]string{key}, members...)...)
	}

	return removed
}

func (s *Store) SCard(key string) int {
	bucket := s.getBucket(key)
	val, ok := bucket.Get(key)
	if !ok || val.Type != SetType {
		return 0
	}

	val.LastAccess = time.Now().UnixNano()
	bucket.Set(key, val)
	return len(val.Set)
}

func (s *Store) SIsMember(key, member string) bool {
	bucket := s.getBucket(key)
	val, ok := bucket.Get(key)
	if !ok || val.Type != SetType {
		return false
	}

	_, exists := val.Set[member]
	val.LastAccess = time.Now().UnixNano()
	bucket.Set(key, val)
	return exists
}

func (s *Store) SUnion(keys ...string) []string {
	result := make(map[string]struct{})
	for _, k := range keys {
		bucket := s.getBucket(k)
		val, ok := bucket.Get(k)
		if !ok || val.Type != SetType {
			continue
		}
		val.LastAccess = time.Now().UnixNano()
		bucket.Set(k, val)
		for m := range val.Set {
			result[m] = struct{}{}
		}
	}

	out := make([]string, 0, len(result))
	for m := range result {
		out = append(out, m)
	}
	return out
}

func (s *Store) SInter(keys ...string) []string {
	if len(keys) == 0 {
		return nil
	}

	// Start with first set
	firstKey := keys[0]
	bucket := s.getBucket(firstKey)
	val, ok := bucket.Get(firstKey)
	if !ok || val.Type != SetType {
		return nil
	}

	val.LastAccess = time.Now().UnixNano()
	bucket.Set(firstKey, val)

	result := make(map[string]struct{})
	for m := range val.Set {
		result[m] = struct{}{}
	}

	// Intersect with remaining sets
	for _, k := range keys[1:] {
		bucket := s.getBucket(k)
		v, ok := bucket.Get(k)
		if !ok || v.Type != SetType {
			return nil
		}
		v.LastAccess = time.Now().UnixNano()
		bucket.Set(k, v)
		for m := range result {
			if _, exists := v.Set[m]; !exists {
				delete(result, m)
			}
		}
	}

	out := make([]string, 0, len(result))
	for m := range result {
		out = append(out, m)
	}
	return out
}

func (s *Store) SDiff(keys ...string) []string {
	if len(keys) == 0 {
		return nil
	}

	firstKey := keys[0]
	bucket := s.getBucket(firstKey)
	val, ok := bucket.Get(firstKey)
	if !ok || val.Type != SetType {
		return nil
	}

	val.LastAccess = time.Now().UnixNano()
	bucket.Set(firstKey, val)

	result := make(map[string]struct{})
	for m := range val.Set {
		result[m] = struct{}{}
	}

	for _, k := range keys[1:] {
		bucket := s.getBucket(k)
		v, ok := bucket.Get(k)
		if !ok || v.Type != SetType {
			continue
		}
		v.LastAccess = time.Now().UnixNano()
		bucket.Set(k, v)
		for m := range v.Set {
			delete(result, m)
		}
	}

	out := make([]string, 0, len(result))
	for m := range result {
		out = append(out, m)
	}
	return out
}

func (s *Store) SPop(key string, count int) []string {
	bucket := s.getBucket(key)
	val, ok := bucket.Get(key)
	if !ok || val.Type != SetType {
		return nil
	}

	n := len(val.Set)
	if n == 0 {
		return nil
	}

	// Flatten to slice
	all := make([]string, 0, n)
	for m := range val.Set {
		all = append(all, m)
	}

	if count <= 0 {
		count = 1
	}
	if count > n {
		count = n
	}

	// Sample without replacement - simple approach
	selected := all[:count]

	// Remove from set
	for _, m := range selected {
		delete(val.Set, m)
	}

	if len(val.Set) == 0 {
		bucket.Delete(key)
	} else {
		val.LastAccess = time.Now().UnixNano()
		bucket.Set(key, val)
	}

	if len(selected) > 0 {
		s.logToAOF("SPOP", append([]string{key}, selected...)...)
	}

	return selected
}

func (s *Store) SRandMember(key string, count int) []string {
	bucket := s.getBucket(key)
	val, ok := bucket.Get(key)
	if !ok || val.Type != SetType {
		return nil
	}

	n := len(val.Set)
	if n == 0 {
		return nil
	}

	// Flatten to slice
	all := make([]string, 0, n)
	for m := range val.Set {
		all = append(all, m)
	}

	if count <= 0 {
		return []string{all[0]} // return single random
	}

	if count > n {
		count = n
	}

	val.LastAccess = time.Now().UnixNano()
	bucket.Set(key, val)
	return all[:count]
}

func (s *Store) SMembers(key string) []string {
	bucket := s.getBucket(key)
	val, ok := bucket.Get(key)
	if !ok || val.Type != SetType {
		return nil
	}

	out := make([]string, 0, len(val.Set))
	for m := range val.Set {
		out = append(out, m)
	}

	// Update last access
	val.LastAccess = time.Now().UnixNano()
	bucket.Set(key, val)

	return out
}

func (s *Store) HSet(key, field, value string) int {
	bucket := s.getBucket(key)
	val, ok := bucket.Get(key)
	if !ok {
		val = Value{Type: HashType, Hash: make(map[string]string)}
	}

	if val.Type != HashType {
		return 0
	}

	_, exists := val.Hash[field]
	val.Hash[field] = value
	val.LastAccess = time.Now().UnixNano()
	bucket.Set(key, val)

	s.logToAOF("HSET", key, field, value)

	if exists {
		return 0
	}
	return 1
}

func (s *Store) HGet(key, field string) (string, bool) {
	bucket := s.getBucket(key)
	val, ok := bucket.Get(key)
	if !ok || val.Type != HashType {
		return "", false
	}

	value, exists := val.Hash[field]

	// Update last access
	val.LastAccess = time.Now().UnixNano()
	bucket.Set(key, val)

	return value, exists
}

func (s *Store) HDel(key string, fields ...string) int {
	bucket := s.getBucket(key)
	val, ok := bucket.Get(key)
	if !ok || val.Type != HashType {
		return 0
	}

	deleted := 0
	for _, f := range fields {
		if _, exists := val.Hash[f]; exists {
			delete(val.Hash, f)
			deleted++
		}
	}

	if deleted > 0 {
		s.logToAOF("HDEL", append([]string{key}, fields...)...)
	}

	if len(val.Hash) == 0 {
		bucket.Delete(key)
	} else {
		val.LastAccess = time.Now().UnixNano()
		bucket.Set(key, val)
	}

	return deleted
}

func (s *Store) HGetAll(key string) map[string]string {
	bucket := s.getBucket(key)
	val, ok := bucket.Get(key)
	if !ok || val.Type != HashType {
		return nil
	}

	result := make(map[string]string, len(val.Hash))
	for k, v := range val.Hash {
		result[k] = v
	}

	// Update last access
	val.LastAccess = time.Now().UnixNano()
	bucket.Set(key, val)

	return result
}

func (s *Store) CMSIncr(key, item string, count uint32) {
	bucket := s.getBucket(key)
	val, ok := bucket.Get(key)
	if !ok {
		val = Value{
			Type: CMSType,
			CMS:  datastuctures.NewCountMinSketch(4, 1000),
		}
	}

	if val.Type != CMSType {
		return
	}

	val.CMS.Incr(item, count)
	val.LastAccess = time.Now().UnixNano()
	bucket.Set(key, val)

	s.logToAOF("CMS.INCR", key, item, fmt.Sprintf("%d", count))
}

func (s *Store) CMSQuery(key, item string) uint32 {
	bucket := s.getBucket(key)
	val, ok := bucket.Get(key)
	if !ok || val.Type != CMSType {
		return 0
	}

	val.LastAccess = time.Now().UnixNano()
	bucket.Set(key, val)
	return val.CMS.Query(item)
}

func (s *Store) BFAdd(key, item string) bool {
	bucket := s.getBucket(key)
	val, ok := bucket.Get(key)
	if !ok || val.Type != BFType {
		bf := datastuctures.NewBloomFilter(1_000_000, 7)
		bf.Add(item)
		bucket.Set(key, Value{
			Type: BFType,
			BF:   bf,
		})

		s.logToAOF("BF.ADD", key, item)
		return true
	}

	if val.Type != BFType {
		return false
	}

	val.BF.Add(item)
	val.LastAccess = time.Now().UnixNano()
	bucket.Set(key, val)

	s.logToAOF("BF.ADD", key, item)
	return true
}

func (s *Store) BFExists(key, item string) bool {
	bucket := s.getBucket(key)
	val, ok := bucket.Get(key)
	if !ok || val.Type != BFType {
		return false
	}

	val.LastAccess = time.Now().UnixNano()
	bucket.Set(key, val)
	return val.BF.Exists(item)
}

// Add placeholder implementations for other data structures
// These can be expanded based on your needs

func (s *Store) LPush(key string, values ...string) int {
	bucket := s.getBucket(key)
	val, ok := bucket.Get(key)
	if !ok {
		val = Value{Type: ListType, List: []string{}}
	}

	if val.Type != ListType {
		return -1
	}

	// Prepend values
	for i := len(values) - 1; i >= 0; i-- {
		val.List = append([]string{values[i]}, val.List...)
	}

	val.LastAccess = time.Now().UnixNano()
	bucket.Set(key, val)

	s.logToAOF("LPUSH", append([]string{key}, values...)...)
	return len(val.List)
}

func (s *Store) RPush(key string, values ...string) int {
	bucket := s.getBucket(key)
	val, ok := bucket.Get(key)
	if !ok {
		val = Value{Type: ListType, List: []string{}}
	}

	if val.Type != ListType {
		return -1
	}

	val.List = append(val.List, values...)
	val.LastAccess = time.Now().UnixNano()
	bucket.Set(key, val)

	s.logToAOF("RPUSH", append([]string{key}, values...)...)
	return len(val.List)
}

func (s *Store) LPop(key string) (string, bool) {
	bucket := s.getBucket(key)
	val, ok := bucket.Get(key)
	if !ok || val.Type != ListType || len(val.List) == 0 {
		return "", false
	}

	item := val.List[0]
	val.List = val.List[1:]
	val.LastAccess = time.Now().UnixNano()
	bucket.Set(key, val)

	s.logToAOF("LPOP", key)
	return item, true
}

func (s *Store) RPop(key string) (string, bool) {
	bucket := s.getBucket(key)
	val, ok := bucket.Get(key)
	if !ok || val.Type != ListType || len(val.List) == 0 {
		return "", false
	}

	idx := len(val.List) - 1
	item := val.List[idx]
	val.List = val.List[:idx]
	val.LastAccess = time.Now().UnixNano()
	bucket.Set(key, val)

	s.logToAOF("RPOP", key)
	return item, true
}

func (s *Store) LLen(key string) int {
	bucket := s.getBucket(key)
	val, ok := bucket.Get(key)
	if !ok || val.Type != ListType {
		return 0
	}

	val.LastAccess = time.Now().UnixNano()
	bucket.Set(key, val)
	return len(val.List)
}

func (s *Store) LRange(key string, start, stop int) []string {
	bucket := s.getBucket(key)
	val, ok := bucket.Get(key)
	if !ok || val.Type != ListType {
		return nil
	}

	n := len(val.List)
	if n == 0 {
		return nil
	}

	// Handle negative indices
	if start < 0 {
		start = n + start
	}
	if stop < 0 {
		stop = n + stop
	}

	// Clamp to bounds
	if start < 0 {
		start = 0
	}
	if stop >= n {
		stop = n - 1
	}
	if start > stop || start >= n {
		return nil
	}

	val.LastAccess = time.Now().UnixNano()
	bucket.Set(key, val)
	return val.List[start : stop+1]
}

// Basic implementation of other data structures
func (s *Store) ZAdd(key string, members map[string]float64) int {
	bucket := s.getBucket(key)
	val, ok := bucket.Get(key)
	if !ok {
		val = Value{Type: ZSetType, ZSet: make(map[string]float64)}
	}

	if val.Type != ZSetType {
		return -1
	}

	added := 0
	for member, score := range members {
		if _, exists := val.ZSet[member]; !exists {
			added++
		}
		val.ZSet[member] = score
	}

	val.LastAccess = time.Now().UnixNano()
	bucket.Set(key, val)

	for member, score := range members {
		s.logToAOF("ZADD", key, fmt.Sprintf("%f", score), member)
	}

	return added
}

func (s *Store) ZScore(key, member string) (float64, bool) {
	bucket := s.getBucket(key)
	val, ok := bucket.Get(key)
	if !ok || val.Type != ZSetType {
		return 0, false
	}

	score, exists := val.ZSet[member]
	val.LastAccess = time.Now().UnixNano()
	bucket.Set(key, val)
	return score, exists
}

func (s *Store) ZCard(key string) int {
	bucket := s.getBucket(key)
	val, ok := bucket.Get(key)
	if !ok || val.Type != ZSetType {
		return 0
	}

	val.LastAccess = time.Now().UnixNano()
	bucket.Set(key, val)
	return len(val.ZSet)
}

func (s *Store) ZRange(key string, start, stop int, withScores bool) []string {
	bucket := s.getBucket(key)
	val, ok := bucket.Get(key)
	if !ok || val.Type != ZSetType {
		return nil
	}

	// Sort members by score
	type pair struct {
		member string
		score  float64
	}
	pairs := make([]pair, 0, len(val.ZSet))
	for m, score := range val.ZSet {
		pairs = append(pairs, pair{m, score})
	}

	sort.Slice(pairs, func(i, j int) bool {
		if pairs[i].score == pairs[j].score {
			return pairs[i].member < pairs[j].member
		}
		return pairs[i].score < pairs[j].score
	})

	n := len(pairs)
	if n == 0 {
		return nil
	}

	// Handle negative indices
	if start < 0 {
		start = n + start
	}
	if stop < 0 {
		stop = n + stop
	}

	// Clamp to bounds
	if start < 0 {
		start = 0
	}
	if stop >= n {
		stop = n - 1
	}
	if start > stop || start >= n {
		return nil
	}

	result := make([]string, 0, stop-start+1)
	for _, p := range pairs[start : stop+1] {
		result = append(result, p.member)
		if withScores {
			result = append(result, fmt.Sprintf("%f", p.score))
		}
	}

	val.LastAccess = time.Now().UnixNano()
	bucket.Set(key, val)
	return result
}

// Persistence methods - AOF loading implementation
func (s *Store) LoadFromAOF() error {
	if s.aof == nil {
		return nil
	}

	log.Printf("Loading AOF data from: %s", s.aof.path)
	commands, err := s.aof.LoadCommands()
	if err != nil {
		return fmt.Errorf("failed to load AOF commands: %v", err)
	}

	log.Printf("Replaying %d AOF commands", len(commands))
	replayCount := 0

	// Replay each command to restore data
	for _, cmd := range commands {
		if len(cmd) == 0 {
			continue
		}

		cmdName := strings.ToUpper(cmd[0])

		switch cmdName {
		case "SET":
			if len(cmd) >= 3 {
				key := cmd[1]
				value := []byte(cmd[2])

				// Create Value struct
				val := Value{
					Type:       StringType,
					Data:       value,
					Expiration: 0, // No expiration from AOF replay
					LastAccess: time.Now().UnixNano(),
				}

				// Set directly to bucket without logging to AOF again
				bucket := s.getBucket(key)
				bucket.Set(key, val)
				replayCount++
			}

		case "HSET":
			if len(cmd) >= 4 {
				key := cmd[1]
				field := cmd[2]
				value := cmd[3]

				bucket := s.getBucket(key)
				bucket.mu.Lock()

				// Get or create hash
				val, exists := bucket.data[key]
				if !exists || val.Type != HashType {
					val = Value{
						Type:       HashType,
						Hash:       make(map[string]string),
						Expiration: 0,
						LastAccess: time.Now().UnixNano(),
					}
				}

				// Set field in hash
				val.Hash[field] = value
				val.LastAccess = time.Now().UnixNano()
				bucket.data[key] = val

				bucket.mu.Unlock()
				replayCount++
			}

		case "SADD":
			if len(cmd) >= 3 {
				key := cmd[1]
				bucket := s.getBucket(key)
				bucket.mu.Lock()

				// Get or create set
				val, exists := bucket.data[key]
				if !exists || val.Type != SetType {
					val = Value{
						Type:       SetType,
						Set:        make(map[string]struct{}),
						Expiration: 0,
						LastAccess: time.Now().UnixNano(),
					}
				}

				// Add all members to set (SADD can add multiple members)
				for i := 2; i < len(cmd); i++ {
					val.Set[cmd[i]] = struct{}{}
				}
				val.LastAccess = time.Now().UnixNano()
				bucket.data[key] = val

				bucket.mu.Unlock()
				replayCount++
			}

		case "DEL":
			if len(cmd) >= 2 {
				key := cmd[1]
				bucket := s.getBucket(key)
				bucket.Delete(key)
				replayCount++
			}

		case "LPUSH", "RPUSH":
			if len(cmd) >= 3 {
				key := cmd[1]
				bucket := s.getBucket(key)
				bucket.mu.Lock()

				// Get or create list
				val, exists := bucket.data[key]
				if !exists || val.Type != ListType {
					val = Value{
						Type:       ListType,
						List:       []string{},
						Expiration: 0,
						LastAccess: time.Now().UnixNano(),
					}
				}

				// Add elements to list
				for i := 2; i < len(cmd); i++ {
					if cmdName == "LPUSH" {
						// Prepend to front of list
						val.List = append([]string{cmd[i]}, val.List...)
					} else {
						// Append to end of list
						val.List = append(val.List, cmd[i])
					}
				}
				val.LastAccess = time.Now().UnixNano()
				bucket.data[key] = val

				bucket.mu.Unlock()
				replayCount++
			}

		default:
			// Skip unknown commands (they'll be logged but not counted as failures)
			continue
		}
	}

	log.Printf("Successfully replayed %d/%d AOF commands", replayCount, len(commands))
	return nil
}

func (s *Store) LoadFromRDB() error {
	if s.rdb == nil {
		return nil
	}
	// TODO: Implement RDB loading for sharded store
	return nil
}

func (s *Store) LoadFromPersistence() error {
	if err := s.LoadFromRDB(); err != nil {
		log.Printf("WARNING: Failed to load RDB: %v", err)
	}
	if err := s.LoadFromAOF(); err != nil {
		log.Printf("WARNING: Failed to load AOF: %v", err)
	}
	return nil
}

// GetAllData returns all data from all buckets - used for RDB serialization
func (s *Store) GetAllData() map[string]Value {
	allData := make(map[string]Value)

	for i := range s.buckets {
		bucket := s.buckets[i]
		bucket.mu.RLock()
		for key, value := range bucket.data {
			// Check if not expired
			if expTime, hasExp := bucket.ttl[key]; !hasExp || time.Now().Before(expTime) {
				allData[key] = value
			}
		}
		bucket.mu.RUnlock()
	}

	return allData
}

// GetAllTTL returns all TTL data from all buckets - used for RDB serialization
func (s *Store) GetAllTTL() map[string]time.Time {
	allTTL := make(map[string]time.Time)

	for i := range s.buckets {
		bucket := s.buckets[i]
		bucket.mu.RLock()
		for key, expTime := range bucket.ttl {
			if time.Now().Before(expTime) {
				allTTL[key] = expTime
			}
		}
		bucket.mu.RUnlock()
	}

	return allTTL
}

// SetDataDirect sets data directly in the appropriate bucket - used for RDB loading
func (s *Store) SetDataDirect(key string, value Value, expTime *time.Time) {
	bucket := s.getBucket(key)
	bucket.mu.Lock()
	bucket.data[key] = value
	if expTime != nil {
		bucket.ttl[key] = *expTime
	}
	bucket.mu.Unlock()
}

func (s *Store) ZRank(key, member string) (int, bool) {
	bucket := s.getBucket(key)
	val, ok := bucket.Get(key)
	if !ok || val.Type != ZSetType {
		return 0, false
	}

	// Sort members by score
	type pair struct {
		member string
		score  float64
	}
	pairs := make([]pair, 0, len(val.ZSet))
	for m, score := range val.ZSet {
		pairs = append(pairs, pair{m, score})
	}

	sort.Slice(pairs, func(i, j int) bool {
		if pairs[i].score == pairs[j].score {
			return pairs[i].member < pairs[j].member
		}
		return pairs[i].score < pairs[j].score
	})

	// Find rank
	for rank, p := range pairs {
		if p.member == member {
			val.LastAccess = time.Now().UnixNano()
			bucket.Set(key, val)
			return rank, true
		}
	}

	return 0, false
}

func (s *Store) SaveRDBSnapshot() error {
	if s.rdb == nil {
		return fmt.Errorf("RDB not enabled for this store")
	}
	return s.rdb.Save(s)
}
