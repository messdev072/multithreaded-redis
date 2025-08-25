package store

import (
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"time"
)

type ValueType int

const (
	StringType ValueType = iota
	SetType
	HashType
	CMSType
	ListType
	ZSetType
)

type CountMinSketch struct {
	depth     int
	width     int
	table     [][]uint32
	hashFuncs []func(string) uint32
}

type Value struct {
	Type ValueType
	Data []byte              // for strings
	Set  map[string]struct{} // for sets
	Hash map[string]string
	CMS  *CountMinSketch // for Count-Min Sketch
	List []string
	ZSet map[string]float64
}

type Store struct {
	mu      sync.RWMutex
	data    map[string]Value
	ttl     map[string]time.Time
	ttlKeys []string // for random sampling
}

func (s *Store) expired(key string) bool {
	exp, ok := s.ttl[key]
	if !ok {
		return false
	}
	if time.Now().After(exp) {
		s.mu.Lock()
		delete(s.data, key)
		delete(s.ttl, key)
		s.mu.Unlock()
		return true
	}
	return false
}

func NewStore() *Store {
	return &Store{
		data: make(map[string]Value),
		ttl:  make(map[string]time.Time),
	}
}

func (s *Store) Set(key string, val []byte, expire time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.expired(key)

	s.data[key] = Value{Data: val}
	if expire > 0 {
		if _, exists := s.ttl[key]; !exists {
			s.ttlKeys = append(s.ttlKeys, key) //track new TTL key
		}
		s.ttl[key] = time.Now().Add(expire)
	} else {
		delete(s.ttl, key)
	}
}

func (s *Store) Get(key string) ([]byte, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.expired(key) {
		return nil, false
	}

	s.mu.RLock()
	val, ok := s.data[key]
	s.mu.RUnlock()
	if !ok {
		return nil, false
	}
	return val.Data, true
}

func (s *Store) Delete(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	_, exists := s.data[key]
	if exists {
		delete(s.data, key)
		delete(s.ttl, key)
		return true
	}

	return exists
}

func (s *Store) TTL(key string) int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()

	exp, ok := s.ttl[key]
	if !ok {
		if _, exists := s.data[key]; exists {
			return -1 // no expiration
		}
		return -2 // key does not exist
	}

	ttl := time.Until(exp)
	if ttl <= 0 {
		return -2
	}
	return int64(ttl.Seconds())
}

func (s *Store) PTTL(key string) int64 {
	s.mu.Lock()
	defer s.mu.RUnlock()

	exp, ok := s.ttl[key]
	if !ok {
		if _, exists := s.data[key]; exists {
			return -1
		}
		return -2
	}

	ttl := time.Until(exp)
	if ttl <= 0 {
		return -2
	}
	return ttl.Microseconds()
}

func (s *Store) StartCleaner(sampleSize int, interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for range ticker.C {
			for {
				expired := s.expireCycle(sampleSize)
				if expired < sampleSize/4 { // if less than 25% expired, break to avoid busy loop
					break
				}
			}
		}
	}()
}

func (s *Store) expireCycle(sampleSize int) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.ttlKeys) == 0 {
		return 0
	}

	expiredCount := 0
	now := time.Now()

	for i := 0; i < sampleSize; i++ {
		// pick random key
		idx := rand.Intn(len(s.ttlKeys))
		k := s.ttlKeys[idx]

		exp, ok := s.ttl[k]
		if !ok {
			continue
		}
		if now.After(exp) {
			delete(s.data, k)
			delete(s.ttl, k)
			expiredCount++
		}
	}
	return expiredCount
}

func (s *Store) SAdd(key string, members ...string) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.expired(key) {
		// expired key is like it never existed
	}

	val, ok := s.data[key]
	if !ok {
		val = Value{Type: SetType, Set: make(map[string]struct{})}
		s.data[key] = val
	}

	if val.Type != SetType {
		return 0 // in Redis, this would be a WRONGTYPE error (we’ll handle in dispatcher)
	}

	added := 0
	for _, m := range members {
		if _, exists := val.Set[m]; !exists {
			val.Set[m] = struct{}{}
			added++
		}
	}
	s.data[key] = val
	return added
}

func (s *Store) SRem(key string, members ...string) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.expired(key) {
		return 0
	}

	val, ok := s.data[key]
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
	return removed
}

// Return all members.
func (s *Store) SMembers(key string) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.expired(key) {
		return nil
	}

	val, ok := s.data[key]
	if !ok || val.Type != SetType {
		return nil
	}

	out := make([]string, 0, len(val.Set))
	for m := range val.Set {
		out = append(out, m)
	}
	return out
}

// Cardinality (count of set members)
func (s *Store) SCard(key string) int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.expired(key) {
		return 0
	}

	val, ok := s.data[key]
	if !ok || val.Type != SetType {
		return 0
	}

	return len(val.Set)
}

func (s *Store) SIsMember(key, member string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.expired(key) {
		return false
	}

	val, ok := s.data[key]
	if !ok || val.Type != SetType {
		return false
	}

	_, exists := val.Set[member]
	return exists
}

// SUnion returns the union of multiple sets
func (s *Store) SUnion(keys ...string) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make(map[string]struct{})
	for _, k := range keys {
		if s.expired(k) {
			continue
		}
		val, ok := s.data[k]
		if !ok || val.Type != SetType {
			continue
		}
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

// SInter returns the intersection of multiple sets
func (s *Store) SInter(keys ...string) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if len(keys) == 0 {
		return nil
	}

	//Start with 1st set
	firstKey := keys[0]
	if s.expired(firstKey) {
		return nil
	}
	val, ok := s.data[firstKey]
	if !ok || val.Type != SetType {
		return nil
	}

	result := make(map[string]struct{})
	for m := range val.Set {
		result[m] = struct{}{}
	}

	//Intersert with remaining sets
	for _, k := range keys[1:] {
		if s.expired(k) {
			return nil
		}
		val, ok := s.data[k]
		if !ok || val.Type != SetType {
			return nil
		}
		for m := range result {
			if _, exists := val.Set[m]; !exists {
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

// Difference (elements in first set but not in others).
func (s *Store) SDiff(keys ...string) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if len(keys) == 0 {
		return nil
	}

	firstKey := keys[0]
	if s.expired(firstKey) {
		return nil
	}
	val, ok := s.data[firstKey]
	if !ok || val.Type != SetType {
		return nil
	}

	result := make(map[string]struct{})
	for m := range val.Set {
		result[m] = struct{}{}
	}

	for _, k := range keys[1:] {
		if s.expired(k) {
			continue
		}
		val, ok := s.data[k]
		if !ok || val.Type != SetType {
			continue
		}
		for m := range val.Set {
			delete(result, m)
		}
	}

	out := make([]string, 0, len(result))
	for m := range result {
		out = append(out, m)
	}
	return out
}

// Return one or more random ellements
func (s *Store) SRandMember(key string, count int) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.expired(key) {
		return nil
	}
	val, ok := s.data[key]
	if !ok || val.Type != SetType {
		return nil
	}

	n := len(val.Set)
	if n == 0 {
		return nil
	}

	//Flatten to slice
	all := make([]string, 0, n)
	for m := range val.Set {
		all = append(all, m)
	}

	if count <= 0 {
		// return single random
		return []string{all[rand.Intn(n)]}
	}

	//Cap count
	if count > n {
		count = n
	}

	//Sample without replacement
	rand.Shuffle(n, func(i, j int) {
		all[i], all[j] = all[j], all[i]
	})
	return all[:count]
}

// Removes the chosen elements
func (s *Store) SPop(key string, count int) []string {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.expired(key) {
		return nil
	}
	val, ok := s.data[key]
	if !ok || val.Type != SetType {
		return nil
	}

	n := len(val.Set)
	if n == 0 {
		return nil
	}

	//Flatten to slice
	all := make([]string, 0, n)
	for m := range val.Set {
		all = append(all, m)
	}

	if count <= 0 {
		// default: one element
		count = 1
	}
	if count > n {
		count = n
	}

	// Shuffle and pick
	rand.Shuffle(n, func(i, j int) { all[i], all[j] = all[j], all[i] })
	selected := all[:count]

	// Remove from set
	for _, m := range selected {
		delete(val.Set, m)
	}

	// If empty after removal, delete key entirely
	if len(val.Set) == 0 {
		delete(s.data, key)
	}

	return selected
}

// HSET key field value
func (s *Store) HSet(key, field, value string) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.expired(key) {
		delete(s.data, key)
	}

	val, ok := s.data[key]
	if !ok {
		val = Value{Type: HashType, Hash: make(map[string]string)}
		s.data[key] = val
	}
	if val.Type != HashType {
		return 0
	}

	_, exists := val.Hash[field]
	val.Hash[field] = value
	if exists {
		return 0
	}
	return 1
}

// HGET key field
func (s *Store) HGet(key, field string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.expired(key) {
		delete(s.data, key)
		return "", false
	}

	val, ok := s.data[key]
	if !ok || val.Type != HashType {
		return "", false
	}
	value, ok := val.Hash[field]
	return value, ok
}

// HDEL key field [field...]
func (s *Store) HDel(key string, fields ...string) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.expired(key) {
		delete(s.data, key)
		return 0
	}

	val, ok := s.data[key]
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

	if len(val.Hash) == 0 {
		delete(s.data, key)
	}
	return deleted
}

// HGETALL key
func (s *Store) HGetAll(key string) map[string]string {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.expired(key) {
		delete(s.data, key)
		return nil
	}

	val, ok := s.data[key]
	if !ok || val.Type != HashType {
		return nil
	}

	result := make(map[string]string, len(val.Hash))
	for k, val := range val.Hash {
		result[k] = val
	}
	return result
}

// CMS.INCR key item count
func (s *Store) CMSIncr(key, item string, count uint32) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.expired(key) {
		delete(s.data, key)
	}

	val, ok := s.data[key]
	if !ok {
		val = Value{Type: CMSType, CMS: NewCountMinSketch(4, 1000)}
	}
	if val.Type != CMSType {
		return // in Redis, this would be a WRONGTYPE error (we’ll handle in dispatcher)
	}

	val.CMS.Incr(item, count)
	s.data[key] = val
}

// CMS.QUERY key item
func (s *Store) CMSQuery(key, item string) uint32 {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.expired(key) {
		delete(s.data, key)
		return 0
	}

	val, ok := s.data[key]
	if !ok || val.Type != CMSType {
		return 0
	}

	return val.CMS.Query(item)
}

// LPUSH
func (s *Store) LPush(key string, values ...string) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	val, ok := s.data[key]
	if !ok {
		val = Value{Type: ListType, List: []string{}}
		s.data[key] = val
	}
	if val.Type != ListType {
		return -1
	}

	// Prepend (reverse order for multiple push)
	for i := len(values) - 1; i >= 0; i-- {
		val.List = append([]string{values[i]}, val.List...)
	}
	s.data[key] = val
	return len(val.List)
}

// RPUSH
func (s *Store) RPush(key string, values ...string) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	val, ok := s.data[key]
	if !ok {
		val = Value{Type: ListType, List: []string{}}
		s.data[key] = val
	}
	if val.Type != ListType {
		return -1
	}

	val.List = append(val.List, values...)
	s.data[key] = val
	return len(val.List)
}

// LPOP
func (s *Store) LPop(key string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.expired(key) {
		delete(s.data, key)
		return "", false
	}

	val, ok := s.data[key]
	if !ok || val.Type != ListType || len(val.List) == 0 {
		return "", false
	}

	item := val.List[0]
	val.List = val.List[1:]
	s.data[key] = val
	return item, true
}

// RPOP
func (s *Store) RPop(key string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.expired(key) {
		delete(s.data, key)
		return "", false
	}

	val, ok := s.data[key]
	if !ok || val.Type != ListType || len(val.List) == 0 {
		return "", false
	}

	idx := len(val.List) - 1
	item := val.List[idx]
	val.List = val.List[:idx]
	s.data[key] = val
	return item, true
}

// LLEN
func (s *Store) LLen(key string) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.expired(key) {
		delete(s.data, key)
		return 0
	}

	val, ok := s.data[key]
	if !ok || val.Type != ListType {
		return 0
	}
	return len(val.List)
}

// LRANGE
func (s *Store) LRange(key string, start, stop int) []string {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.expired(key) {
		delete(s.data, key)
		return nil
	}

	val, ok := s.data[key]
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

	return val.List[start : stop+1]
}

// ZADD
func (s *Store) ZAdd(key string, members map[string]float64) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	val, ok := s.data[key]
	if !ok {
		val = Value{Type: ZSetType, ZSet: make(map[string]float64)}
		s.data[key] = val
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
	s.data[key] = val
	return added
}

// ZSCORE
func (s *Store) ZScore(key, member string) (float64, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.expired(key) {
		delete(s.data, key)
		return 0, false
	}

	val, ok := s.data[key]
	if !ok || val.Type != ZSetType {
		return 0, false
	}

	score, exists := val.ZSet[member]
	return score, exists
}

// ZCARD
func (s *Store) ZCard(key string) int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.expired(key) {
		delete(s.data, key)
		return 0
	}

	val, ok := s.data[key]
	if !ok || val.Type != ZSetType {
		return 0
	}

	return len(val.ZSet)
}

// ZRANK
func (s *Store) ZRank(key, member string) (int, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.expired(key) {
		delete(s.data, key)
		return 0, false
	}

	val, ok := s.data[key]
	if !ok || val.Type != ZSetType {
		return 0, false
	}

	// sort menbers by score
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
			return pairs[i].member < pairs[j].member // tie-breaker: lex order
		}
		return pairs[i].score < pairs[j].score
	})
	// find rank
	for rank, p := range pairs {
		if p.member == member {
			return rank, true
		}
	}
	return 0, false
}

// ZRANGE
func (s *Store) ZRange(key string, start, stop int, withScores bool) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.expired(key) {
		delete(s.data, key)
		return nil
	}

	val, ok := s.data[key]
	if !ok || val.Type != ZSetType {
		return nil
	}

	// sort menbers by score
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
			return pairs[i].member < pairs[j].member // tie-breaker: lex order
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
	return result
}
