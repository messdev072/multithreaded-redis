package store

import (
	"sync"
	"time"
)

type Value struct {
	Data []byte
}

type Store struct {
	mu   sync.RWMutex
	data map[string]Value
	ttl  map[string]time.Time
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

	s.data[key] = Value{Data: val}
	if expire > 0 {
		s.ttl[key] = time.Now().Add(expire)
	} else {
		delete(s.ttl, key)
	}
}

func (s *Store) Get(key string) ([]byte, bool) {
	s.mu.RLock()
	exp, hasTTL := s.ttl[key]
	s.mu.RUnlock()

	if hasTTL && time.Now().After(exp) {
		// expired, remove lazily
		s.mu.Lock()
		delete(s.data, key)
		delete(s.ttl, key)
		s.mu.Unlock()
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
	if ttl < 0 {
		return -2
	}
	return int64(ttl.Seconds())
}

func (s *Store) StartCleaner(sampleSize int, interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for range ticker.C {
			s.mu.Lock()
			for k, exp := range s.ttl {
				if time.Now().After(exp) {
					delete(s.data, k)
					delete(s.ttl, k)
				}
			}
			s.mu.Unlock()
		}
	}()
}
