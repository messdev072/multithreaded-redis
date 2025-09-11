package store

import (
	"sync"
	"time"
)

// Bucket represents a single sharded bucket with its own lock
type Bucket struct {
	mu   sync.RWMutex
	data map[string]Value
	ttl  map[string]time.Time
}

// NewBucket creates a new bucket with initialized maps
func NewBucket() *Bucket {
	return &Bucket{
		data: make(map[string]Value),
		ttl:  make(map[string]time.Time),
	}
}

// Get retrieves a value from the bucket
func (b *Bucket) Get(key string) (Value, bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	// Check TTL first
	if expTime, exists := b.ttl[key]; exists && time.Now().After(expTime) {
		// Key expired, clean it up
		delete(b.data, key)
		delete(b.ttl, key)
		return Value{}, false
	}

	val, exists := b.data[key]
	return val, exists
}

// Set stores a value in the bucket
func (b *Bucket) Set(key string, value Value) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.data[key] = value
	if value.Expiration > 0 {
		b.ttl[key] = time.Unix(value.Expiration, 0)
	} else {
		delete(b.ttl, key)
	}
}

// Delete removes a key from the bucket
func (b *Bucket) Delete(key string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	_, exists := b.data[key]
	if exists {
		delete(b.data, key)
		delete(b.ttl, key)
	}
	return exists
}

// Exists checks if a key exists in the bucket
func (b *Bucket) Exists(key string) bool {
	b.mu.RLock()
	defer b.mu.RUnlock()

	// Check TTL first
	if expTime, exists := b.ttl[key]; exists && time.Now().After(expTime) {
		return false
	}

	_, exists := b.data[key]
	return exists
}

// Keys returns all non-expired keys in the bucket
func (b *Bucket) Keys() []string {
	b.mu.RLock()
	defer b.mu.RUnlock()

	now := time.Now()
	keys := GlobalPoolManager.GetStringSlice()

	for key := range b.data {
		if expTime, hasExp := b.ttl[key]; !hasExp || now.Before(expTime) {
			keys = append(keys, key)
		}
	}

	// Return a copy since we need to return the pooled slice
	result := make([]string, len(keys))
	copy(result, keys)
	GlobalPoolManager.PutStringSlice(keys)

	return result
}

// Size returns the number of non-expired keys in the bucket
func (b *Bucket) Size() int {
	b.mu.RLock()
	defer b.mu.RUnlock()

	now := time.Now()
	count := 0

	for key := range b.data {
		if expTime, hasExp := b.ttl[key]; !hasExp || now.Before(expTime) {
			count++
		}
	}

	return count
}

// CleanupExpired removes all expired keys from the bucket
func (b *Bucket) CleanupExpired() int {
	b.mu.Lock()
	defer b.mu.Unlock()

	now := time.Now()
	cleaned := 0

	for key, expTime := range b.ttl {
		if now.After(expTime) {
			delete(b.data, key)
			delete(b.ttl, key)
			cleaned++
		}
	}

	return cleaned
}

// GetStats returns statistics for this bucket
func (b *Bucket) GetStats() BucketStats {
	b.mu.RLock()
	defer b.mu.RUnlock()

	stats := BucketStats{}
	now := time.Now()

	for key, value := range b.data {
		// Skip expired keys
		if expTime, hasExp := b.ttl[key]; hasExp && now.After(expTime) {
			continue
		}

		stats.KeyCount++
		if _, hasExp := b.ttl[key]; hasExp {
			stats.ExpiringKeys++
		}

		// Count by type
		switch value.Type {
		case StringType:
			stats.StringKeys++
		case SetType:
			stats.SetKeys++
		case HashType:
			stats.HashKeys++
		case ListType:
			stats.ListKeys++
		case ZSetType:
			stats.ZSetKeys++
		case BFType:
			stats.BFKeys++
		case CMSType:
			stats.CMSKeys++
		}
	}

	return stats
}

// BucketStats contains statistics for a single bucket
type BucketStats struct {
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
