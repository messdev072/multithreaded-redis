package store

import (
	"sync"
	"time"
)

// PoolManager manages various object pools to reduce GC pressure
type PoolManager struct {
	// Map pools for different common sizes
	smallMapPool  sync.Pool // for maps with expected size < 10
	mediumMapPool sync.Pool // for maps with expected size 10-100
	largeMapPool  sync.Pool // for maps with expected size > 100

	// String slice pools
	stringSlicePool sync.Pool

	// Byte slice pools by size
	smallBytePool  sync.Pool // 1KB buffers
	mediumBytePool sync.Pool // 4KB buffers
	largeBytePool  sync.Pool // 16KB buffers

	// TTL entry pools
	ttlEntryPool sync.Pool

	// Command argument pools
	argsPool sync.Pool

	// PubSub message pools
	pubsubMsgPool sync.Pool

	// Cleanup ticker for periodic pool maintenance
	cleanupTicker *time.Ticker
	stopCleanup   chan struct{}
}

// NewPoolManager creates a new pool manager with pre-configured pools
func NewPoolManager() *PoolManager {
	pm := &PoolManager{
		stopCleanup: make(chan struct{}),
	}

	// Initialize map pools
	pm.smallMapPool.New = func() interface{} {
		return make(map[string]interface{}, 8)
	}
	pm.mediumMapPool.New = func() interface{} {
		return make(map[string]interface{}, 64)
	}
	pm.largeMapPool.New = func() interface{} {
		return make(map[string]interface{}, 256)
	}

	// Initialize string slice pool
	pm.stringSlicePool.New = func() interface{} {
		s := make([]string, 0, 16)
		return &s
	}

	// Initialize byte slice pools
	pm.smallBytePool.New = func() interface{} {
		s := make([]byte, 0, 1024) // 1KB
		return &s
	}
	pm.mediumBytePool.New = func() interface{} {
		s := make([]byte, 0, 4096) // 4KB
		return &s
	}
	pm.largeBytePool.New = func() interface{} {
		s := make([]byte, 0, 16384) // 16KB
		return &s
	}

	// Initialize TTL entry pool
	pm.ttlEntryPool.New = func() interface{} {
		return &TTLEntry{}
	}

	// Initialize command args pool
	pm.argsPool.New = func() interface{} {
		s := make([]string, 0, 8)
		return &s
	}

	// Initialize PubSub message pool
	pm.pubsubMsgPool.New = func() interface{} {
		return &PubSubMessage{}
	}

	// Start cleanup routine
	pm.startCleanup()

	return pm
}

// TTLEntry represents a TTL entry for pooling
type TTLEntry struct {
	Key    string
	Expiry time.Time
}

// Reset resets a TTL entry for reuse
func (e *TTLEntry) Reset() {
	e.Key = ""
	e.Expiry = time.Time{}
}

// GetMap returns a map from the appropriate pool based on expected size
func (pm *PoolManager) GetMap(expectedSize int) map[string]interface{} {
	var m map[string]interface{}

	switch {
	case expectedSize < 10:
		m = pm.smallMapPool.Get().(map[string]interface{})
	case expectedSize < 100:
		m = pm.mediumMapPool.Get().(map[string]interface{})
	default:
		m = pm.largeMapPool.Get().(map[string]interface{})
	}

	// Clear the map
	for k := range m {
		delete(m, k)
	}

	return m
}

// PutMap returns a map to the appropriate pool
func (pm *PoolManager) PutMap(m map[string]interface{}) {
	if m == nil {
		return
	}

	size := len(m)

	// Only pool if not too large to avoid memory waste
	if size > 1000 {
		return
	}

	// Use size as a proxy for determining which pool to use
	// since we can't check capacity of maps
	switch {
	case size <= 8: // likely came from small pool
		pm.smallMapPool.Put(m)
	case size <= 64: // likely came from medium pool
		pm.mediumMapPool.Put(m)
	default:
		pm.largeMapPool.Put(m)
	}
}

// GetStringSlice returns a string slice from the pool
func (pm *PoolManager) GetStringSlice() []string {
	slicePtr := pm.stringSlicePool.Get().(*[]string)
	slice := *slicePtr
	return slice[:0] // reset length but keep capacity
}

// PutStringSlice returns a string slice to the pool
func (pm *PoolManager) PutStringSlice(slice []string) {
	if slice == nil || cap(slice) > 1000 {
		return // don't pool if too large
	}

	// Clear the slice
	for i := range slice {
		slice[i] = ""
	}

	slice = slice[:0]
	pm.stringSlicePool.Put(&slice)
}

// GetByteSlice returns a byte slice from the appropriate pool
func (pm *PoolManager) GetByteSlice(size int) []byte {
	switch {
	case size <= 1024:
		slicePtr := pm.smallBytePool.Get().(*[]byte)
		slice := *slicePtr
		if cap(slice) >= size {
			return slice[:size]
		}
		pm.smallBytePool.Put(slicePtr) // put back, wrong size
		return make([]byte, size)

	case size <= 4096:
		slicePtr := pm.mediumBytePool.Get().(*[]byte)
		slice := *slicePtr
		if cap(slice) >= size {
			return slice[:size]
		}
		pm.mediumBytePool.Put(slicePtr) // put back, wrong size
		return make([]byte, size)

	case size <= 16384:
		slicePtr := pm.largeBytePool.Get().(*[]byte)
		slice := *slicePtr
		if cap(slice) >= size {
			return slice[:size]
		}
		pm.largeBytePool.Put(slicePtr) // put back, wrong size
		return make([]byte, size)

	default:
		return make([]byte, size) // too large to pool
	}
}

// PutByteSlice returns a byte slice to the appropriate pool
func (pm *PoolManager) PutByteSlice(slice []byte) {
	if slice == nil {
		return
	}

	capacity := cap(slice)

	switch {
	case capacity <= 1024:
		slice = slice[:0]
		pm.smallBytePool.Put(&slice)
	case capacity <= 4096:
		slice = slice[:0]
		pm.mediumBytePool.Put(&slice)
	case capacity <= 16384:
		slice = slice[:0]
		pm.largeBytePool.Put(&slice)
		// don't pool if too large
	}
}

// GetTTLEntry returns a TTL entry from the pool
func (pm *PoolManager) GetTTLEntry() *TTLEntry {
	entry := pm.ttlEntryPool.Get().(*TTLEntry)
	entry.Reset()
	return entry
}

// PutTTLEntry returns a TTL entry to the pool
func (pm *PoolManager) PutTTLEntry(entry *TTLEntry) {
	if entry != nil {
		pm.ttlEntryPool.Put(entry)
	}
}

// GetArgs returns a string slice for command arguments
func (pm *PoolManager) GetArgs() []string {
	argsPtr := pm.argsPool.Get().(*[]string)
	args := *argsPtr
	return args[:0] // reset length but keep capacity
}

// PutArgs returns command arguments to the pool
func (pm *PoolManager) PutArgs(args []string) {
	if args == nil || cap(args) > 100 {
		return // don't pool if too large
	}

	// Clear the slice
	for i := range args {
		args[i] = ""
	}

	args = args[:0]
	pm.argsPool.Put(&args)
}

// GetPubSubMessage returns a PubSub message from the pool
func (pm *PoolManager) GetPubSubMessage() *PubSubMessage {
	msg := pm.pubsubMsgPool.Get().(*PubSubMessage)
	msg.Channel = ""
	msg.Message = ""
	return msg
}

// PutPubSubMessage returns a PubSub message to the pool
func (pm *PoolManager) PutPubSubMessage(msg *PubSubMessage) {
	if msg != nil {
		pm.pubsubMsgPool.Put(msg)
	}
}

// startCleanup starts the periodic cleanup routine
func (pm *PoolManager) startCleanup() {
	pm.cleanupTicker = time.NewTicker(5 * time.Minute) // cleanup every 5 minutes

	go func() {
		for {
			select {
			case <-pm.cleanupTicker.C:
				pm.cleanup()
			case <-pm.stopCleanup:
				pm.cleanupTicker.Stop()
				return
			}
		}
	}()
}

// cleanup performs periodic pool maintenance
func (pm *PoolManager) cleanup() {
	// Force GC to clean up any unreferenced pooled objects
	// This is a no-op since Go's GC will handle this automatically,
	// but we could add specific cleanup logic here if needed
}

// Stop stops the pool manager and cleanup routines
func (pm *PoolManager) Stop() {
	close(pm.stopCleanup)
	if pm.cleanupTicker != nil {
		pm.cleanupTicker.Stop()
	}
}

// Global pool manager instance
var GlobalPoolManager = NewPoolManager()
