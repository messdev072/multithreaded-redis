package store

import (
	"bytes"
	"encoding/gob"
	"log"
	"time"

	"multithreaded-redis/internal/datastuctures"
)

// SerializedValue is used for serializing the Value struct
type SerializedValue struct {
	Type ValueType
	Data []byte              // for strings
	Set  map[string]struct{} // for sets
	Hash map[string]string   // for hashes
	CMS  []byte              // serialized CMS data
}

func init() {
	gob.Register(SerializedValue{})
}

func (s *Store) serializeValue(v Value) []byte {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)

	// Create a serializable version of the value
	sv := SerializedValue{
		Type: v.Type,
		Data: v.Data,
		Set:  v.Set,
		Hash: v.Hash,
	}

	// If we have a CMS, serialize it separately
	if v.CMS != nil {
		cmsBytes, err := v.CMS.GobEncode()
		if err != nil {
			log.Printf("ERROR: Failed to encode CMS: %v", err)
			return nil
		}
		sv.CMS = cmsBytes
	}

	// Encode the serialized version
	if err := enc.Encode(sv); err != nil {
		log.Printf("ERROR: Failed to encode value: %v", err)
		return nil
	}

	bytes := buf.Bytes()
	if len(bytes) == 0 {
		log.Printf("WARNING: Serialization produced empty byte array")
	}
	return bytes
}

func (s *Store) restoreFromDump(kd KeyDump) error {
	var sv SerializedValue
	buf := bytes.NewBuffer(kd.ValueBytes)
	dec := gob.NewDecoder(buf)

	// Decode the serialized value
	if err := dec.Decode(&sv); err != nil {
		log.Printf("ERROR: Failed to decode value: %v", err)
		return err
	}

	// Create the actual Value
	v := Value{
		Type: sv.Type,
		Data: sv.Data,
		Set:  sv.Set,
		Hash: sv.Hash,
	}

	// If we have serialized CMS data, deserialize it
	if len(sv.CMS) > 0 {
		cms := &datastuctures.CountMinSketch{}
		if err := cms.GobDecode(sv.CMS); err != nil {
			log.Printf("ERROR: Failed to decode CMS: %v", err)
			return err
		}
		v.CMS = cms
	}

	// Initialize nil maps if needed
	if v.Hash == nil {
		v.Hash = make(map[string]string)
	}
	if v.Set == nil {
		v.Set = make(map[string]struct{})
	}
	if v.ZSet == nil {
		v.ZSet = make(map[string]float64)
	}

	// set expiration & last access
	if !kd.TTL.IsZero() {
		v.Expiration = kd.TTL.UnixNano()
	} else {
		v.Expiration = 0
	}
	v.LastAccess = time.Now().UnixNano()

	//set into store with proper TTL handling
	var expTime *time.Time
	if !kd.TTL.IsZero() {
		expTime = &kd.TTL
	}

	// Create deep copies of the maps to avoid any shared references
	if v.Hash != nil {
		newHash := make(map[string]string, len(v.Hash))
		for k, val := range v.Hash {
			newHash[k] = val
		}
		v.Hash = newHash
	}
	if v.Set != nil {
		newSet := make(map[string]struct{}, len(v.Set))
		for k, val := range v.Set {
			newSet[k] = val
		}
		v.Set = newSet
	}
	if v.ZSet != nil {
		newZSet := make(map[string]float64, len(v.ZSet))
		for k, val := range v.ZSet {
			newZSet[k] = val
		}
		v.ZSet = newZSet
	}

	// Store the value using the new sharded approach
	s.SetDataDirect(kd.Key, v, expTime)

	log.Printf("DEBUG: %s - Successfully restored value with type=%d", kd.Key, v.Type)
	if v.Type == StringType {
		log.Printf("DEBUG: %s - Stored string value: %q", kd.Key, string(v.Data))
	}

	return nil
}

func (s *Store) getExpirationTime(key string) time.Time {
	bucket := s.getBucket(key)
	bucket.mu.RLock()
	defer bucket.mu.RUnlock()
	if expTime, ok := bucket.ttl[key]; ok {
		return expTime
	}
	return time.Time{}
}

func (s *Store) getRaw(key string) (Value, bool) {
	bucket := s.getBucket(key)
	bucket.mu.RLock()
	defer bucket.mu.RUnlock()
	v, ok := bucket.data[key]
	return v, ok
}
