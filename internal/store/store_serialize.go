package store

import (
	"bytes"
	"encoding/gob"
	"fmt"
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

	// For logging
	switch v.Type {
	case StringType:
		log.Printf("DEBUG: Serializing string value: type=%d, data=%q", v.Type, string(v.Data))
	case SetType:
		log.Printf("DEBUG: Serializing set value: type=%d, members=%d", v.Type, len(v.Set))
	case HashType:
		log.Printf("DEBUG: Serializing hash value: type=%d, fields=%d", v.Type, len(v.Hash))
	case CMSType:
		if v.CMS != nil {
			log.Printf("DEBUG: Serializing CMS value: type=%d, width=%d, depth=%d", v.Type, v.CMS.Width, v.CMS.Depth)
		} else {
			log.Printf("DEBUG: Serializing CMS value: type=%d, but CMS is nil", v.Type)
		}
	default:
		log.Printf("DEBUG: Serializing value: type=%d", v.Type)
	}

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

	// Log restore operation for all types
	switch v.Type {
	case StringType:
		log.Printf("DEBUG: Restoring string value: type=%d, data=%q", v.Type, string(v.Data))
		if len(v.Data) == 0 {
			return fmt.Errorf("empty data for string value")
		}
	case SetType:
		log.Printf("DEBUG: Restoring set value: type=%d, members=%d", v.Type, len(v.Set))
	case HashType:
		log.Printf("DEBUG: Restoring hash value: type=%d, fields=%d", v.Type, len(v.Hash))
	case CMSType:
		if v.CMS != nil {
			log.Printf("DEBUG: Restoring CMS value: type=%d, width=%d, depth=%d", v.Type, v.CMS.Width, v.CMS.Depth)
		} else {
			log.Printf("DEBUG: Restoring CMS value: type=%d, but CMS is nil", v.Type)
		}
	default:
		log.Printf("DEBUG: Restoring value: type=%d", v.Type)
	}

	// set expiration & last access
	if !kd.TTL.IsZero() {
		v.Expiration = kd.TTL.UnixNano()
	} else {
		v.Expiration = 0
	}
	v.LastAccess = time.Now().UnixNano()

	//set into store with proper TTL handling
	s.mu.Lock()
	defer s.mu.Unlock()

	if kd.Key == "key2" {
		log.Printf("DEBUG: key2 - Restoring with type %d and value %q", v.Type, string(v.Data))
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

	// Store the value and set TTL if needed
	s.data[kd.Key] = v
	if !kd.TTL.IsZero() {
		s.ttl[kd.Key] = kd.TTL
	}

	log.Printf("DEBUG: %s - Successfully restored value with type=%d", kd.Key, v.Type)
	if v.Type == StringType {
		log.Printf("DEBUG: %s - Stored string value: %q", kd.Key, string(v.Data))
	}

	// Extra debug logging for key2
	if kd.Key == "key2" {
		// Verify it was stored
		if stored, ok := s.data[kd.Key]; ok {
			log.Printf("DEBUG: key2 - Verified in store with type %d and value %q",
				stored.Type, string(stored.Data))
		} else {
			log.Printf("ERROR: key2 - Failed to verify in store after setting!")
		}
	}
	return nil
}

func (s *Store) getExpirationTime(key string) time.Time {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if expNano, ok := s.ttl[key]; ok {
		return expNano
	}
	return time.Time{}
}

func (s *Store) getRaw(key string) (Value, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.data[key]
	return v, ok
}
