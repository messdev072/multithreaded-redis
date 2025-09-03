package store

import (
	"bytes"
	"encoding/gob"
	"time"
)

func (s *Store) serializeValue(v Value) []byte {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	// Encode the fields you need
	_ = enc.Encode(v.Type)
	_ = enc.Encode(v.Data)
	_ = enc.Encode(v.Hash)
	_ = enc.Encode(v.Set)
	_ = enc.Encode(v.List)
	//_ = enc.Encode(v.CMS) // ensure CMS is gob-encodable or skip
	//_ = enc.Encode(v.Bloom)
	return buf.Bytes()
}

func (s *Store) restoreFromDump(kd KeyDump) {
	var v Value
	buf := bytes.NewBuffer(kd.ValueBytes)
	dec := gob.NewDecoder(buf)
	_ = dec.Decode(&v.Type)
	_ = dec.Decode(&v.Data)
	_ = dec.Decode(&v.Hash)
	_ = dec.Decode(&v.Set)
	_ = dec.Decode(&v.List)
	// set expiration & last access
	if !kd.TTL.IsZero() {
		v.Expiration = kd.TTL.UnixNano()
	} else {
		v.Expiration = 0
	}
	v.LastAccess = time.Now().UnixNano()
	//set into store
	s.mu.Lock()
	s.data[kd.Key] = v
	s.mu.Unlock()
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
