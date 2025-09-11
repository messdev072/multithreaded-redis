package store

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// RDB represents Redis Database snapshot functionality
type RDB struct {
	path           string
	mu             sync.RWMutex
	lastSave       time.Time
	saveInProgress bool
	stats          RDBStats
}

// RDBStats tracks RDB snapshot statistics
type RDBStats struct {
	LastSaveTime     time.Time
	LastSaveDuration time.Duration
	LastSaveKeys     int64
	TotalSaves       int64
	TotalSaveTime    time.Duration
	LastError        error
}

// RDB file format constants
const (
	RDBMagicString  = "REDIS0011"
	RDBVersion      = 11
	RDBTypeString   = 0
	RDBTypeList     = 1
	RDBTypeSet      = 2
	RDBTypeZSet     = 3
	RDBTypeHash     = 4
	RDBSelectDB     = 254
	RDBExpireTime   = 253
	RDBExpireTimeMS = 252
	RDBEOF          = 255
)

// NewRDB creates a new RDB instance
func NewRDB(path string) (*RDB, error) {
	// Ensure directory exists
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create RDB directory: %v", err)
	}

	return &RDB{
		path: path,
		stats: RDBStats{
			LastSaveTime: time.Now(),
		},
	}, nil
}

// Save creates a snapshot of the store data
func (r *RDB) Save(store *Store) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.saveInProgress {
		return fmt.Errorf("save already in progress")
	}

	r.saveInProgress = true
	defer func() { r.saveInProgress = false }()

	start := time.Now()

	// Create temporary file for atomic write
	tmpPath := r.path + ".tmp"
	file, err := os.Create(tmpPath)
	if err != nil {
		r.stats.LastError = err
		return fmt.Errorf("failed to create RDB temp file: %v", err)
	}
	defer file.Close()

	writer := bufio.NewWriter(file)
	defer writer.Flush()

	// Write RDB header
	if err := r.writeHeader(writer); err != nil {
		r.stats.LastError = err
		return fmt.Errorf("failed to write RDB header: %v", err)
	}

	// Write database selector (DB 0)
	if err := r.writeDBSelector(writer, 0); err != nil {
		r.stats.LastError = err
		return fmt.Errorf("failed to write DB selector: %v", err)
	}

	// Write all key-value pairs
	keyCount, err := r.writeKeyValuePairs(writer, store)
	if err != nil {
		r.stats.LastError = err
		return fmt.Errorf("failed to write key-value pairs: %v", err)
	}

	// Write EOF marker
	if err := writer.WriteByte(RDBEOF); err != nil {
		r.stats.LastError = err
		return fmt.Errorf("failed to write EOF: %v", err)
	}

	// Write checksum (simplified - just write 8 bytes)
	checksum := uint64(time.Now().Unix())
	if err := binary.Write(writer, binary.LittleEndian, checksum); err != nil {
		r.stats.LastError = err
		return fmt.Errorf("failed to write checksum: %v", err)
	}

	if err := writer.Flush(); err != nil {
		r.stats.LastError = err
		return fmt.Errorf("failed to flush RDB file: %v", err)
	}

	if err := file.Sync(); err != nil {
		r.stats.LastError = err
		return fmt.Errorf("failed to sync RDB file: %v", err)
	}

	// Atomic rename
	if err := os.Rename(tmpPath, r.path); err != nil {
		r.stats.LastError = err
		return fmt.Errorf("failed to rename RDB file: %v", err)
	}

	// Update statistics
	duration := time.Since(start)
	r.stats.LastSaveTime = start
	r.stats.LastSaveDuration = duration
	r.stats.LastSaveKeys = keyCount
	r.stats.TotalSaves++
	r.stats.TotalSaveTime += duration
	r.stats.LastError = nil
	r.lastSave = start

	log.Printf("RDB: Saved %d keys in %v to %s", keyCount, duration, r.path)
	return nil
}

// Load restores data from RDB snapshot
func (r *RDB) Load(store *Store) error {
	r.mu.RLock()
	defer r.mu.RUnlock()

	file, err := os.Open(r.path)
	if os.IsNotExist(err) {
		log.Printf("RDB: No snapshot file found at %s", r.path)
		return nil // Not an error - empty database
	}
	if err != nil {
		return fmt.Errorf("failed to open RDB file: %v", err)
	}
	defer file.Close()

	reader := bufio.NewReader(file)

	// Read and verify header
	if err := r.readHeader(reader); err != nil {
		return fmt.Errorf("failed to read RDB header: %v", err)
	}

	keyCount := int64(0)
	for {
		// Read next byte to determine what follows
		b, err := reader.ReadByte()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to read RDB: %v", err)
		}

		switch b {
		case RDBSelectDB:
			// Read database number (we only support DB 0)
			dbNum, err := r.readLength(reader)
			if err != nil {
				return fmt.Errorf("failed to read DB number: %v", err)
			}
			if dbNum != 0 {
				return fmt.Errorf("unsupported database number: %d", dbNum)
			}

		case RDBExpireTime, RDBExpireTimeMS:
			// Read expiration and key-value pair
			if err := r.readKeyValueWithExpiry(reader, store, b == RDBExpireTimeMS); err != nil {
				return fmt.Errorf("failed to read key-value with expiry: %v", err)
			}
			keyCount++

		case RDBEOF:
			// End of file
			goto done

		default:
			// Regular key-value pair (type is in the byte we just read)
			if err := r.readKeyValue(reader, store, b); err != nil {
				return fmt.Errorf("failed to read key-value: %v", err)
			}
			keyCount++
		}
	}

done:
	log.Printf("RDB: Loaded %d keys from %s", keyCount, r.path)
	return nil
}

// writeHeader writes the RDB file header
func (r *RDB) writeHeader(w *bufio.Writer) error {
	_, err := w.WriteString(RDBMagicString)
	return err
}

// writeDBSelector writes database selector
func (r *RDB) writeDBSelector(w *bufio.Writer, dbNum int) error {
	if err := w.WriteByte(RDBSelectDB); err != nil {
		return err
	}
	return r.writeLength(w, uint64(dbNum))
}

// writeKeyValuePairs writes all key-value pairs from store
func (r *RDB) writeKeyValuePairs(w *bufio.Writer, store *Store) (int64, error) {
	data := store.GetAllData()
	ttl := store.GetAllTTL()

	keyCount := int64(0)
	for key, value := range data {
		// Check if key has expiration
		if expTime, hasExpiry := ttl[key]; hasExpiry {
			// Write expiration time in milliseconds
			if err := w.WriteByte(RDBExpireTimeMS); err != nil {
				return keyCount, err
			}
			expMs := expTime.UnixMilli()
			if err := binary.Write(w, binary.LittleEndian, expMs); err != nil {
				return keyCount, err
			}
		}

		// Write key-value pair based on type
		if err := r.writeKeyValueByType(w, key, value); err != nil {
			return keyCount, err
		}
		keyCount++
	}

	return keyCount, nil
}

// writeKeyValueByType writes a key-value pair based on value type
func (r *RDB) writeKeyValueByType(w *bufio.Writer, key string, value Value) error {
	switch value.Type {
	case StringType:
		return r.writeStringKV(w, key, value)
	case SetType:
		return r.writeSetKV(w, key, value)
	case HashType:
		return r.writeHashKV(w, key, value)
	case ListType:
		return r.writeListKV(w, key, value)
	case ZSetType:
		return r.writeZSetKV(w, key, value)
	default:
		// Skip unsupported types for now
		log.Printf("RDB: Skipping unsupported type %d for key %s", value.Type, key)
		return nil
	}
}

// writeStringKV writes a string key-value pair
func (r *RDB) writeStringKV(w *bufio.Writer, key string, value Value) error {
	if err := w.WriteByte(RDBTypeString); err != nil {
		return err
	}
	if err := r.writeString(w, key); err != nil {
		return err
	}
	return r.writeString(w, string(value.Data))
}

// writeSetKV writes a set key-value pair
func (r *RDB) writeSetKV(w *bufio.Writer, key string, value Value) error {
	if err := w.WriteByte(RDBTypeSet); err != nil {
		return err
	}
	if err := r.writeString(w, key); err != nil {
		return err
	}
	if err := r.writeLength(w, uint64(len(value.Set))); err != nil {
		return err
	}
	for member := range value.Set {
		if err := r.writeString(w, member); err != nil {
			return err
		}
	}
	return nil
}

// writeHashKV writes a hash key-value pair
func (r *RDB) writeHashKV(w *bufio.Writer, key string, value Value) error {
	if err := w.WriteByte(RDBTypeHash); err != nil {
		return err
	}
	if err := r.writeString(w, key); err != nil {
		return err
	}
	if err := r.writeLength(w, uint64(len(value.Hash))); err != nil {
		return err
	}
	for field, val := range value.Hash {
		if err := r.writeString(w, field); err != nil {
			return err
		}
		if err := r.writeString(w, val); err != nil {
			return err
		}
	}
	return nil
}

// writeListKV writes a list key-value pair
func (r *RDB) writeListKV(w *bufio.Writer, key string, value Value) error {
	if err := w.WriteByte(RDBTypeList); err != nil {
		return err
	}
	if err := r.writeString(w, key); err != nil {
		return err
	}
	if err := r.writeLength(w, uint64(len(value.List))); err != nil {
		return err
	}
	for _, item := range value.List {
		if err := r.writeString(w, item); err != nil {
			return err
		}
	}
	return nil
}

// writeZSetKV writes a sorted set key-value pair
func (r *RDB) writeZSetKV(w *bufio.Writer, key string, value Value) error {
	if err := w.WriteByte(RDBTypeZSet); err != nil {
		return err
	}
	if err := r.writeString(w, key); err != nil {
		return err
	}
	if err := r.writeLength(w, uint64(len(value.ZSet))); err != nil {
		return err
	}
	for member, score := range value.ZSet {
		if err := r.writeString(w, member); err != nil {
			return err
		}
		if err := binary.Write(w, binary.LittleEndian, score); err != nil {
			return err
		}
	}
	return nil
}

// writeString writes a string with length prefix
func (r *RDB) writeString(w *bufio.Writer, s string) error {
	if err := r.writeLength(w, uint64(len(s))); err != nil {
		return err
	}
	_, err := w.WriteString(s)
	return err
}

// writeLength writes a length value (simplified encoding)
func (r *RDB) writeLength(w *bufio.Writer, length uint64) error {
	if length < 64 {
		// 6-bit length
		return w.WriteByte(byte(length))
	} else if length < 16384 {
		// 14-bit length
		b1 := byte((length >> 8) | 0x40)
		b2 := byte(length & 0xFF)
		if err := w.WriteByte(b1); err != nil {
			return err
		}
		return w.WriteByte(b2)
	} else {
		// 32-bit length
		if err := w.WriteByte(0x80); err != nil {
			return err
		}
		return binary.Write(w, binary.BigEndian, uint32(length))
	}
}

// readHeader reads and verifies RDB header
func (r *RDB) readHeader(reader *bufio.Reader) error {
	header := make([]byte, len(RDBMagicString))
	if _, err := io.ReadFull(reader, header); err != nil {
		return fmt.Errorf("failed to read header: %v", err)
	}
	if string(header) != RDBMagicString {
		return fmt.Errorf("invalid RDB magic string: %s", string(header))
	}
	return nil
}

// readLength reads a length value
func (r *RDB) readLength(reader *bufio.Reader) (uint64, error) {
	b, err := reader.ReadByte()
	if err != nil {
		return 0, err
	}

	switch (b & 0xC0) >> 6 {
	case 0:
		// 6-bit length
		return uint64(b & 0x3F), nil
	case 1:
		// 14-bit length
		b2, err := reader.ReadByte()
		if err != nil {
			return 0, err
		}
		return uint64(b&0x3F)<<8 | uint64(b2), nil
	case 2:
		// 32-bit length
		var length uint32
		if err := binary.Read(reader, binary.BigEndian, &length); err != nil {
			return 0, err
		}
		return uint64(length), nil
	default:
		return 0, fmt.Errorf("invalid length encoding")
	}
}

// readString reads a string with length prefix
func (r *RDB) readString(reader *bufio.Reader) (string, error) {
	length, err := r.readLength(reader)
	if err != nil {
		return "", err
	}

	data := make([]byte, length)
	if _, err := io.ReadFull(reader, data); err != nil {
		return "", err
	}

	return string(data), nil
}

// readKeyValueWithExpiry reads a key-value pair with expiration
func (r *RDB) readKeyValueWithExpiry(reader *bufio.Reader, store *Store, isMilliseconds bool) error {
	var expTime time.Time

	if isMilliseconds {
		var expMs int64
		if err := binary.Read(reader, binary.LittleEndian, &expMs); err != nil {
			return err
		}
		expTime = time.UnixMilli(expMs)
	} else {
		var expSec int32
		if err := binary.Read(reader, binary.LittleEndian, &expSec); err != nil {
			return err
		}
		expTime = time.Unix(int64(expSec), 0)
	}

	// Read the type byte
	valueType, err := reader.ReadByte()
	if err != nil {
		return err
	}

	// Read the key-value pair
	key, value, err := r.readKeyValueByType(reader, valueType)
	if err != nil {
		return err
	}

	// Store with expiration
	store.SetDataDirect(key, value, &expTime)

	return nil
}

// readKeyValue reads a key-value pair without expiration
func (r *RDB) readKeyValue(reader *bufio.Reader, store *Store, valueType byte) error {
	key, value, err := r.readKeyValueByType(reader, valueType)
	if err != nil {
		return err
	}

	store.SetDataDirect(key, value, nil)
	return nil
}

// readKeyValueByType reads a key-value pair based on type
func (r *RDB) readKeyValueByType(reader *bufio.Reader, valueType byte) (string, Value, error) {
	var value Value

	switch valueType {
	case RDBTypeString:
		return r.readStringKV(reader)
	case RDBTypeSet:
		return r.readSetKV(reader)
	case RDBTypeHash:
		return r.readHashKV(reader)
	case RDBTypeList:
		return r.readListKV(reader)
	case RDBTypeZSet:
		return r.readZSetKV(reader)
	default:
		return "", value, fmt.Errorf("unsupported RDB type: %d", valueType)
	}
}

// readStringKV reads a string key-value pair
func (r *RDB) readStringKV(reader *bufio.Reader) (string, Value, error) {
	key, err := r.readString(reader)
	if err != nil {
		return "", Value{}, err
	}

	data, err := r.readString(reader)
	if err != nil {
		return "", Value{}, err
	}

	value := Value{
		Type: StringType,
		Data: []byte(data),
	}

	return key, value, nil
}

// readSetKV reads a set key-value pair
func (r *RDB) readSetKV(reader *bufio.Reader) (string, Value, error) {
	key, err := r.readString(reader)
	if err != nil {
		return "", Value{}, err
	}

	setSize, err := r.readLength(reader)
	if err != nil {
		return "", Value{}, err
	}

	set := make(map[string]struct{})
	for i := uint64(0); i < setSize; i++ {
		member, err := r.readString(reader)
		if err != nil {
			return "", Value{}, err
		}
		set[member] = struct{}{}
	}

	value := Value{
		Type: SetType,
		Set:  set,
	}

	return key, value, nil
}

// readHashKV reads a hash key-value pair
func (r *RDB) readHashKV(reader *bufio.Reader) (string, Value, error) {
	key, err := r.readString(reader)
	if err != nil {
		return "", Value{}, err
	}

	hashSize, err := r.readLength(reader)
	if err != nil {
		return "", Value{}, err
	}

	hash := make(map[string]string)
	for i := uint64(0); i < hashSize; i++ {
		field, err := r.readString(reader)
		if err != nil {
			return "", Value{}, err
		}
		val, err := r.readString(reader)
		if err != nil {
			return "", Value{}, err
		}
		hash[field] = val
	}

	value := Value{
		Type: HashType,
		Hash: hash,
	}

	return key, value, nil
}

// readListKV reads a list key-value pair
func (r *RDB) readListKV(reader *bufio.Reader) (string, Value, error) {
	key, err := r.readString(reader)
	if err != nil {
		return "", Value{}, err
	}

	listSize, err := r.readLength(reader)
	if err != nil {
		return "", Value{}, err
	}

	list := make([]string, listSize)
	for i := uint64(0); i < listSize; i++ {
		item, err := r.readString(reader)
		if err != nil {
			return "", Value{}, err
		}
		list[i] = item
	}

	value := Value{
		Type: ListType,
		List: list,
	}

	return key, value, nil
}

// readZSetKV reads a sorted set key-value pair
func (r *RDB) readZSetKV(reader *bufio.Reader) (string, Value, error) {
	key, err := r.readString(reader)
	if err != nil {
		return "", Value{}, err
	}

	zsetSize, err := r.readLength(reader)
	if err != nil {
		return "", Value{}, err
	}

	zset := make(map[string]float64)
	for i := uint64(0); i < zsetSize; i++ {
		member, err := r.readString(reader)
		if err != nil {
			return "", Value{}, err
		}
		var score float64
		if err := binary.Read(reader, binary.LittleEndian, &score); err != nil {
			return "", Value{}, err
		}
		zset[member] = score
	}

	value := Value{
		Type: ZSetType,
		ZSet: zset,
	}

	return key, value, nil
}

// GetStats returns RDB statistics
func (r *RDB) GetStats() RDBStats {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.stats
}

// GetLastSaveTime returns the time of the last successful save
func (r *RDB) GetLastSaveTime() time.Time {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.lastSave
}

// IsSaveInProgress returns whether a save operation is currently running
func (r *RDB) IsSaveInProgress() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.saveInProgress
}
