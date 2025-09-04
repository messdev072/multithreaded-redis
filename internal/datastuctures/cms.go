package datastuctures

import (
	"bytes"
	"encoding/gob"
	"hash/fnv"
)

// cmsData is used for serialization of CountMinSketch
type cmsData struct {
	Depth int
	Width int
	Table [][]uint32
}

func init() {
	gob.Register(&cmsData{})
}

type CountMinSketch struct {
	Depth     int
	Width     int
	Table     [][]uint32
	HashFuncs []func(string) uint32 `json:"-" gob:"-"` // Skip serialization of functions
}

func NewCountMinSketch(depth, width int) *CountMinSketch {
	cms := &CountMinSketch{
		Depth: depth,
		Width: width,
		Table: make([][]uint32, depth),
	}

	for i := range cms.Table {
		cms.Table[i] = make([]uint32, width)
	}

	// simple hash family : FNV with varying seeds
	cms.HashFuncs = make([]func(string) uint32, depth)
	for i := 0; i < depth; i++ {
		seed := uint32(i + 1) // simple seed based on depth index
		cms.HashFuncs[i] = func(s string) uint32 {
			h := fnv.New32a()
			h.Write([]byte(s))
			return (h.Sum32() + seed) % uint32(width)
		}
	}

	return cms
}

func (cms *CountMinSketch) Incr(item string, count uint32) {
	for i := 0; i < cms.Depth; i++ {
		idx := cms.HashFuncs[i](item)
		cms.Table[i][idx] += count
	}
}

func (cms *CountMinSketch) Query(item string) uint32 {
	min := ^uint32(0) // max uint32 value
	for i := 0; i < cms.Depth; i++ {
		idx := cms.HashFuncs[i](item)
		val := cms.Table[i][idx]
		if val < min {
			min = val
		}
	}
	return min
}

// GobEncode implements gob.GobEncoder interface
func (cms *CountMinSketch) GobEncode() ([]byte, error) {
	data := &cmsData{
		Depth: cms.Depth,
		Width: cms.Width,
		Table: cms.Table,
	}
	
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(data); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}// GobDecode implements gob.GobDecoder interface
func (cms *CountMinSketch) GobDecode(data []byte) error {
	var tmp cmsData
	dec := gob.NewDecoder(bytes.NewReader(data))
	if err := dec.Decode(&tmp); err != nil {
		return err
	}

	// Restore data
	cms.Depth = tmp.Depth
	cms.Width = tmp.Width
	cms.Table = tmp.Table

	// Recreate hash functions
	cms.HashFuncs = make([]func(string) uint32, cms.Depth)
	for i := 0; i < cms.Depth; i++ {
		seed := uint32(i + 1)
		cms.HashFuncs[i] = func(s string) uint32 {
			h := fnv.New32a()
			h.Write([]byte(s))
			return (h.Sum32() + seed) % uint32(cms.Width)
		}
	}

	return nil
}
