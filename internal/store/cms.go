package store

import (
	"hash/fnv"
)

func NewCountMinSketch(depth, width int) *CountMinSketch {
	cms := &CountMinSketch{
		depth: depth,
		width: width,
		table: make([][]uint32, depth),
	}

	for i := range cms.table {
		cms.table[i] = make([]uint32, width)
	}

	// simple hash family : FNV with varying seeds
	cms.hashFuncs = make([]func(string) uint32, depth)
	for i := 0; i < depth; i++ {
		seed := uint32(i + 1) // simple seed based on depth index
		cms.hashFuncs[i] = func(s string) uint32 {
			h := fnv.New32a()
			h.Write([]byte(s))
			return (h.Sum32() + seed) % uint32(width)
		}
	}

	return cms
}

func (cms *CountMinSketch) Incr(item string, count uint32) {
	for i := 0; i < cms.depth; i++ {
		idx := cms.hashFuncs[i](item)
		cms.table[i][idx] += count
	}
}

func (cms *CountMinSketch) Query(item string) uint32 {
	min := ^uint32(0) // max uint32 value
	for i := 0; i < cms.depth; i++ {
		idx := cms.hashFuncs[i](item)
		val := cms.table[i][idx]
		if val < min {
			min = val
		}
	}
	return min
}
