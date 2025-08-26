package datastuctures

import "hash/fnv"

type BloomFilter struct {
	m     uint
	k     uint
	bits  []byte
	seeds []uint64
}

func NewBloomFilter(m, k uint) *BloomFilter {
	seeds := make([]uint64, k)
	for i := uint(0); i < k; i++ {
		seeds[i] = uint64(i + 1) // simple different seeds
	}
	return &BloomFilter{
		m:     m,
		k:     k,
		bits:  make([]byte, (m+7)/8), // round up to full bytes
		seeds: seeds,
	}
}

func (bf *BloomFilter) hash(data string, seed uint64) uint {
	h := fnv.New64a()
	h.Write(([]byte(data)))
	sum := h.Sum64() ^ seed
	return uint(sum % uint64(bf.m))
}

func (bf *BloomFilter) Add(item string) {
	for _, seed := range bf.seeds {
		pos := bf.hash(item, seed)
		byteIndex := pos / 8
		bitIndex := pos % 8
		bf.bits[byteIndex] |= (1 << bitIndex)
	}
}

func (bf *BloomFilter) Exists(item string) bool {
	for _, seed := range bf.seeds {
		pos := bf.hash(item, seed)
		byteIndex := pos / 8
		bitIndex := pos % 8
		if bf.bits[byteIndex]&(1<<bitIndex) == 0 {
			return false
		}
	}
	return true
}
