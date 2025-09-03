package store

import (
	"hash/fnv"
	"sort"
	"strconv"
	"sync"
)

type HashRing struct {
	mutex    sync.RWMutex
	replicas int               // virtual nodes per real node
	keys     []uint32          // sorted hashes of virtual nodes
	vnodeMap map[uint32]string // maps virtual node hash to real node
	nodes    map[string]struct{}
}

func NewHashRing(replicas int) *HashRing {
	hr := &HashRing{
		replicas: replicas,
		vnodeMap: make(map[uint32]string),
		nodes:    make(map[string]struct{}),
		keys:     nil,
	}
	return hr
}

func (hr *HashRing) hashStr(s string) uint32 {
	hf := fnv.New32a()
	hf.Write([]byte(s))
	return hf.Sum32()
}

func (hr *HashRing) AddNode(nodeID string) {
	hr.mutex.Lock()
	defer hr.mutex.Unlock()

	if _, ok := hr.nodes[nodeID]; ok {
		return
	}

	hr.nodes[nodeID] = struct{}{}
	for i := 0; i < hr.replicas; i++ {
		vnodeKey := nodeID + "#" + strconv.Itoa(i)
		hv := hr.hashStr(vnodeKey)
		hr.keys = append(hr.keys, hv)
		hr.vnodeMap[hv] = nodeID
	}
	sort.Slice(hr.keys, func(i, j int) bool { return hr.keys[i] < hr.keys[j] })
}

func (hr *HashRing) RemoveNode(nodeID string) {
	hr.mutex.Lock()
	defer hr.mutex.Unlock()
	if _, ok := hr.nodes[nodeID]; !ok {
		return
	}
	delete(hr.nodes, nodeID)
	// rebuild keys without vnode entries for nodeID
	newKeys := hr.keys[:0]
	for _, kv := range hr.keys {
		if hr.vnodeMap[kv] == nodeID {
			delete(hr.vnodeMap, kv)
			continue
		}
		newKeys = append(newKeys, kv)
	}
	hr.keys = newKeys
}

func (hr *HashRing) GetNode(key string) (string, bool) {
	hr.mutex.RLock()
	defer hr.mutex.RUnlock()
	if len(hr.keys) == 0 {
		return "", false
	}
	hv := hr.hashStr(key)
	// Binery search for the closest vnode hash >= hv
	idx := sort.Search(len(hr.keys), func(i int) bool { return hr.keys[i] >= hv })
	if idx == len(hr.keys) {
		idx = 0
	}
	node := hr.vnodeMap[hr.keys[idx]]
	return node, true
}

func (hr *HashRing) Nodes() []string {
	hr.mutex.RLock()
	defer hr.mutex.RUnlock()

	out := make([]string, 0, len(hr.nodes))
	for n := range hr.nodes {
		out = append(out, n)
	}
	return out
}
