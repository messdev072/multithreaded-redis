package store

import (
	"context"
	"log"
	"time"
)

func (ss *SharedStore) BackgroundMigrateTo(ctx context.Context, destNode string, batchSize int) error {
	// iterate over all shards except destNode
	ss.mu.RLock()
	nodes := ss.ring.Nodes()
	ss.mu.RUnlock()

	log.Printf("Starting migration scan to node %s from nodes: %v", destNode, nodes)

	// Track which keys we've already processed
	processedKeys := make(map[string]bool)
	totalKeys := 0
	migratedKeys := 0

	// First pass to count total keys and collect unique keys
	nodeKeys := make(map[string][]string)
	for _, node := range nodes {
		if node == destNode {
			continue
		}
		if srcShard, ok := ss.getShardByNodeID(node); ok {
			keys := srcShard.Store.ScanKeys(-1)
			uniqKeys := make([]string, 0, len(keys))
			for _, k := range keys {
				if !processedKeys[k] {
					targetNode, ok := ss.ring.GetNode(k)
					if ok {
						log.Printf("DEBUG: %s currently maps to node %s", k, targetNode)
						if targetNode == destNode {
							uniqKeys = append(uniqKeys, k)
							processedKeys[k] = false // false means not yet processed
						}
					}
				}
			}
			nodeKeys[node] = uniqKeys
			totalKeys += len(uniqKeys)
			log.Printf("Node %s has %d unique keys to migrate", node, len(uniqKeys))
		} else {
			log.Printf("Warning: Could not find source shard for node %s", node)
		}
	}

	log.Printf("Starting migration to node %s: %d unique keys to process", destNode, totalKeys)
	lastProgress := time.Now()

	// Process each node's unique keys
	for node, keys := range nodeKeys {
		srcShard, ok := ss.getShardByNodeID(node)
		if !ok {
			continue
		}
		// Process in batches
		for i := 0; i < len(keys); i += batchSize {
			end := i + batchSize
			if end > len(keys) {
				end = len(keys)
			}
			batch := keys[i:end]
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			log.Printf("Node %s: processing batch of %d keys", node, len(batch))

			for _, k := range batch {
				if processedKeys[k] { // Skip already processed keys
					continue
				}
				// ship keys that currently dont map to destNode anymore
				target, ok := ss.ring.GetNode(k)
				if !ok {
					log.Printf("Warning: Could not get target node for key %s", k)
					continue
				}
				log.Printf("DEBUG: %s currently maps to node %s", k, target)
				if target != destNode {
					log.Printf("Key %s maps to node %s (not %s), skipping", k, target, destNode)
					continue
				}
				// DUMPKEY
				var kd KeyDump
				dumpReq := ShardRequest{
					Command:  "DUMPKEY",
					Key:      k,
					Reply:    make(chan interface{}, 1),
					internal: true,
				}
				srcShard.inbox <- dumpReq
				select {
				case resp := <-dumpReq.Reply:
					if resp == nil {
						// key vanished or expired; skip
						log.Printf("Key %s vanished or expired during migration", k)
						continue
					}

					switch v := resp.(type) {
					case KeyDump:
						kd = v
						log.Printf("DEBUG: %s - Successfully dumped from shard %s with type %d and data %q",
							k, node, v.ValueType, string(v.ValueBytes))
					case *KeyDump:
						kd = *v
						log.Printf("DEBUG: %s - Successfully dumped from shard %s with type %d and data %q",
							k, node, v.ValueType, string(v.ValueBytes))
					default:
						log.Printf("unexpected dump response type for key %s: %T (value: %v)", k, resp, resp)
						continue
					}
				case <-time.After(5 * time.Second):
					log.Printf("timeout waiting for DUMPKEY response for key %s", k)
					continue
				}

				// MIGRATE_RESTORE -> dest
				destShard, ok := ss.getShardByNodeID(destNode)
				if !ok {
					log.Printf("destination shard %s not found", destNode)
					continue
				}
				if k == "key2" {
					log.Printf("DEBUG: Attempting to migrate key2 to node %s with value type %d and %d bytes",
						destNode, kd.ValueType, len(kd.ValueBytes))
				}
				restoreReq := ShardRequest{
					Command: "MIGRATE_RESTORE",
					Key:     k,
					Payload: kd,
					Reply:   make(chan interface{}, 1),
				}
				destShard.inbox <- restoreReq
				res := <-restoreReq.Reply
				if err, isErr := res.(error); isErr {
					log.Printf("restore error for key %s -> %v", k, err)
					//optionally retry/backoff
					continue
				}
				if k == "key2" {
					log.Printf("DEBUG: Successfully restored key2 to node %s", destNode)
				}

				// MIGRATE_DELETE -> source (must be sent to srcShard, not destShard)
				delReq := ShardRequest{
					Command:  "MIGRATE_DELETE",
					Key:     k,
					Reply:   make(chan interface{}, 1),
					internal: true,  // mark as internal to prevent rerouting
				}
				// Send delete to source shard where the key originally was
				srcShard.inbox <- delReq
				delResp := <-delReq.Reply
				if deleted, ok := delResp.(bool); ok && deleted {
					log.Printf("DEBUG: %s - Successfully deleted from source shard %s", k, node)
				} else {
					log.Printf("WARNING: %s - Failed to delete from source shard %s (response: %v)", k, node, delResp)
				}

				processedKeys[k] = true
				migratedKeys++

				// Report progress every second
				if time.Since(lastProgress) > time.Second {
					progress := float64(migratedKeys) / float64(totalKeys) * 100
					log.Printf("Migration progress: %d/%d keys (%.1f%%)", migratedKeys, totalKeys, progress)
					lastProgress = time.Now()
				}

				//sleep abit to reduce load but make it shorter since we increased batch size
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(100 * time.Microsecond):
				}
			}
		}
	}
	log.Printf("Migration completed: %d/%d keys processed", migratedKeys, totalKeys)
	return nil
}
