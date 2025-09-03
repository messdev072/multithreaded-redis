package store

import (
	"context"
	"log"
	"time"
)

func (ss *SharedStore) backgroundMigrateTo(ctx context.Context, destNode string, batchSize int) error {
	// iterate over all shards except destNode
	ss.mu.RLock()
	nodes := ss.ring.Nodes()
	ss.mu.RUnlock()

	for _, node := range nodes {
		if node == destNode {
			continue
		}
		srcShard, ok := ss.getShardByNodeID(node)
		if !ok {
			continue
		}
		cursorDone := false
		for !cursorDone {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
			keys := srcShard.Store.ScanKeys(batchSize)
			if len(keys) == 0 {
				cursorDone = true
				break
			}
			for _, k := range keys {
				// ship keys that currently dont map to destNode anymore
				target, ok := ss.ring.GetNode(k)
				if !ok || target != destNode {
					continue
				}
				// DUMPKEY
				dumpReq := ShardRequest{
					Command: "DUMPKEY",
					Key:     k,
					Reply:   make(chan interface{}, 1),
				}
				srcShard.inbox <- dumpReq
				resp := <-dumpReq.Reply
				if resp == nil {
					// key vanished or expired; skip
					continue
				}
				kd, ok := resp.(KeyDump)
				if !ok {
					log.Printf("unexpected dump response type for key %s: %T", k, resp)
					continue
				}

				// MIGRATE_RESTORE -> dest
				destShard, ok := ss.getShardByNodeID(destNode)
				if !ok {
					log.Printf("destination shard %s not found", destNode)
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

				// MIGRATE_DELETE -> source
				delReq := ShardRequest{
					Command: "MIGRATE_DELETE",
					Key:     k,
					Reply:   make(chan interface{}, 1),
				}
				srcShard.inbox <- delReq
				<-delReq.Reply

				//sleep abit to reduce load
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(1 * time.Millisecond):
				}
			}
		}
	}
	return nil
}
