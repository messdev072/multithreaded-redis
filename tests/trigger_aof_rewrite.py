#!/usr/bin/env python3

import redis
import time

def trigger_aof_rewrite():
    """Generate enough data to trigger AOF rewrite (1MB threshold)"""
    
    print("🎯 Triggering AOF Rewrite Test...")
    
    r = redis.Redis(host='localhost', port=6380, decode_responses=True)
    
    # Generate lots of data to exceed 1MB threshold
    print("📈 Generating large amount of data...")
    
    for batch in range(10):  # 10 batches
        print(f"  Batch {batch + 1}/10...")
        
        for i in range(500):  # 500 operations per batch
            idx = batch * 500 + i
            
            # Generate large values to reach the threshold faster
            large_value = f"large_data_{'x' * 100}_{idx}"
            
            r.set(f"large_key:{idx}", large_value)
            r.hset(f"large_hash:{idx}", "field1", large_value)
            r.hset(f"large_hash:{idx}", "field2", large_value)
            r.sadd(f"large_set:{idx}", large_value, f"member2_{idx}")
            
            # Create some redundant operations for compaction testing
            r.set(f"redundant:{idx}", "old_value")
            r.set(f"redundant:{idx}", "new_value")  # This should replace the old one
            
            # Some deletions to test compaction
            if i % 50 == 0 and idx > 0:
                r.delete(f"redundant:{idx-1}")
    
    print("✅ Data generation complete!")
    
    # Check final AOF sizes
    import os
    import subprocess
    
    # Get actual file sizes using the correct path
    total_size = 0
    aof_files = []
    
    # Check if we're in the correct directory
    if not os.path.exists("logs"):
        print("⚠️  logs directory not found, checking current directory...")
        # Try running from parent directory
        try:
            result = subprocess.run(['du', '-b', 'logs/redis.aof.*'], 
                                  capture_output=True, text=True, shell=True)
            if result.returncode == 0:
                lines = result.stdout.strip().split('\n')
                for line in lines:
                    if line.strip():
                        size_str, path = line.split('\t')
                        size = int(size_str)
                        shard_num = path.split('shard-')[1] if 'shard-' in path else '?'
                        total_size += size
                        print(f"📊 AOF shard-{shard_num}: {size:,} bytes")
                        aof_files.append(path)
        except Exception as e:
            print(f"Error getting AOF sizes: {e}")
    else:
        # Normal path checking
        for shard in [0, 1]:
            aof_path = f"logs/redis.aof.shard-{shard}"
            if os.path.exists(aof_path):
                size = os.path.getsize(aof_path)
                total_size += size
                print(f"📊 AOF shard-{shard}: {size:,} bytes")
                aof_files.append(aof_path)
    
    print(f"📊 Total AOF size: {total_size:,} bytes")
    print(f"🎯 Rewrite threshold: 1,048,576 bytes")
    
    if total_size > 1048576:
        print("🎉 AOF size EXCEEDS rewrite threshold!")
        print(f"   → Size is {total_size / 1048576:.1f}x larger than threshold")
        print("   → AOF rewrite should be triggered!")
        
        # Show some stats
        if aof_files:
            print(f"\n📁 AOF files found: {len(aof_files)}")
            for f in aof_files:
                print(f"   • {f}")
    else:
        print("ℹ️  Still below threshold, may need more data")

if __name__ == "__main__":
    trigger_aof_rewrite()
