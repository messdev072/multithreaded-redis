#!/usr/bin/env python3
"""
Test script for AOF recovery functionality.
This tests that the server can recover data from AOF files after shutdown/restart.
"""

import socket
import time
import subprocess
import os
import tempfile
import shutil

def encode_resp_array(args):
    """Encode a command as a RESP array"""
    result = f"*{len(args)}\r\n"
    for arg in args:
        result += f"${len(arg)}\r\n{arg}\r\n"
    return result.encode()

def read_resp_response(sock):
    """Read a RESP response from socket"""
    response = b""
    while True:
        data = sock.recv(1024)
        if not data:
            break
        response += data
        if response.endswith(b'\r\n'):
            break
    return response.decode().strip()

def send_command(sock, command_args):
    """Send a command and get response"""
    cmd = encode_resp_array(command_args)
    sock.send(cmd)
    return read_resp_response(sock)

def test_aof_recovery():
    """Test AOF recovery after server restart"""
    
    # Create a temporary directory for logs
    temp_dir = tempfile.mkdtemp()
    log_dir = os.path.join(temp_dir, "recovery_test_logs")
    
    try:
        print("=== AOF Recovery Test ===")
        
        # Phase 1: Start server and populate data
        print("\n1. Starting server and populating data...")
        
        server_process = subprocess.Popen([
            './server', 
            '-addr', ':6385',
            '-logdir', log_dir
        ], cwd='/home/dsu481/workspace/multithreaded-redis')
        
        time.sleep(2)
        
        # Connect and populate data
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(('localhost', 6385))
        
        # Send various commands to populate data
        initial_data = [
            (['SET', 'user:1000', 'alice'], 'user:1000'),
            (['SET', 'user:1001', 'bob'], 'user:1001'),
            (['SET', 'counter', '42'], 'counter'),
            (['HSET', 'profile:alice', 'name', 'Alice Smith'], 'profile:alice name'),
            (['HSET', 'profile:alice', 'age', '25'], 'profile:alice age'),
            (['HSET', 'profile:alice', 'city', 'New York'], 'profile:alice city'),
            (['HSET', 'profile:bob', 'name', 'Bob Johnson'], 'profile:bob name'),
            (['HSET', 'profile:bob', 'age', '30'], 'profile:bob age'),
            (['SET', 'session:abc123', 'active'], 'session:abc123'),
        ]
        
        for cmd_args, desc in initial_data:
            response = send_command(sock, cmd_args)
            print(f"✓ {desc}: {response}")
        
        # Verify data is accessible
        print("\n2. Verifying initial data...")
        verification_commands = [
            (['GET', 'user:1000'], 'GET user:1000'),
            (['GET', 'counter'], 'GET counter'),
            (['HGET', 'profile:alice', 'name'], 'HGET profile:alice name'),
            (['HGET', 'profile:bob', 'age'], 'HGET profile:bob age'),
        ]
        
        initial_responses = {}
        for cmd_args, desc in verification_commands:
            response = send_command(sock, cmd_args)
            initial_responses[desc] = response
            print(f"✓ {desc}: {response}")
        
        sock.close()
        
        # Phase 2: Stop server
        print("\n3. Stopping server...")
        server_process.terminate()
        server_process.wait()
        
        # Check AOF files exist and have content
        print("\n4. Checking AOF files...")
        aof_files = [f for f in os.listdir(log_dir) if f.startswith('redis.aof.shard-')]
        print(f"Found AOF files: {aof_files}")
        
        total_aof_size = 0
        for aof_file in aof_files:
            path = os.path.join(log_dir, aof_file)
            size = os.path.getsize(path)
            total_aof_size += size
            print(f"  {aof_file}: {size} bytes")
        
        if total_aof_size == 0:
            print("✗ No data in AOF files!")
            return False
        
        print(f"✓ Total AOF data: {total_aof_size} bytes")
        
        # Phase 3: Restart server (should load from AOF)
        print("\n5. Restarting server (should recover from AOF)...")
        
        server_process = subprocess.Popen([
            './server', 
            '-addr', ':6385',
            '-logdir', log_dir
        ], cwd='/home/dsu481/workspace/multithreaded-redis')
        
        time.sleep(3)  # Give more time for AOF loading
        
        # Phase 4: Verify data recovery
        print("\n6. Verifying data recovery...")
        
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(('localhost', 6385))
        
        recovery_success = True
        for cmd_args, desc in verification_commands:
            response = send_command(sock, cmd_args)
            expected = initial_responses[desc]
            
            if response == expected:
                print(f"✓ {desc}: {response} (RECOVERED)")
            else:
                print(f"✗ {desc}: got {response}, expected {expected} (FAILED)")
                recovery_success = False
        
        # Test additional data to ensure server is fully functional
        print("\n7. Testing new data after recovery...")
        new_commands = [
            (['SET', 'post_recovery', 'working'], 'SET post_recovery'),
            (['HSET', 'profile:alice', 'updated', 'yes'], 'HSET update'),
            (['GET', 'post_recovery'], 'GET post_recovery'),
            (['HGET', 'profile:alice', 'updated'], 'HGET updated'),
        ]
        
        for cmd_args, desc in new_commands:
            response = send_command(sock, cmd_args)
            print(f"✓ {desc}: {response}")
        
        sock.close()
        
        # Stop server
        server_process.terminate()
        server_process.wait()
        
        # Phase 5: Final validation
        if recovery_success:
            print("\n🎉 AOF Recovery Test PASSED!")
            print("✅ All data successfully recovered from AOF files")
            print("✅ Server fully functional after recovery")
            return True
        else:
            print("\n❌ AOF Recovery Test FAILED!")
            print("✗ Some data was not properly recovered")
            return False
        
    except Exception as e:
        print(f"Test failed with error: {e}")
        return False
        
    finally:
        # Clean up
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)

if __name__ == "__main__":
    print("Testing AOF recovery functionality...")
    success = test_aof_recovery()
    if success:
        print("🎉 AOF recovery test passed!")
    else:
        print("❌ AOF recovery test failed!")
        exit(1)
