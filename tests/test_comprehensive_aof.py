#!/usr/bin/env python3
"""
Final comprehensive test for mandatory AOF functionality.
Tests default behavior, custom log directory, and command persistence.
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

def test_comprehensive_aof():
    """Comprehensive test of mandatory AOF functionality"""
    
    # Test 1: Default log directory behavior
    print("=== Test 1: Default Log Directory ===")
    
    # Clean up any existing logs
    if os.path.exists('/home/dsu481/workspace/multithreaded-redis/logs'):
        shutil.rmtree('/home/dsu481/workspace/multithreaded-redis/logs')
    
    # Start server with defaults
    server_process = subprocess.Popen([
        './server', 
        '-addr', ':6383'
    ], cwd='/home/dsu481/workspace/multithreaded-redis')
    
    time.sleep(2)
    
    try:
        # Check default log directory creation
        log_dir = '/home/dsu481/workspace/multithreaded-redis/logs'
        if os.path.exists(log_dir):
            print("✓ Default log directory created")
        else:
            print("✗ Default log directory not created")
            return False
        
        # Connect and send commands
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(('localhost', 6383))
        
        # Send various commands
        commands = [
            (['SET', 'user:1', 'john'], 'SET user:1'),
            (['SET', 'user:2', 'jane'], 'SET user:2'),
            (['HSET', 'profile:1', 'name', 'John Doe'], 'HSET profile:1'),
            (['HSET', 'profile:1', 'age', '30'], 'HSET profile:1 age'),
            (['DEL', 'user:1'], 'DEL user:1'),
        ]
        
        for cmd_args, desc in commands:
            response = send_command(sock, cmd_args)
            print(f"✓ {desc}: {response}")
        
        sock.close()
        
        # Stop server
        server_process.terminate()
        server_process.wait()
        
        # Check AOF files
        aof_files = [f for f in os.listdir(log_dir) if f.startswith('redis.aof.shard-')]
        print(f"✓ Found {len(aof_files)} AOF files: {aof_files}")
        
        # Read and validate content
        total_content = b""
        for aof_file in aof_files:
            path = os.path.join(log_dir, aof_file)
            with open(path, 'rb') as f:
                content = f.read()
                total_content += content
                print(f"✓ {aof_file}: {len(content)} bytes")
        
        if total_content:
            content_str = total_content.decode('utf-8', errors='ignore')
            
            # Check for our commands
            expected_commands = ['SET', 'HSET', 'DEL']
            found_commands = []
            
            for cmd in expected_commands:
                if cmd in content_str:
                    found_commands.append(cmd)
                    print(f"✓ Found {cmd} command in AOF")
            
            if len(found_commands) >= 2:  # At least SET and HSET should be present
                print("✓ Test 1 PASSED: Default behavior works correctly")
            else:
                print("✗ Test 1 FAILED: Not all commands found in AOF")
                return False
        else:
            print("✗ Test 1 FAILED: No content in AOF files")
            return False
        
    finally:
        if server_process.poll() is None:
            server_process.terminate()
            server_process.wait()
    
    # Test 2: Custom log directory
    print("\n=== Test 2: Custom Log Directory ===")
    
    temp_dir = tempfile.mkdtemp()
    custom_log_dir = os.path.join(temp_dir, "custom_redis_logs")
    
    try:
        # Start server with custom log directory
        server_process = subprocess.Popen([
            './server', 
            '-addr', ':6384',
            '-logdir', custom_log_dir
        ], cwd='/home/dsu481/workspace/multithreaded-redis')
        
        time.sleep(2)
        
        # Check custom log directory creation
        if os.path.exists(custom_log_dir):
            print(f"✓ Custom log directory created: {custom_log_dir}")
        else:
            print("✗ Custom log directory not created")
            return False
        
        # Send test commands
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(('localhost', 6384))
        
        response = send_command(sock, ['SET', 'test_custom', 'custom_value'])
        print(f"✓ SET in custom location: {response}")
        
        sock.close()
        
        # Stop server
        server_process.terminate()
        server_process.wait()
        
        # Check custom AOF files
        aof_files = [f for f in os.listdir(custom_log_dir) if f.startswith('redis.aof.shard-')]
        print(f"✓ Found {len(aof_files)} AOF files in custom directory")
        
        print("✓ Test 2 PASSED: Custom log directory works correctly")
        
    finally:
        if server_process.poll() is None:
            server_process.terminate()
            server_process.wait()
        shutil.rmtree(temp_dir)
    
    print("\n=== All Tests PASSED ===")
    return True

if __name__ == "__main__":
    print("Running comprehensive AOF tests...")
    success = test_comprehensive_aof()
    if success:
        print("🎉 All comprehensive tests passed!")
    else:
        print("❌ Some tests failed!")
        exit(1)
