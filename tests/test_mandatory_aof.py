#!/usr/bin/env python3
"""
Test script for mandatory AOF with log directory.
This tests that the server creates AOF files in the specified log directory.
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

def test_mandatory_aof_logdir():
    """Test server with mandatory AOF in log directory"""
    
    # Create a temporary directory for logs
    temp_dir = tempfile.mkdtemp()
    log_dir = os.path.join(temp_dir, "redis_logs")
    
    try:
        print(f"Testing with log directory: {log_dir}")
        
        # Start the server with custom log directory
        server_process = subprocess.Popen([
            './server', 
            '-addr', ':6382',  # Use different port
            '-logdir', log_dir
        ], cwd='/home/dsu481/workspace/multithreaded-redis')
        
        # Give server time to start
        time.sleep(2)
        
        print("Checking that log directory was created...")
        if not os.path.exists(log_dir):
            print("✗ Log directory was not created!")
            return False
        print(f"✓ Log directory created: {log_dir}")
        
        print("Connecting to server...")
        
        # Connect to server
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(('localhost', 6382))
        
        try:
            # Test some commands
            print("Sending test commands...")
            
            # SET command
            cmd = encode_resp_array(['SET', 'test_key', 'test_value'])
            sock.send(cmd)
            response = read_resp_response(sock)
            print(f"SET response: {response}")
            
            # HSET command  
            cmd = encode_resp_array(['HSET', 'test_hash', 'field1', 'value1'])
            sock.send(cmd)
            response = read_resp_response(sock)
            print(f"HSET response: {response}")
            
            print("Commands sent successfully!")
            
        finally:
            sock.close()
        
        # Give server time to write to AOF
        time.sleep(1)
        
        # Stop the server
        print("Stopping server...")
        server_process.terminate()
        server_process.wait()
        
        # Check for AOF files in log directory
        print("Checking for AOF files in log directory...")
        
        aof_files = []
        for filename in os.listdir(log_dir):
            if filename.startswith('redis.aof.shard-'):
                aof_path = os.path.join(log_dir, filename)
                aof_files.append(aof_path)
                print(f"Found AOF file: {filename}")
        
        if not aof_files:
            print("✗ No AOF files found in log directory!")
            return False
        
        print(f"✓ Found {len(aof_files)} AOF files in log directory")
        
        # Validate AOF content
        total_content = b""
        for aof_file in aof_files:
            with open(aof_file, 'rb') as f:
                content = f.read()
                total_content += content
                print(f"AOF file {os.path.basename(aof_file)} size: {len(content)} bytes")
        
        if not total_content:
            print("✗ AOF files are empty!")
            return False
        
        # Check RESP format
        if total_content.startswith(b'*'):
            print("✓ AOF content uses proper RESP format")
        else:
            print("✗ AOF content is not in RESP format")
            return False
        
        # Check for our commands
        content_str = total_content.decode('utf-8', errors='ignore')
        
        if 'SET' in content_str and 'test_key' in content_str:
            print("✓ SET command found in AOF")
        else:
            print("✗ SET command not found in AOF")
            return False
        
        if 'HSET' in content_str and 'test_hash' in content_str:
            print("✓ HSET command found in AOF")
        else:
            print("✗ HSET command not found in AOF")
            return False
        
        print("✓ All tests passed!")
        return True
        
    except Exception as e:
        print(f"Test failed with error: {e}")
        return False
        
    finally:
        # Clean up
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)

if __name__ == "__main__":
    print("Testing mandatory AOF with log directory...")
    success = test_mandatory_aof_logdir()
    if success:
        print("All tests passed!")
    else:
        print("Some tests failed!")
        exit(1)
