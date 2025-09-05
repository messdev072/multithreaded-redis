import socket
import time
import unittest
import random
import string
import threading
import subprocess
import sys
import os

class RedisClient:
    def __init__(self, host='localhost', port=6380):
        self.host = host
        self.port = port
        self.sock = None
        self.connect()
    
    def connect(self):
        try:
            self.sock = socket.create_connection((self.host, self.port), timeout=10)
        except Exception as e:
            print(f"Failed to connect to {self.host}:{self.port}: {e}")
            raise
    
    def encode_command(self, *args):
        """Encode command using RESP protocol"""
        cmd = f"*{len(args)}\r\n"
        for arg in args:
            arg_str = str(arg)
            arg_bytes = arg_str.encode('utf-8')
            cmd += f"${len(arg_bytes)}\r\n{arg_str}\r\n"
        return cmd.encode('utf-8')

    def decode_response(self):
        """Decode RESP response"""
        try:
            # Read the first line to determine response type
            first_byte = self.sock.recv(1).decode('utf-8')
            if not first_byte:
                raise Exception("Empty response")
            
            # Read the rest of the line
            line = first_byte
            while True:
                char = self.sock.recv(1).decode('utf-8')
                line += char
                if line.endswith('\r\n'):
                    break
            
            if first_byte == '+':  # Simple String
                return line[1:-2]
            elif first_byte == '-':  # Error
                raise Exception(f"Redis Error: {line[1:-2]}")
            elif first_byte == ':':  # Integer
                return int(line[1:-2])
            elif first_byte == '$':  # Bulk String
                length = int(line[1:-2])
                if length == -1:
                    return None
                data = self.sock.recv(length).decode('utf-8')
                self.sock.recv(2)  # Read trailing \r\n
                return data
            elif first_byte == '*':  # Array
                count = int(line[1:-2])
                if count == -1:
                    return None
                result = []
                for _ in range(count):
                    result.append(self.decode_response())
                return result
            else:
                raise Exception(f"Unknown response type: {first_byte}")
        except Exception as e:
            print(f"Error decoding response: {e}")
            raise

    def execute(self, *args):
        try:
            command = self.encode_command(*args)
            self.sock.sendall(command)
            return self.decode_response()
        except Exception as e:
            print(f"Error executing command {args}: {e}")
            # Try to reconnect once
            try:
                self.sock.close()
                self.connect()
                command = self.encode_command(*args)
                self.sock.sendall(command)
                return self.decode_response()
            except:
                raise e

    def close(self):
        if self.sock:
            self.sock.close()

class TestSharding(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """Start the Redis server before running tests"""
        print("Starting Redis server...")
        cls.server_process = subprocess.Popen(
            ['./server'],
            cwd='/home/dsu481/workspace/multithreaded-redis',
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        # Wait for server to start
        time.sleep(2)
        
        # Test connection
        try:
            test_client = RedisClient()
            test_client.execute('PING')
            test_client.close()
            print("Server started successfully!")
        except Exception as e:
            cls.server_process.terminate()
            raise Exception(f"Failed to start server: {e}")

    @classmethod
    def tearDownClass(cls):
        """Stop the Redis server after all tests"""
        print("Stopping Redis server...")
        cls.server_process.terminate()
        cls.server_process.wait()

    def setUp(self):
        self.client = RedisClient()
        # Generate random keys and values for testing
        self.test_data = {}
        for i in range(10):
            key = f'test-key-{i}'
            value = f'test-value-{i}-{random.randint(1000, 9999)}'
            self.test_data[key] = value

    def tearDown(self):
        self.client.close()

    def test_01_basic_operations(self):
        """Test basic SET/GET operations"""
        print("\n=== Testing basic operations ===")
        
        # Test PING
        response = self.client.execute('PING')
        self.assertEqual(response, 'PONG')
        
        # Test SET/GET
        for key, value in self.test_data.items():
            response = self.client.execute('SET', key, value)
            self.assertEqual(response, 'OK')
            
        # Verify all data
        for key, value in self.test_data.items():
            response = self.client.execute('GET', key)
            self.assertEqual(response, value, f"Key {key}: expected {value}, got {response}")
        
        print(f"✓ Successfully set and retrieved {len(self.test_data)} key-value pairs")

    def test_02_add_shard(self):
        """Test adding a new shard and key redistribution"""
        print("\n=== Testing shard addition ===")
        
        # First, set some initial data
        initial_data = {}
        for i in range(15):
            key = f'initial-key-{i}'
            value = f'initial-value-{i}'
            initial_data[key] = value
            response = self.client.execute('SET', key, value)
            self.assertEqual(response, 'OK')
        
        print(f"✓ Set {len(initial_data)} initial keys")
        
        # Add a new shard
        response = self.client.execute('ADDNODE', 'shard-2')
        self.assertEqual(response, 'OK')
        print("✓ Added new shard: shard-2")
        
        # Wait for migration to complete
        time.sleep(3)
        
        # Add more data after shard addition
        post_add_data = {}
        for i in range(10):
            key = f'post-add-key-{i}'
            value = f'post-add-value-{i}'
            post_add_data[key] = value
            response = self.client.execute('SET', key, value)
            self.assertEqual(response, 'OK')
        
        # Verify all data is still accessible
        all_data = {**initial_data, **post_add_data}
        for key, value in all_data.items():
            response = self.client.execute('GET', key)
            self.assertEqual(response, value, f"Key {key}: expected {value}, got {response}")
        
        print(f"✓ All {len(all_data)} keys are accessible after shard addition")

    def test_03_add_another_shard(self):
        """Test adding a third shard"""
        print("\n=== Testing addition of third shard ===")
        
        # Add more data
        more_data = {}
        for i in range(20):
            key = f'more-key-{i}'
            value = f'more-value-{i}'
            more_data[key] = value
            response = self.client.execute('SET', key, value)
            self.assertEqual(response, 'OK')
        
        # Add shard-3
        response = self.client.execute('ADDNODE', 'shard-3')
        self.assertEqual(response, 'OK')
        print("✓ Added third shard: shard-3")
        
        # Wait for migration
        time.sleep(3)
        
        # Verify data after adding third shard
        for key, value in more_data.items():
            response = self.client.execute('GET', key)
            self.assertEqual(response, value, f"Key {key}: expected {value}, got {response}")
        
        print(f"✓ All {len(more_data)} keys accessible after adding third shard")

    def test_04_remove_shard(self):
        """Test removing a shard and key migration"""
        print("\n=== Testing shard removal ===")
        
        # Set some data before removal
        removal_data = {}
        for i in range(15):
            key = f'removal-test-{i}'
            value = f'removal-value-{i}'
            removal_data[key] = value
            response = self.client.execute('SET', key, value)
            self.assertEqual(response, 'OK')
        
        print(f"✓ Set {len(removal_data)} keys before removal")
        
        # Remove shard-3 (the last one added, this should trigger key migration)
        # When running all tests, we should have shard-0, shard-1, shard-2, shard-3 at this point
        response = self.client.execute('REMOVENODE', 'shard-3')
        self.assertEqual(response, 'OK')
        print("✓ Removed shard: shard-3")
        
        # Wait for migration to complete
        time.sleep(3)
        
        # Verify all data is still accessible after removal
        for key, value in removal_data.items():
            response = self.client.execute('GET', key)
            self.assertEqual(response, value, f"Key {key}: expected {value}, got {response}")
        
        print(f"✓ All {len(removal_data)} keys accessible after shard removal")
        
        # Add some new data to verify the system still works
        post_removal_data = {}
        for i in range(10):
            key = f'post-removal-{i}'
            value = f'post-removal-value-{i}'
            post_removal_data[key] = value
            response = self.client.execute('SET', key, value)
            self.assertEqual(response, 'OK')
        
        # Verify new data
        for key, value in post_removal_data.items():
            response = self.client.execute('GET', key)
            self.assertEqual(response, value)
        
        print(f"✓ Successfully added and retrieved {len(post_removal_data)} new keys after removal")

    def test_05_stress_test(self):
        """Stress test with multiple operations"""
        print("\n=== Running stress test ===")
        
        # Generate larger dataset
        stress_data = {}
        for i in range(100):
            key = f'stress-{i}-{random.randint(1000, 9999)}'
            value = ''.join(random.choices(string.ascii_letters + string.digits, k=50))
            stress_data[key] = value
        
        # Set all data
        for key, value in stress_data.items():
            response = self.client.execute('SET', key, value)
            self.assertEqual(response, 'OK')
        
        # Verify all data
        for key, value in stress_data.items():
            response = self.client.execute('GET', key)
            self.assertEqual(response, value)
        
        print(f"✓ Stress test completed: {len(stress_data)} keys set and retrieved successfully")

if __name__ == '__main__':
    # Run tests with high verbosity
    unittest.main(verbosity=2, buffer=True)