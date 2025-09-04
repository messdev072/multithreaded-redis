import socket
import time
import unittest
import random
import string

class RedisClient:
    def __init__(self, host='localhost', port=6380):
        self.sock = socket.create_connection((host, port))
    
    def encode_command(self, *args):
        """Encode command using RESP protocol"""
        cmd = f"*{len(args)}\r\n"
        for arg in args:
            arg_bytes = str(arg).encode()
            cmd += f"${len(arg_bytes)}\r\n{arg}\r\n"
        return cmd.encode()

    def decode_response(self):
        """Decode RESP response"""
        data = self.sock.recv(4096).decode()
        if not data:
            raise Exception("Empty response")
        
        if data.startswith('+'):  # Simple String
            return data[1:].rstrip('\r\n')
        elif data.startswith('-'):  # Error
            raise Exception(data[1:].rstrip('\r\n'))
        elif data.startswith(':'):  # Integer
            return int(data[1:].rstrip('\r\n'))
        elif data.startswith('$'):  # Bulk String
            if data.startswith('$-1'):
                return None
            return data[data.find('\r\n')+2:-2]
        return data.rstrip('\r\n')

    def execute(self, *args):
        self.sock.sendall(self.encode_command(*args))
        return self.decode_response()

    def close(self):
        self.sock.close()

class TestSharding(unittest.TestCase):
    def setUp(self):
        self.client = RedisClient()
        # Generate random keys and values
        self.test_data = {}
        for i in range(1000):
            key = ''.join(random.choices(string.ascii_letters, k=10))
            value = ''.join(random.choices(string.ascii_letters, k=20))
            self.test_data[key] = value

    def tearDown(self):
        self.client.close()

    def test_01_initial_sharding(self):
        """Test initial sharding with 2 nodes"""
        print("\nTesting initial sharding with shard-1 and shard-2...")
        
        # Write test data
        for key, value in self.test_data.items():
            response = self.client.execute('SET', key, value)
            self.assertEqual(response, 'OK')
            
        # Verify data
        for key, value in self.test_data.items():
            response = self.client.execute('GET', key)
            self.assertEqual(response, value)

    def test_02_add_shard(self):
        """Test adding shard-3"""
        print("\nTesting addition of shard-3...")
        
        # Add shard-3 (this should trigger migration in the background)
        response = self.client.execute('ADDNODE', 'shard-3')
        self.assertEqual(response, 'OK')
        
        # Wait for migration to start
        time.sleep(1)
        
        # Write new data while migration is happening
        new_data = {}
        for i in range(5):
            key = f'new-key-{i}'
            value = f'new-value-{i}'
            new_data[key] = value
            response = self.client.execute('SET', key, value)
            self.assertEqual(response, 'OK')
        
        # Verify all data is accessible during migration
        for key, value in {**self.test_data, **new_data}.items():
            response = self.client.execute('GET', key)
            self.assertEqual(response, value)
        
        # Wait for migration to complete (you might want to add a command to check migration status)
        time.sleep(5)
        
        # Verify data after migration
        for key, value in {**self.test_data, **new_data}.items():
            response = self.client.execute('GET', key)
            self.assertEqual(response, value)

    # def test_03_remove_shard(self):
    #     """Test removing shard-1"""
    #     print("\nTesting removal of shard-1...")
        
    #     # Remove shard-1 (this should trigger migration off the node)
    #     response = self.client.execute('ADMIN', 'REMOVENODE', 'shard-1')
    #     self.assertEqual(response, 'OK')
        
    #     # Wait for migration to start
    #     time.sleep(1)
        
    #     # Write new data while migration is happening
    #     new_data = {}
    #     for i in range(100):
    #         key = f'newer-key-{i}'
    #         value = f'newer-value-{i}'
    #         new_data[key] = value
    #         response = self.client.execute('SET', key, value)
    #         self.assertEqual(response, 'OK')
        
    #     # Verify all data is accessible during migration
    #     for key, value in {**self.test_data, **new_data}.items():
    #         response = self.client.execute('GET', key)
    #         self.assertEqual(response, value)
        
    #     # Wait for migration to complete
    #     time.sleep(5)
        
    #     # Verify data after migration
    #     for key, value in {**self.test_data, **new_data}.items():
    #         response = self.client.execute('GET', key)
    #         self.assertEqual(response, value)

if __name__ == '__main__':
    unittest.main(verbosity=2)