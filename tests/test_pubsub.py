#!/usr/bin/env python3

import socket
import time
import threading
import unittest
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
            raise

    def close(self):
        if self.sock:
            self.sock.close()

class TestPubSub(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """Start the Redis server once for all tests"""
        print("Starting Redis server...")
        cls.server_process = subprocess.Popen(
            ['./server'],
            cwd='/home/dsu481/workspace/multithreaded-redis',
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        # Give server time to start
        time.sleep(2)
        
        # Check if server started successfully
        if cls.server_process.poll() is not None:
            stdout, stderr = cls.server_process.communicate()
            print(f"Server failed to start. stdout: {stdout.decode()}, stderr: {stderr.decode()}")
            sys.exit(1)
        
        print("Redis server started successfully")

    @classmethod
    def tearDownClass(cls):
        """Stop the Redis server"""
        print("Stopping Redis server...")
        cls.server_process.terminate()
        cls.server_process.wait()
        print("Redis server stopped")

    def setUp(self):
        """Create a new client for each test"""
        self.client = RedisClient()
        self.subscriber_client = RedisClient()

    def tearDown(self):
        """Clean up clients"""
        self.client.close()
        self.subscriber_client.close()

    def test_01_basic_publish(self):
        """Test basic PUBLISH command"""
        print("\n=== Testing basic PUBLISH ===")
        
        # Publish to a channel with no subscribers
        response = self.client.execute('PUBLISH', 'test-channel', 'hello world')
        self.assertEqual(response, 0, "Should return 0 when no subscribers")
        print("✓ PUBLISH with no subscribers returns 0")

    def test_02_subscribe_and_publish(self):
        """Test SUBSCRIBE and PUBLISH interaction"""
        print("\n=== Testing SUBSCRIBE and PUBLISH ===")
        
        messages_received = []
        
        def subscriber_thread():
            try:
                # Subscribe to a channel
                response = self.subscriber_client.execute('SUBSCRIBE', 'test-channel')
                print(f"Subscribe response: {response}")
                
                # Listen for messages
                for i in range(2):  # Expect 2 messages
                    message = self.subscriber_client.decode_response()
                    messages_received.append(message)
                    print(f"Received message: {message}")
                    
            except Exception as e:
                print(f"Subscriber error: {e}")
        
        # Start subscriber in background
        subscriber = threading.Thread(target=subscriber_thread)
        subscriber.start()
        
        # Give subscriber time to subscribe
        time.sleep(1)
        
        # Publish messages
        response1 = self.client.execute('PUBLISH', 'test-channel', 'message1')
        self.assertEqual(response1, 1, "Should return 1 when one subscriber")
        
        response2 = self.client.execute('PUBLISH', 'test-channel', 'message2')
        self.assertEqual(response2, 1, "Should return 1 when one subscriber")
        
        # Wait for subscriber to receive messages
        subscriber.join(timeout=5)
        
        print(f"Messages received: {messages_received}")
        print("✓ Subscribe and publish working")

    def test_03_multiple_subscribers(self):
        """Test multiple subscribers to same channel"""
        print("\n=== Testing multiple subscribers ===")
        
        # Create additional subscriber client
        subscriber2 = RedisClient()
        
        try:
            messages1 = []
            messages2 = []
            
            def subscriber1_thread():
                try:
                    self.subscriber_client.execute('SUBSCRIBE', 'multi-channel')
                    for i in range(2):
                        message = self.subscriber_client.decode_response()
                        messages1.append(message)
                except Exception as e:
                    print(f"Subscriber1 error: {e}")
            
            def subscriber2_thread():
                try:
                    subscriber2.execute('SUBSCRIBE', 'multi-channel')
                    for i in range(2):
                        message = subscriber2.decode_response()
                        messages2.append(message)
                except Exception as e:
                    print(f"Subscriber2 error: {e}")
            
            # Start both subscribers
            thread1 = threading.Thread(target=subscriber1_thread)
            thread2 = threading.Thread(target=subscriber2_thread)
            
            thread1.start()
            thread2.start()
            
            # Give subscribers time to subscribe
            time.sleep(1)
            
            # Publish a message
            response = self.client.execute('PUBLISH', 'multi-channel', 'broadcast message')
            self.assertEqual(response, 2, "Should return 2 when two subscribers")
            
            # Wait for threads
            thread1.join(timeout=3)
            thread2.join(timeout=3)
            
            print(f"Subscriber1 messages: {messages1}")
            print(f"Subscriber2 messages: {messages2}")
            print("✓ Multiple subscribers working")
            
        finally:
            subscriber2.close()

    def test_04_different_channels(self):
        """Test subscribing to different channels"""
        print("\n=== Testing different channels ===")
        
        messages_channel1 = []
        messages_channel2 = []
        
        # Create another client for second channel
        client2 = RedisClient()
        
        try:
            def subscriber1_thread():
                try:
                    self.subscriber_client.execute('SUBSCRIBE', 'channel1')
                    message = self.subscriber_client.decode_response()
                    messages_channel1.append(message)
                except Exception as e:
                    print(f"Subscriber1 error: {e}")
            
            def subscriber2_thread():
                try:
                    client2.execute('SUBSCRIBE', 'channel2')
                    message = client2.decode_response()
                    messages_channel2.append(message)
                except Exception as e:
                    print(f"Subscriber2 error: {e}")
            
            # Start subscribers
            thread1 = threading.Thread(target=subscriber1_thread)
            thread2 = threading.Thread(target=subscriber2_thread)
            
            thread1.start()
            thread2.start()
            
            # Give time to subscribe
            time.sleep(1)
            
            # Publish to different channels
            response1 = self.client.execute('PUBLISH', 'channel1', 'message for channel1')
            response2 = self.client.execute('PUBLISH', 'channel2', 'message for channel2')
            
            self.assertEqual(response1, 1)
            self.assertEqual(response2, 1)
            
            # Wait for messages
            thread1.join(timeout=3)
            thread2.join(timeout=3)
            
            print(f"Channel1 messages: {messages_channel1}")
            print(f"Channel2 messages: {messages_channel2}")
            print("✓ Different channels working independently")
            
        finally:
            client2.close()

if __name__ == '__main__':
    # Run tests with high verbosity
    unittest.main(verbosity=2, buffer=True)
