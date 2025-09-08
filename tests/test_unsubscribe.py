#!/usr/bin/env python3

import socket
import time
import threading
import unittest
import subprocess
import sys

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

class TestUnsubscribe(unittest.TestCase):
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
        """Create clients for each test"""
        self.subscriber = RedisClient()
        self.publisher = RedisClient()

    def tearDown(self):
        """Clean up clients"""
        self.subscriber.close()
        self.publisher.close()

    def test_01_unsubscribe_specific_channels(self):
        """Test unsubscribing from specific channels"""
        print("\n=== Testing UNSUBSCRIBE from specific channels ===")
        
        messages_received = []
        
        def subscriber_thread():
            try:
                # Subscribe to multiple channels
                print("Subscribing to channel1, channel2, channel3")
                self.subscriber.execute('SUBSCRIBE', 'channel1', 'channel2', 'channel3')
                
                # Wait for a message on channel1
                msg1 = self.subscriber.decode_response()
                messages_received.append(msg1)
                print(f"Received: {msg1}")
                
                # Unsubscribe from channel2
                print("Unsubscribing from channel2")
                self.subscriber.execute('UNSUBSCRIBE', 'channel2')
                
                # Wait for another message on channel1 
                msg2 = self.subscriber.decode_response()
                messages_received.append(msg2)
                print(f"Received: {msg2}")
                
            except Exception as e:
                print(f"Subscriber error: {e}")
        
        # Start subscriber
        thread = threading.Thread(target=subscriber_thread)
        thread.start()
        
        # Give time to subscribe
        time.sleep(1)
        
        # Publish to channel1 (should be received)
        count1 = self.publisher.execute('PUBLISH', 'channel1', 'message1')
        print(f"Published to channel1, subscribers: {count1}")
        
        # Give time for unsubscribe
        time.sleep(1)
        
        # Publish to channel1 again (should be received)
        count2 = self.publisher.execute('PUBLISH', 'channel1', 'message2')
        print(f"Published to channel1 again, subscribers: {count2}")
        
        # Publish to channel2 (should NOT be received)
        count3 = self.publisher.execute('PUBLISH', 'channel2', 'message3')
        print(f"Published to channel2, subscribers: {count3}")
        
        # Wait for subscriber
        thread.join(timeout=5)
        
        print(f"Messages received: {messages_received}")
        self.assertEqual(count1, 1, "Should have 1 subscriber for channel1")
        self.assertEqual(count2, 1, "Should still have 1 subscriber for channel1")
        self.assertEqual(count3, 0, "Should have 0 subscribers for channel2 after unsubscribe")
        print("✓ Specific channel unsubscribe working")

    def test_02_unsubscribe_all_channels(self):
        """Test unsubscribing from all channels"""
        print("\n=== Testing UNSUBSCRIBE from all channels ===")
        
        messages_received = []
        unsubscribe_done = threading.Event()
        
        def subscriber_thread():
            try:
                # Subscribe to multiple channels
                print("Subscribing to test1, test2")
                self.subscriber.execute('SUBSCRIBE', 'test1', 'test2')
                
                # Wait for one message
                msg1 = self.subscriber.decode_response()
                messages_received.append(msg1)
                print(f"Received: {msg1}")
                
                # Signal that we're ready for unsubscribe
                unsubscribe_done.set()
                
                # Unsubscribe from all channels
                print("Unsubscribing from all channels")
                self.subscriber.execute('UNSUBSCRIBE')
                
            except Exception as e:
                print(f"Subscriber error: {e}")
        
        # Start subscriber
        thread = threading.Thread(target=subscriber_thread)
        thread.start()
        
        # Give time to subscribe
        time.sleep(1)
        
        # Publish to test1 (should be received)
        count1 = self.publisher.execute('PUBLISH', 'test1', 'before_unsubscribe')
        print(f"Published to test1, subscribers: {count1}")
        
        # Wait for the message to be received before unsubscribing
        unsubscribe_done.wait(timeout=3)
        
        # Give time for unsubscribe to complete
        time.sleep(1)
        
        # Publish to test1 again (should NOT be received)
        count2 = self.publisher.execute('PUBLISH', 'test1', 'after_unsubscribe')
        print(f"Published to test1 after unsubscribe, subscribers: {count2}")
        
        # Publish to test2 (should NOT be received)
        count3 = self.publisher.execute('PUBLISH', 'test2', 'after_unsubscribe')
        print(f"Published to test2 after unsubscribe, subscribers: {count3}")
        
        # Wait for subscriber
        thread.join(timeout=5)
        
        print(f"Messages received: {messages_received}")
        self.assertEqual(count1, 1, "Should have 1 subscriber before unsubscribe")
        self.assertEqual(count2, 0, "Should have 0 subscribers after unsubscribe all")
        self.assertEqual(count3, 0, "Should have 0 subscribers after unsubscribe all")
        print("✓ Unsubscribe all channels working")

    def test_03_partial_unsubscribe(self):
        """Test partial unsubscribe behavior"""
        print("\n=== Testing partial unsubscribe ===")
        
        messages_received = []
        
        def subscriber_thread():
            try:
                # Subscribe to 3 channels
                print("Subscribing to alpha, beta, gamma")
                self.subscriber.execute('SUBSCRIBE', 'alpha', 'beta', 'gamma')
                
                # Wait for messages
                for i in range(4):  # Expect 4 messages
                    msg = self.subscriber.decode_response()
                    messages_received.append(msg)
                    print(f"Received message {i+1}: {msg}")
                    
                    if i == 1:  # After second message, unsubscribe from beta
                        print("Unsubscribing from beta")
                        self.subscriber.execute('UNSUBSCRIBE', 'beta')
                
            except Exception as e:
                print(f"Subscriber error: {e}")
        
        # Start subscriber
        thread = threading.Thread(target=subscriber_thread)
        thread.start()
        
        # Give time to subscribe
        time.sleep(1)
        
        # Publish to alpha (should be received)
        self.publisher.execute('PUBLISH', 'alpha', 'alpha_msg1')
        time.sleep(0.2)
        
        # Publish to beta (should be received)
        self.publisher.execute('PUBLISH', 'beta', 'beta_msg1')
        time.sleep(0.5)  # Give time for unsubscribe
        
        # Publish to alpha again (should be received)
        self.publisher.execute('PUBLISH', 'alpha', 'alpha_msg2')
        time.sleep(0.2)
        
        # Publish to beta again (should NOT be received)
        count_beta = self.publisher.execute('PUBLISH', 'beta', 'beta_msg2')
        time.sleep(0.2)
        
        # Publish to gamma (should be received)
        self.publisher.execute('PUBLISH', 'gamma', 'gamma_msg1')
        
        # Wait for subscriber
        thread.join(timeout=5)
        
        print(f"Total messages received: {len(messages_received)}")
        print(f"Beta subscribers after unsubscribe: {count_beta}")
        self.assertEqual(count_beta, 0, "Beta should have no subscribers after partial unsubscribe")
        print("✓ Partial unsubscribe working")

if __name__ == '__main__':
    # Run tests with high verbosity
    unittest.main(verbosity=2, buffer=True)
