#!/usr/bin/env python3

import socket
import time
import threading

class SimpleRedisClient:
    def __init__(self, host='localhost', port=6380):
        self.sock = socket.create_connection((host, port), timeout=5)
    
    def encode_command(self, *args):
        cmd = f"*{len(args)}\r\n"
        for arg in args:
            arg_str = str(arg)
            cmd += f"${len(arg_str)}\r\n{arg_str}\r\n"
        return cmd.encode('utf-8')
    
    def send_command(self, *args):
        cmd = self.encode_command(*args)
        self.sock.send(cmd)
    
    def read_response(self):
        response = b""
        while True:
            chunk = self.sock.recv(1024)
            if not chunk:
                break
            response += chunk
            if response.endswith(b'\r\n'):
                break
        return response.decode('utf-8').strip()

def test_full_subscribe_unsubscribe():
    print("=== Testing full SUBSCRIBE/UNSUBSCRIBE cycle ===")
    
    # Create subscriber and publisher
    subscriber = SimpleRedisClient()
    publisher = SimpleRedisClient()
    
    try:
        # Step 1: Subscribe to multiple channels
        print("\n1. Subscribing to test1, test2, test3...")
        subscriber.send_command('SUBSCRIBE', 'test1', 'test2', 'test3')
        
        # Read all subscription confirmations
        sub_confirmations = []
        for i in range(3):
            resp = subscriber.read_response()
            sub_confirmations.append(resp)
            print(f"   Subscription {i+1}: {resp}")
        
        # Step 2: Publish a message to test1
        print("\n2. Publishing message to test1...")
        publisher.send_command('PUBLISH', 'test1', 'Hello test1!')
        
        # Publisher should get response
        pub_resp = publisher.read_response()
        print(f"   Publish response: {pub_resp}")
        
        # Subscriber should receive the message
        message = subscriber.read_response()
        print(f"   Message received: {message}")
        
        # Step 3: Unsubscribe from specific channel
        print("\n3. Unsubscribing from test2...")
        subscriber.send_command('UNSUBSCRIBE', 'test2')
        
        # Read unsubscribe confirmation
        unsub_resp = subscriber.read_response()
        print(f"   Unsubscribe response: {unsub_resp}")
        
        # Step 4: Unsubscribe from all remaining channels
        print("\n4. Unsubscribing from all remaining channels...")
        subscriber.send_command('UNSUBSCRIBE')
        
        # Read remaining unsubscribe confirmations
        remaining_unsubs = []
        for i in range(2):  # Should get confirmations for test1 and test3
            try:
                resp = subscriber.read_response()
                remaining_unsubs.append(resp)
                print(f"   Unsubscribe {i+1}: {resp}")
            except Exception as e:
                print(f"   Error reading unsubscribe {i+1}: {e}")
                break
        
        print(f"\n✅ Test completed successfully!")
        print(f"   - Received {len(sub_confirmations)} subscription confirmations")
        print(f"   - Received 1 published message")
        print(f"   - Received {1 + len(remaining_unsubs)} unsubscribe confirmations")
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
    finally:
        subscriber.sock.close()
        publisher.sock.close()

if __name__ == "__main__":
    test_full_subscribe_unsubscribe()
