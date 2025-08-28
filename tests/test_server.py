#!/usr/bin/env python3

import socket
import time
from typing import List, Union

class RedisClient:
    def __init__(self, host="localhost", port=6379):
        self.sock = socket.create_connection((host, port))

    def _encode_command(self, *args) -> bytes:
        """Encode command to RESP protocol"""
        parts = [f"${len(str(arg))}\r\n{str(arg)}\r\n" for arg in args]
        return f"*{len(args)}\r\n{''.join(parts)}".encode()

    def _decode_response(self) -> Union[str, bytes, int, list, None]:
        """Decode RESP response"""
        data = self.sock.recv(4096).decode()
        if not data:
            return None

        if data.startswith('+'):  # Simple String
            return data[1:].strip()
        elif data.startswith('-'):  # Error
            raise Exception(data[1:].strip())
        elif data.startswith(':'):  # Integer
            return int(data[1:].strip())
        elif data.startswith('$'):  # Bulk String
            if data[1:3] == '-1':  # Null
                return None
            return data.split('\r\n')[1]
        elif data.startswith('*'):  # Array
            if data[1:3] == '-1':  # Null array
                return None
            count = int(data[1:].split('\r\n')[0])
            result = []
            parts = data.split('\r\n')
            i = 2
            for _ in range(count):
                if parts[i-1].startswith('$'):
                    result.append(parts[i])
                    i += 2
            return result
        return data.strip()

    def execute(self, *args):
        """Execute a command and return the response"""
        self.sock.sendall(self._encode_command(*args))
        return self._decode_response()

    def close(self):
        self.sock.close()

def run_tests():
    client = RedisClient()
    
    def test(name: str, *args):
        print(f"\nTesting {name}:")
        try:
            result = client.execute(*args)
            print(f"Command: {args}")
            print(f"Response: {result}")
            return result
        except Exception as e:
            print(f"Error: {e}")
            return None

    # Test PING
    test("PING", "PING")

    # String operations
    test("SET", "SET", "mykey", "myvalue")
    test("GET", "GET", "mykey")
    test("SET with expiry", "SET", "tempkey", "tempvalue", "EX", "5")
    test("GET expired", "GET", "tempkey")
    time.sleep(6)  # Wait for key to expire
    test("GET after expire", "GET", "tempkey")

    # Set operations
    test("SADD", "SADD", "myset", "value1", "value2", "value3")
    test("SMEMBERS", "SMEMBERS", "myset")
    test("SCARD", "SCARD", "myset")
    test("SISMEMBER", "SISMEMBER", "myset", "value1")
    test("SISMEMBER", "SISMEMBER", "myset", "nonexistent")
    test("SREM", "SREM", "myset", "value2")
    test("SMEMBERS after SREM", "SMEMBERS", "myset")
    test("SADD set2", "SADD", "set2", "value1", "value4")
    test("SUNION", "SUNION", "myset", "set2")
    test("SINTER", "SINTER", "myset", "set2")
    test("SDIFF", "SDIFF", "myset", "set2")

    # Hash operations
    test("HSET", "HSET", "myhash", "field1", "value1")
    test("HGET", "HGET", "myhash", "field1")
    test("HSET multiple", "HSET", "myhash", "field2", "value2", "field3", "value3")
    test("HGETALL", "HGETALL", "myhash")
    test("HDEL", "HDEL", "myhash", "field2")
    test("HGETALL after HDEL", "HGETALL", "myhash")

    # List operations
    test("LPUSH", "LPUSH", "mylist", "value1", "value2", "value3")
    test("LLEN", "LLEN", "mylist")
    test("LRANGE", "LRANGE", "mylist", "0", "-1")
    test("LPOP", "LPOP", "mylist")
    test("RPUSH", "RPUSH", "mylist", "value4")
    test("RPOP", "RPOP", "mylist")

    # Sorted Set operations
    test("ZADD", "ZADD", "myzset", "1", "one", "2", "two", "3", "three")
    test("ZCARD", "ZCARD", "myzset")
    test("ZSCORE", "ZSCORE", "myzset", "two")
    test("ZRANK", "ZRANK", "myzset", "two")
    test("ZRANGE", "ZRANGE", "myzset", "0", "-1")
    test("ZRANGE with scores", "ZRANGE", "myzset", "0", "-1", "WITHSCORES")

    # Bloom Filter operations
    test("BF.ADD", "BF.ADD", "myfilter", "item1")
    test("BF.EXISTS true", "BF.EXISTS", "myfilter", "item1")
    test("BF.EXISTS false", "BF.EXISTS", "myfilter", "nonexistent")

    # Count-Min Sketch operations
    test("CMSINCR", "CMSINCR", "mycms", "item1", "5")
    test("CMSQUERY", "CMSQUERY", "mycms", "item1")

    # Cleanup
    test("DEL", "DEL", "mykey", "myset", "set2", "myhash", "mylist", "myzset", "myfilter", "mycms")
    
    client.close()

if __name__ == "__main__":
    run_tests()
