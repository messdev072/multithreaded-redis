#!/usr/bin/env python3
"""
Test script to verify that all handlers have proper authentication and permission checks.
This script tests various Redis commands to ensure ACL enforcement works correctly.
"""

import redis
import time
import sys
import subprocess

def test_handler_authentication():
    """Test that all handlers require authentication and check permissions."""
    
    # Start server with ACL enabled
    print("Starting Redis server with ACL enabled...")
    server_process = subprocess.Popen(
        ["./server", "-require-auth"],
        cwd="/home/dsu481/workspace/multithreaded-redis",
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    
    # Wait for server to start
    time.sleep(2)
    
    try:
        # Test 1: Connection without authentication should fail for data commands
        print("\n=== Test 1: Commands without authentication ===")
        r = redis.Redis(host='localhost', port=6380, decode_responses=True)
        
        test_commands = [
            ("SET", lambda: r.set("test_key", "test_value")),
            ("GET", lambda: r.get("test_key")),
            ("DEL", lambda: r.delete("test_key")),
            ("TTL", lambda: r.ttl("test_key")),
            ("SADD", lambda: r.sadd("test_set", "member1")),
            ("SMEMBERS", lambda: r.smembers("test_set")),
            ("HSET", lambda: r.hset("test_hash", "field", "value")),
            ("HGET", lambda: r.hget("test_hash", "field")),
            ("LPUSH", lambda: r.lpush("test_list", "item")),
            ("LRANGE", lambda: r.lrange("test_list", 0, -1)),
        ]
        
        auth_required_count = 0
        for cmd_name, cmd_func in test_commands:
            try:
                cmd_func()
                print(f"  ❌ {cmd_name}: No authentication required (SECURITY ISSUE!)")
            except redis.ResponseError as e:
                if ("NOAUTH" in str(e) or "Authentication required" in str(e) or 
                    "user default is disabled" in str(e) or "connection state not found" in str(e)):
                    print(f"  ✅ {cmd_name}: Properly requires authentication")
                    auth_required_count += 1
                else:
                    print(f"  ❓ {cmd_name}: Unexpected error: {e}")
            except Exception as e:
                print(f"  ❓ {cmd_name}: Connection/other error: {e}")
        
        print(f"\nAuthentication enforcement: {auth_required_count}/{len(test_commands)} commands properly protected")
        
        # Test 2: Authentication with limited user
        print("\n=== Test 2: Limited user permissions ===")
        
        # Authenticate as read-only user
        try:
            r.execute_command("AUTH", "readonly", "readpass")
            print("  ✅ Successfully authenticated as readonly user")
        except Exception as e:
            print(f"  ❌ Failed to authenticate as readonly user: {e}")
            return False
        
        # Test read operations (should work)
        read_success_count = 0
        read_commands = [
            ("GET", lambda: r.get("nonexistent_key")),
            ("TTL", lambda: r.ttl("nonexistent_key")),
            ("SMEMBERS", lambda: r.smembers("nonexistent_set")),
            ("HGET", lambda: r.hget("nonexistent_hash", "field")),
            ("LRANGE", lambda: r.lrange("nonexistent_list", 0, -1)),
        ]
        
        for cmd_name, cmd_func in read_commands:
            try:
                result = cmd_func()
                print(f"  ✅ {cmd_name}: Read operation allowed (result: {result})")
                read_success_count += 1
            except redis.ResponseError as e:
                print(f"  ❌ {cmd_name}: Read operation denied: {e}")
            except Exception as e:
                print(f"  ❓ {cmd_name}: Unexpected error: {e}")
        
        # Test write operations (should fail)
        write_denied_count = 0
        write_commands = [
            ("SET", lambda: r.set("readonly_test", "should_fail")),
            ("DEL", lambda: r.delete("readonly_test")),
            ("SADD", lambda: r.sadd("readonly_set", "member")),
            ("HSET", lambda: r.hset("readonly_hash", "field", "value")),
            ("LPUSH", lambda: r.lpush("readonly_list", "item")),
        ]
        
        for cmd_name, cmd_func in write_commands:
            try:
                cmd_func()
                print(f"  ❌ {cmd_name}: Write operation allowed (PERMISSION ISSUE!)")
            except redis.ResponseError as e:
                if ("NOPERM" in str(e) or "no permission" in str(e).lower() or "denied" in str(e).lower() or
                    "not allowed to execute" in str(e) or "category -write" in str(e)):
                    print(f"  ✅ {cmd_name}: Write operation properly denied")
                    write_denied_count += 1
                else:
                    print(f"  ❓ {cmd_name}: Unexpected error: {e}")
            except Exception as e:
                print(f"  ❓ {cmd_name}: Connection error: {e}")
        
        print(f"\nRead permissions: {read_success_count}/{len(read_commands)} commands allowed")
        print(f"Write permissions: {write_denied_count}/{len(write_commands)} commands properly denied")
        
        # Test 3: Admin operations
        print("\n=== Test 3: Admin operations ===")
        admin_commands = [
            ("ADDNODE", lambda: r.execute_command("ADDNODE", "test-node")),
            ("REMOVENODE", lambda: r.execute_command("REMOVENODE", "test-node")),
        ]
        
        admin_denied_count = 0
        for cmd_name, cmd_func in admin_commands:
            try:
                cmd_func()
                print(f"  ❌ {cmd_name}: Admin operation allowed for readonly user (SECURITY ISSUE!)")
            except redis.ResponseError as e:
                if ("NOPERM" in str(e) or "no permission" in str(e).lower() or "denied" in str(e).lower() or
                    "not allowed to execute" in str(e) or "category" in str(e)):
                    print(f"  ✅ {cmd_name}: Admin operation properly denied")
                    admin_denied_count += 1
                else:
                    print(f"  ❓ {cmd_name}: Unexpected error: {e}")
            except Exception as e:
                print(f"  ❓ {cmd_name}: Connection error: {e}")
        
        print(f"\nAdmin permissions: {admin_denied_count}/{len(admin_commands)} commands properly denied")
        
        # Summary
        print("\n=== SUMMARY ===")
        print(f"Authentication: {auth_required_count}/{len(test_commands)} handlers require auth")
        print(f"Read permissions: {read_success_count}/{len(read_commands)} read commands work")
        print(f"Write permissions: {write_denied_count}/{len(write_commands)} write commands denied")
        print(f"Admin permissions: {admin_denied_count}/{len(admin_commands)} admin commands denied")
        
        # Check if all tests passed
        if (auth_required_count == len(test_commands) and 
            read_success_count == len(read_commands) and 
            write_denied_count == len(write_commands) and 
            admin_denied_count == len(admin_commands)):
            print("\n🎉 ALL TESTS PASSED! Handler authentication is working correctly.")
            return True
        else:
            print("\n⚠️  Some tests failed. Please review the security implementation.")
            return False
            
    except Exception as e:
        print(f"Test failed with error: {e}")
        return False
    finally:
        # Stop server
        server_process.terminate()
        server_process.wait(timeout=5)
        print("\nServer stopped.")

if __name__ == "__main__":
    success = test_handler_authentication()
    sys.exit(0 if success else 1)
