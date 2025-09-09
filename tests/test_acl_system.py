#!/usr/bin/env python3

import redis
import time
import subprocess
import os
import signal
import sys

def test_acl_system():
    """Test Redis ACL authentication and authorization system"""
    
    print("🔐 Testing ACL Authentication System...")
    
    # Connect to Redis
    r = redis.Redis(host='localhost', port=6380, decode_responses=True)
    
    try:
        # Test 1: Verify server is responding
        print("\n📋 Test 1: Verify Server Response")
        r.ping()
        print("✅ Server is responding")
        
        # Test 2: Test default user access (should work without authentication)
        print("\n📋 Test 2: Test Default User Access")
        r.set("test_key", "test_value")
        value = r.get("test_key")
        if value == "test_value":
            print("✅ Default user can read/write without authentication")
        else:
            print("❌ Default user access failed")
            return False
        
        # Test 3: Test ACL commands
        print("\n📋 Test 3: Test ACL Commands")
        
        # List users
        try:
            users = r.execute_command("ACL", "USERS")
            print(f"✅ ACL USERS: {users}")
        except Exception as e:
            print(f"❌ ACL USERS failed: {e}")
            return False
        
        # Show current user
        try:
            whoami = r.execute_command("ACL", "WHOAMI")
            print(f"✅ ACL WHOAMI: {whoami}")
        except Exception as e:
            print(f"❌ ACL WHOAMI failed: {e}")
            return False
        
        # List categories
        try:
            categories = r.execute_command("ACL", "CAT")
            print(f"✅ ACL CAT: {categories}")
        except Exception as e:
            print(f"❌ ACL CAT failed: {e}")
            return False
        
        # Test 4: Create a new user
        print("\n📋 Test 4: Create New User")
        
        try:
            # Create a read-only user
            result = r.execute_command("ACL", "SETUSER", "readonly", "+@read", "~*", ">readpass")
            print(f"✅ Created readonly user: {result}")
        except Exception as e:
            print(f"❌ Failed to create readonly user: {e}")
            return False
        
        # Create a write-only user
        try:
            result = r.execute_command("ACL", "SETUSER", "writeonly", "+@write", "~write:*", ">writepass")
            print(f"✅ Created writeonly user: {result}")
        except Exception as e:
            print(f"❌ Failed to create writeonly user: {e}")
            return False
        
        # Test 5: Test user authentication
        print("\n📋 Test 5: Test User Authentication")
        
        # Create new connection for readonly user
        try:
            r_readonly = redis.Redis(host='localhost', port=6380, decode_responses=True)
            auth_result = r_readonly.execute_command("AUTH", "readonly", "readpass")
            print(f"✅ Readonly user authenticated: {auth_result}")
            
            # Test read access
            value = r_readonly.get("test_key")
            if value == "test_value":
                print("✅ Readonly user can read data")
            else:
                print("❌ Readonly user cannot read data")
                return False
                
            # Test write access (should fail)
            try:
                r_readonly.set("readonly_test", "should_fail")
                print("❌ Readonly user should not be able to write")
                return False
            except redis.ResponseError as e:
                print(f"✅ Readonly user correctly denied write access: {e}")
            
        except Exception as e:
            print(f"❌ Readonly user authentication failed: {e}")
            return False
        
        # Test 6: Test write-only user
        print("\n📋 Test 6: Test Write-Only User")
        
        try:
            r_writeonly = redis.Redis(host='localhost', port=6380, decode_responses=True)
            auth_result = r_writeonly.execute_command("AUTH", "writeonly", "writepass")
            print(f"✅ Writeonly user authenticated: {auth_result}")
            
            # Test write access to allowed key pattern
            r_writeonly.set("write:allowed", "writeonly_value")
            print("✅ Writeonly user can write to allowed pattern")
            
            # Test write access to disallowed key pattern (should fail)
            try:
                r_writeonly.set("read:disallowed", "should_fail")
                print("❌ Writeonly user should not access keys outside pattern")
                return False
            except redis.ResponseError as e:
                print(f"✅ Writeonly user correctly denied access to disallowed key: {e}")
                
        except Exception as e:
            print(f"❌ Writeonly user authentication failed: {e}")
            return False
        
        # Test 7: Test ACL LIST and GETUSER
        print("\n📋 Test 7: Test ACL Inspection Commands")
        
        try:
            # List all users with their configurations
            acl_list = r.execute_command("ACL", "LIST")
            print(f"✅ ACL LIST: {len(acl_list)} user configurations")
            for user_config in acl_list:
                print(f"   {user_config}")
        except Exception as e:
            print(f"❌ ACL LIST failed: {e}")
            return False
        
        try:
            # Get specific user info
            readonly_info = r.execute_command("ACL", "GETUSER", "readonly")
            print(f"✅ ACL GETUSER readonly: {readonly_info}")
        except Exception as e:
            print(f"❌ ACL GETUSER failed: {e}")
            return False
        
        # Test 8: Test user deletion
        print("\n📋 Test 8: Test User Management")
        
        try:
            # Delete the test users
            deleted = r.execute_command("ACL", "DELUSER", "readonly", "writeonly")
            print(f"✅ Deleted {deleted} users")
        except Exception as e:
            print(f"❌ ACL DELUSER failed: {e}")
            return False
        
        # Test 9: Verify deleted users cannot authenticate
        print("\n📋 Test 9: Verify User Deletion")
        
        try:
            r_deleted = redis.Redis(host='localhost', port=6380, decode_responses=True)
            r_deleted.execute_command("AUTH", "readonly", "readpass")
            print("❌ Deleted user should not be able to authenticate")
            return False
        except redis.ResponseError as e:
            print(f"✅ Deleted user correctly denied authentication: {e}")
        except Exception as e:
            print(f"✅ Deleted user authentication failed as expected: {e}")
        
        print("\n🎉 ACL System Test COMPLETED!")
        print("✅ User Authentication: Working")
        print("✅ Command Authorization: Working") 
        print("✅ Key Pattern Permissions: Working")
        print("✅ User Management: Working")
        print("✅ ACL Inspection: Working")
        print("✅ Security Enforcement: Working")
        
        return True
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        return False

if __name__ == "__main__":
    if test_acl_system():
        print("\n🎉 ALL ACL TESTS PASSED! 🎉")
        print("\n🔐 Redis now has ENTERPRISE-GRADE SECURITY:")
        print("   👤 User Management: Create, modify, delete users")
        print("   🔑 Authentication: Username/password based")
        print("   🛡️  Authorization: Command and key-level permissions")
        print("   📂 Categories: Predefined command groups (@read, @write, @admin)")
        print("   🎯 Key Patterns: Fine-grained key access control")
        print("   🔍 Inspection: Full ACL visibility and debugging")
        sys.exit(0)
    else:
        print("\n💥 SOME ACL TESTS FAILED! 💥")
        sys.exit(1)
