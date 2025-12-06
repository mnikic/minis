# =============================================================================
# TLV Protocol Persistent Client with Large Request Test
# =============================================================================

import socket
import struct
import sys
from time import sleep

# --- Protocol Constants ---
SER_NIL = 0    # No data (e.g., SET success)
SER_ERR = 1    # Error response
SER_STR = 2    # String data (e.g., GET success)
SER_INT = 3    # Integer data
SER_DBL = 4    # Double data
SER_ARR = 5    # Array data
ERR_UNKNOWN_CMD = 1
ERR_MALFORMED = 2

SERVER_HOST = '127.0.0.1'
SERVER_PORT = 1234 

# =============================================================================
# Core Protocol Functions
# =============================================================================

def create_request(command, *args):
    """Encodes a single command into the full protocol format (Big-Endian)."""
    parts = [command] + list(args)
    
    # 1. Build the payload (N + arguments)
    n_args = len(parts)
    payload_parts = [struct.pack('!I', n_args)]
    
    for part in parts:
        data = part.encode('utf-8')
        payload_parts.append(struct.pack('!I', len(data)))
        payload_parts.append(data)
        
    payload = b''.join(payload_parts)
    
    # 2. Add the overall length prefix L
    length_prefix = struct.pack('!I', len(payload))
    return length_prefix + payload

def format_hex_dump(data, max_lines=20):
    """Formats binary data into readable hex lines."""
    output = []
    for i in range(0, len(data)):
        output.append(data[i:i+1].hex())
    
    lines = []
    for i in range(0, len(output), 16):
        lines.append(' '.join(output[i:i+16]))
    
    # Truncate if too long
    if len(lines) > max_lines:
        lines = lines[:max_lines] + [f"... (truncated, {len(lines) - max_lines} more lines)"]
    
    return '\n'.join(lines)

def get_error_name(code):
    """Maps the error code to a readable name."""
    error_names = {
        1: "ERR_UNKNOWN_CMD",
        2: "ERR_MALFORMED",
        3: "ERR_TYPE"
    }
    return error_names.get(code, f"Unknown Error Code ({code})")

def parse_response(response_data):
    """Parses the response using the TLV protocol."""
    
    if len(response_data) < 4:
        print("ERROR: Response too short for 4-byte length prefix.")
        return

    # 1. Read Total Length L (Big-Endian)
    total_len_net = response_data[0:4]
    total_len_host = struct.unpack('!I', total_len_net)[0]
    
    print(f"1. Overall Response Length (Bytes 0-3): {total_len_net.hex()} -> Decimal: {total_len_host}")
    
    # 2. Extract Payload
    payload = response_data[4:]
    
    if len(payload) < total_len_host:
        print(f"ERROR: Received {len(payload)} bytes of payload, but expected {total_len_host} bytes based on header.")
        return
    
    # 3. Read Type T (1 byte)
    response_type = payload[0]
    print(f"2. Payload Type (Byte 4): {hex(response_type)}")
    
    payload_index = 1 

    if response_type == SER_NIL:
        print(f"   (Type NIL) -> (nil)")
        if total_len_host != 1:
             print(f"   WARNING: NIL response should only be 1 byte, but total length is {total_len_host}.")
        print("SUCCESS: Successfully parsed NIL response.")
        return

    elif response_type == SER_ERR:
        print(f"   (Type ERR) -> (err)")
        
        # 4. Read Error Code EC (4 bytes, Big-Endian)
        if len(payload) < payload_index + 4:
            print("ERROR: Payload too short for 1-byte Type + 4-byte Error Code.")
            return
            
        err_code_net = payload[payload_index : payload_index + 4]
        err_code_host = struct.unpack('!I', err_code_net)[0]
        payload_index += 4
        
        print(f"3. Error Code EC (Bytes 5-8): {err_code_net.hex()} -> Decimal: {err_code_host} ({get_error_name(err_code_host)})")

        # 5. Read String Length SL (4 bytes, Big-Endian)
        if len(payload) < payload_index + 4:
            print("ERROR: Payload too short for 4-byte String Length.")
            return
            
        str_len_net = payload[payload_index : payload_index + 4]
        str_len_host = struct.unpack('!I', str_len_net)[0]
        payload_index += 4
        
        print(f"4. Error Message Length SL (Bytes 9-12): {str_len_net.hex()} -> Decimal: {str_len_host}")
        
        # 6. Read Data (Error Message)
        data_start = payload_index
        if len(payload) < data_start + str_len_host:
            print(f"ERROR: Received data is truncated. Expected {str_len_host} bytes, got {len(payload) - data_start}.")
            return

        data = payload[data_start : data_start + str_len_host].decode('utf-8', errors='ignore')
        
        print(f"5. Error Data (Bytes 13+): '{data}'")
        print("SUCCESS: Successfully parsed Error response.")

    elif response_type == SER_STR:
        print(f"   (Type STR) -> (str)")
        
        # 4. Read String Length SL (4 bytes, Big-Endian)
        if len(payload) < payload_index + 4:
            print("ERROR: Payload too short for 4-byte String Length.")
            return
            
        str_len_net = payload[payload_index : payload_index + 4]
        str_len_host = struct.unpack('!I', str_len_net)[0]
        payload_index += 4
        
        print(f"3. String Length SL (Bytes 5-8): {str_len_net.hex()} -> Decimal: {str_len_host}")
        
        # 5. Read Data
        data_start = payload_index
        if len(payload) < data_start + str_len_host:
            print(f"ERROR: Received data is truncated. Expected {str_len_host} bytes, got {len(payload) - data_start}.")
            return

        # Truncate display for very long strings
        data = payload[data_start : data_start + str_len_host].decode('utf-8', errors='ignore')
        display_data = data if len(data) <= 100 else data[:100] + f"... (truncated, total {len(data)} chars)"
        
        print(f"4. Data (Bytes 9+): '{display_data}'")
        print(f"SUCCESS: Successfully parsed string response (length: {str_len_host}).")
        
    else:
        print(f"WARNING: Unknown Type Code: {response_type}")

# =============================================================================
# Persistent Connection Handler
# =============================================================================

def send_and_receive(s, command, *args, expect_close=False):
    """Sends a request on an existing socket and reads the full response."""
    
    cmd_str = f"{command} {' '.join(str(arg)[:50] for arg in args)}"
    if any(len(str(arg)) > 50 for arg in args):
        cmd_str += " (truncated)"
    print(f"\n--- COMMAND: {cmd_str} ---")
    
    request_data = create_request(command, *args)
    print(f"Request size: {len(request_data)} bytes")
    
    try:
        # 1. Send the request
        s.sendall(request_data)
        
        # If we expect the server to close the connection, handle that
        if expect_close:
            try:
                # Try to read - should get empty response or error
                header = s.recv(4)
                if len(header) == 0:
                    print("Server closed connection as expected (after large/invalid request)")
                    return False
            except:
                print("Server closed connection as expected")
                return False
        
        # 2. Read the 4-byte length header for the response
        header = s.recv(4)
        if len(header) != 4:
            if len(header) == 0:
                print(f"Server closed connection (EOF while reading header)")
                return False
            raise EOFError(f"Server closed connection while reading header for command: {cmd_str}")
        
        # Determine the total length L
        total_len = struct.unpack('!I', header)[0]
        
        # 3. Read the remaining payload (L bytes)
        payload = b''
        bytes_left = total_len
        while bytes_left > 0:
            chunk = s.recv(min(bytes_left, 8192))
            if not chunk: 
                raise EOFError(f"Server closed connection while reading payload for command: {cmd_str}")
            payload += chunk
            bytes_left -= len(chunk)
            
        full_response_data = header + payload
        
        print("\n--- Received Raw Response Data (Hex Dump) ---")
        print(f"Total Bytes Received: {len(full_response_data)}")
        print(format_hex_dump(full_response_data, max_lines=10))
        
        print("\n--- Interpretation Attempt (TLV Protocol) ---")
        parse_response(full_response_data)
            
    except EOFError as e:
        print(f"ERROR: {e}")
        return False
    except Exception as e:
        print(f"An error occurred during transaction: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    return True

# =============================================================================
# Test Functions
# =============================================================================

def test_large_request(s, size_bytes):
    """
    Tests the server's handling of a large request.
    Creates a SET command with a very large value.
    """
    print(f"\n{'='*70}")
    print(f"TESTING LARGE REQUEST: {size_bytes:,} bytes value")
    print(f"{'='*70}")
    
    # Create a large string value
    large_value = 'X' * size_bytes
    
    # Server should either:
    # 1. Accept it if within limits and return (nil)
    # 2. Reject it with an error message
    # 3. Close the connection
    result = send_and_receive(s, 'set', 'large_key', large_value, expect_close=(size_bytes > 4096))
    
    if not result:
        print(f"\nLarge request ({size_bytes:,} bytes) caused connection to close or failed")
        return False
    
    return True

def test_message_size_limit(s):
    """Tests requests around the K_MAX_MSG limit (typically 4096 bytes)."""
    print(f"\n{'='*70}")
    print("TESTING MESSAGE SIZE LIMITS")
    print(f"{'='*70}")
    
    # Test 1: Just under the limit (should work)
    # Account for protocol overhead: 4 (length) + 4 (num_args) + 4 (cmd_len) + 3 ("set") + 4 (key_len) + 8 ("test_key") + 4 (val_len) + value
    # Total overhead ≈ 31 bytes, so 4096 - 31 = 4065 bytes for value should be close to limit
    test_sizes = [
        (3000, "well under limit", False),
        (4000, "near limit", False),
        (4090, "at limit boundary", False),
        (5000, "over limit", True),
        (10000, "way over limit", True),
        (100000, "extremely large", True),
    ]
    
    for size, description, expect_close in test_sizes:
        print(f"\n--- Testing {description}: {size:,} bytes ---")
        if not test_large_request(s, size):
            if expect_close:
                print(f"✓ Server correctly rejected/closed for {description}")
                # Reconnect for next test
                try:
                    s.close()
                except:
                    pass
                s = socket.create_connection((SERVER_HOST, SERVER_PORT))
                print("Reconnected to server")
            else:
                print(f"✗ Unexpected failure for {description}")
                return False
        else:
            if expect_close:
                print(f"✗ Server should have rejected {description}")
            else:
                print(f"✓ Server correctly handled {description}")
        
        sleep(0.1)  # Small delay between tests
    
    return True

if __name__ == '__main__':
    s = None
    try:
        # Establish a single connection
        print(f"Attempting to connect to {SERVER_HOST}:{SERVER_PORT}...")
        s = socket.create_connection((SERVER_HOST, SERVER_PORT))
        print("Connection established. Running command sequence...")

        # --- Basic Commands ---
        print(f"\n{'='*70}")
        print("BASIC FUNCTIONALITY TESTS")
        print(f"{'='*70}")
        
        # 1. SET a = "hello_world" (NIL response)
        if not send_and_receive(s, 'set', 'a', 'hello_world'):
            sys.exit(1)

        # 2. GET a (STR response)
        if not send_and_receive(s, 'get', 'a'):
            sys.exit(1)

        # 3. SET b = "456" (NIL response)
        if not send_and_receive(s, 'set', 'b', '456'):
            sys.exit(1)
            
        # 4. GET b (STR response)
        if not send_and_receive(s, 'get', 'b'):
            sys.exit(1)

        # 5. Unknown command (ERR response)
        if not send_and_receive(s, 'foobar', 'baz'):
            pass

        # 6. GET a again to ensure the server state is maintained
        send_and_receive(s, 'get', 'a')
        
        # --- Large Request Tests ---
        test_message_size_limit(s)
        
        print("\n--- All Tests Complete ---")

    except ConnectionRefusedError:
        print(f"\nFATAL ERROR: Connection refused. Is the server running on {SERVER_HOST}:{SERVER_PORT}?")
    except Exception as e:
        print(f"\nFATAL ERROR: A critical error occurred: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if s:
            try:
                s.close()
                print("Connection closed.")
            except:
                pass
