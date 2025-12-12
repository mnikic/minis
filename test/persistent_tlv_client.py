# =============================================================================
# TLV Protocol Persistent Client with Comprehensive Malformed Request Test Suite
# =============================================================================

import socket
import struct
import sys
from time import sleep

# --- Protocol Constants (Client side TLV) ---
SER_NIL = 0     # No data (e.g., SET success)
SER_ERR = 1     # Error response
SER_STR = 2     # String data (e.g., GET success)
SER_INT = 3     # Int
# ... other types (INT, DBL, ARR) are not used in this script
# --- Error Codes expected from C Server's out_err function ---
# These codes must match the server's output based on the C parser logic provided:
C_ERR_MALFORMED = 5 # For "Request too short", "Missing length header", "Trailing garbage", etc.
C_ERR_UNKNOWN = 1   # For "Too many arguments"
C_ERR_2BIG = 2      # For requests exceeding K_MAX_MSG (e.g., 4096 bytes)

# Server-side constant for argument limit (used to test C_ERR_UNKNOWN)
K_MAX_ARGS_TEST_LIMIT = 1024

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
    # The payload starts with the 4-byte argument count (N)
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
    for byte in data:
        output.append(f'{byte:02x}')
    
    lines = []
    for i in range(0, len(output), 16):
        lines.append(' '.join(output[i:i+16]))
    
    # Truncate if too long
    if len(lines) > max_lines:
        lines = lines[:max_lines] + [f"... (truncated, {len(lines) - max_lines} more lines)"]
    
    return '\n'.join(lines)

def get_error_name(code):
    """Maps the C error code to a readable name."""
    error_names = {
        C_ERR_MALFORMED: "C_ERR_MALFORMED",
        C_ERR_UNKNOWN: "C_ERR_UNKNOWN",
        C_ERR_2BIG: "C_ERR_2BIG (Request Too Large)"
    }
    return error_names.get(code, f"Unknown Error Code ({code})")

def parse_response(response_data):
    """Parses the response using the TLV protocol and returns (type, value) or None."""
    if len(response_data) < 4:
        return None

    # 1. Read Total Length L (Big-Endian)
    total_len_net = response_data[0:4]
    total_len_host = struct.unpack('!I', total_len_net)[0]
    
    print(f"1. Overall Response Length (Bytes 0-3): {total_len_net.hex()} -> Decimal: {total_len_host}")
    
    # 2. Extract Payload
    payload = response_data[4:]
    
    if len(payload) < total_len_host:
        print(f"ERROR: Received {len(payload)} bytes of payload, but expected {total_len_host} bytes based on header.")
        return None
    
    # 3. Read Type T (1 byte)
    response_type = payload[0]
    print(f"2. Payload Type (Byte 4): {hex(response_type)}")
    
    payload_index = 1 

    if response_type == SER_NIL:
        print(f"  (Type NIL) -> (nil)")
        if total_len_host != 1:
            print(f"  WARNING: NIL response should only be 1 byte, but total length is {total_len_host}.")
        print("SUCCESS: Successfully parsed NIL response.")
        return (SER_NIL, None)

    elif response_type == SER_ERR:
        print(f"  (Type ERR) -> (err)")
        
        # 4. Read Error Code EC (4 bytes, Big-Endian)
        if len(payload) < payload_index + 4:
            print("ERROR: Payload too short for 1-byte Type + 4-byte Error Code.")
            return None
            
        err_code_net = payload[payload_index : payload_index + 4]
        err_code_host = struct.unpack('!I', err_code_net)[0]
        payload_index += 4
        
        print(f"3. Error Code EC (Bytes 5-8): {err_code_net.hex()} -> Decimal: {err_code_host} ({get_error_name(err_code_host)})")

        # 5. Read String Length SL (4 bytes, Big-Endian)
        if len(payload) < payload_index + 4:
            print("ERROR: Payload too short for 4-byte String Length.")
            return None
            
        str_len_net = payload[payload_index : payload_index + 4]
        str_len_host = struct.unpack('!I', str_len_net)[0]
        payload_index += 4
        
        print(f"4. Error Message Length SL (Bytes 9-12): {str_len_net.hex()} -> Decimal: {str_len_host}")
        
        # 6. Read Data (Error Message)
        data_start = payload_index
        if len(payload) < data_start + str_len_host:
            print(f"ERROR: Received data is truncated. Expected {str_len_host} bytes, got {len(payload) - data_start}.")
            return None

        data = payload[data_start : data_start + str_len_host].decode('utf-8', errors='ignore')
        
        print(f"5. Error Data (Bytes 13+): '{data}'")
        print("SUCCESS: Successfully parsed Error response.")
        return (SER_ERR, (err_code_host, data.strip()))

    elif response_type == SER_STR:
        # ... (SER_STR parsing)
        print(f"  (Type STR) -> (str)")
        if len(payload) < payload_index + 4:
            print("ERROR: Payload too short for 4-byte String Length.")
            return None
            
        str_len_net = payload[payload_index : payload_index + 4]
        str_len_host = struct.unpack('!I', str_len_net)[0]
        payload_index += 4
        
        print(f"3. String Length SL (Bytes 5-8): {str_len_net.hex()} -> Decimal: {str_len_host}")
        
        data_start = payload_index
        if len(payload) < data_start + str_len_host:
            print(f"ERROR: Received data is truncated. Expected {str_len_host} bytes, got {len(payload) - data_start}.")
            return None

        data = payload[data_start : data_start + str_len_host].decode('utf-8', errors='ignore')
        display_data = data if len(data) <= 100 else data[:100] + f"... (truncated, total {len(data)} chars)"
        
        print(f"4. Data (Bytes 9+): '{display_data}'")
        print(f"SUCCESS: Successfully parsed string response (length: {str_len_host}).")
        return (SER_STR, data)
    elif response_type == SER_INT:
        print(f"  (Type INT) -> (int64)")
        int64_size = 8
        if len(payload) < payload_index + int64_size:
            print("ERROR: Payload too short for 64-bit integer.")
            return None
            
        data = struct.unpack('!q', payload[payload_index:payload_index + int64_size])[0]
        payload_index += int64_size
        
        print(f"3. Integer Value (Bytes 5-12): {data}")
        print("SUCCESS: Successfully parsed Integer response.")
        return (SER_INT, data)            
    else:
        print(f"WARNING: Unknown Type Code: {response_type}")
        return None

def send_and_receive(s, command, *args, raw_request=None):
    """
    Sends a request on an existing socket and reads the full response.
    If raw_request is provided, it is sent instead of creating one.
    """
    cmd_str = f"{command} {' '.join(str(arg)[:50] for arg in args)}"
    if raw_request is None:
        request_data = create_request(command, *args)
        print(f"\n--- COMMAND: {cmd_str} ---")
    else:
        request_data = raw_request
        print(f"\n--- COMMAND: RAW MALFORMED PACKET ---")

    print(f"Request size: {len(request_data)} bytes")
    
    try:
        # 1. Send the request
        s.sendall(request_data)
        
        # 2. Read the 4-byte length header for the response
        header = s.recv(4)
        if len(header) != 4:
            if len(header) == 0:
                print(f"Server closed connection (EOF while reading header)")
                return None, False # Connection closed
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
        parsed_result = parse_response(full_response_data)
        return parsed_result, True # Return parsed data and success status
            
    except EOFError as e:
        print(f"ERROR: {e}")
        return None, False # Connection closed
    except Exception as e:
        print(f"An error occurred during transaction: {e}")
        return None, False # Transaction failed


def check_closure(s, test_name):
    """Attempts to send a dummy byte to check if the connection is closed."""
    print("\n--- Connection Closure Check ---")
    sleep(0.1) # Give the server a moment to close the FD
    try:
        # Attempt a tiny command that should fail if the socket is closed
        s.sendall(create_request('ping'))
        
        # If sendall succeeds, try to read the response header
        s.settimeout(0.5) # Set a small timeout for the read
        header = s.recv(4)
        
        if len(header) == 0:
            print("✓ SUCCESS: Connection confirmed closed (read returned EOF).")
            return True
        else:
            print("✗ FAILURE: Server accepted a subsequent command. Connection was NOT closed.")
            return False
    except socket.timeout:
        print("✗ FAILURE: Read timed out, but send succeeded. Connection status is ambiguous (likely still open).")
        return False
    except Exception as e:
        # Expected result: Broken Pipe or Connection Reset
        if 'broken pipe' in str(e).lower() or 'connection reset' in str(e).lower():
            print("✓ SUCCESS: Connection confirmed closed (got 'Broken Pipe' or 'Connection Reset' on send).")
            return True
        print(f"✗ FAILURE: Error during final closure check: {e}")
        return False
    
    return True

def _run_malformed_test_closure(test_name, raw_request_payload, expected_code, expected_msg):
    """
    Helper to run one malformed test. Establishes a new connection, runs the test,
    checks for the expected error, checks for closure, and closes the socket.
    """
    s = None
    success = False
    try:
        # Establish new connection for this isolated test
        s = socket.create_connection((SERVER_HOST, SERVER_PORT))
        
        # 1. Prepend the 4-byte total length (L) to the C-parser-style payload (N, L, D, L, D...)
        total_len = len(raw_request_payload)
        full_malformed_request = struct.pack('!I', total_len) + raw_request_payload
        
        print(f"\n{'='*70}")
        print(f"TESTING MALFORMED: {test_name} (Expecting Code: {expected_code})")
        print(f"{'='*70}")

        result, transaction_ok = send_and_receive(s, f"Malformed: {test_name}", raw_request=full_malformed_request)
        
        if not transaction_ok:
            print("✗ FATAL: Transaction failed before reading response (server closed immediately).")
            return False
        
        # 2. Check the response
        error_check_passed = False
        if result and result[0] == SER_ERR:
            code, msg = result[1]
            print("\n--- Error Validation ---")
            if code == expected_code and expected_msg in msg:
                print(f"✓ SUCCESS: Received expected error code {expected_code} and message: '{msg}'")
                error_check_passed = True
            else:
                print(f"✗ FAILURE: Received error code {code} ({get_error_name(code)}) with message: '{msg}'")
                print(f"  Expected: Code {expected_code} ({get_error_name(expected_code)}) and message containing: '{expected_msg}'")
        else:
            print(f"✗ FAILURE: Did not receive an error response. Got: {result}")
        
        if not error_check_passed:
            return False

        # 3. Check connection closure
        success = check_closure(s, test_name)
        
    finally:
        if s:
            s.close()
            print(f"Connection for '{test_name}' explicitly closed by client.")
            
    return success

# =============================================================================
# Malformed Test Suite
# =============================================================================

def test_malformed_requests_suite():
    
    print(f"\n\n{'='*70}")
    print("STARTING MALFORMED REQUEST SUITE (Testing C Parser Edge Cases)")
    print(f"{'='*70}")

    # 1. Request Too Short (len < 4)
    # Payload (N, L, D...) has only 3 bytes, not enough for the 4-byte count (N)
   # payload_too_short = b'\x00\x00\x01' 
   # if not _run_malformed_test_closure(
   #     "Request Too Short (len < 4)", 
   #     payload_too_short, 
   #     C_ERR_MALFORMED, 
   #     "Request too short for argument count"
   # ): return False
    
    # 2. Zero Arguments (num < 1)
    # Payload has 4 bytes indicating N=0
    payload_zero_args = struct.pack('!I', 0)
    if not _run_malformed_test_closure(
        "Zero Arguments (N=0)", 
        payload_zero_args, 
        C_ERR_MALFORMED, 
        "Must have at least one argument (the command)"
    ): return False

    # 3. Too Many Arguments (num > K_MAX_ARGS)
    # Payload has 4 bytes indicating N = MAX + 1
    num_args = K_MAX_ARGS_TEST_LIMIT + 1
    payload_too_many_args = struct.pack('!I', num_args)
    if not _run_malformed_test_closure(
        f"Too Many Arguments (N={num_args})", 
        payload_too_many_args, 
        C_ERR_MALFORMED, 
        "Too many arguments"
    ): return False

    # 4. Missing Length Header (pos + 4 > reqlen)
    # N=1, but payload ends after N and before the 4-byte length (L) for the first argument.
    num_args = 1
    # 4 bytes for N, plus 2 bytes missing the full 4-byte L
    payload_missing_length = struct.pack('!I', num_args) + b'\x00\x00' 
    if not _run_malformed_test_closure(
        "Missing Argument Length Header", 
        payload_missing_length, 
        C_ERR_MALFORMED, 
        "Argument count mismatch: missing length header"
    ): return False

    # 5. Data Length Exceeds Packet Size (pos + 4 + siz > reqlen)
    # N=1, L=1000, but only 1 byte of data (D) is provided.
    num_args = 1
    arg_len = 1000 
    actual_data = b'A' * 1 
    
    payload_exceeds_packet = (
        struct.pack('!I', num_args) +   # Arg Count N
        struct.pack('!I', arg_len) +    # Arg Length L (Expects 1000)
        actual_data                     # Arg Data D (Only 1 byte)
    )
    if not _run_malformed_test_closure(
        "Data Length Exceeds Packet Size", 
        payload_exceeds_packet, 
        C_ERR_MALFORMED, 
        "Argument count mismatch: data length exceeds packet size"
    ): return False

    # 6. Trailing Garbage (pos != reqlen)
    # Valid N=1, L=4, D=PING, followed by extra bytes.
    num_args = 1
    arg_data = b'PING'
    arg_len = len(arg_data)
    
    valid_part = (
        struct.pack('!I', num_args) +   # Arg Count N
        struct.pack('!I', arg_len) +    # Arg Length L
        arg_data                        # Arg Data D
    )
    
    payload_trailing_garbage = valid_part + b'\xDE' # Add trailing byte
    
    if not _run_malformed_test_closure(
        "Trailing Garbage", 
        payload_trailing_garbage, 
        C_ERR_MALFORMED, 
        "Trailing garbage in request"
    ): return False
    
    return True

def test_oversized_request_and_closure(s):
    """
    Tests the server's handling of an oversized request (10KB value).
    Expects C_ERR_2BIG response, followed by connection closure.
    """
    SIZE = 2000000
    EXPECTED_CODE = C_ERR_2BIG
    EXPECTED_MSG = "request too large; connection closed."
    
    print(f"\n{'='*70}")
    print(f"TESTING OVERSIZED REQUEST: {SIZE:,} bytes value (Expecting Error and Closure)")
    print(f"{'='*70}")
    
    # Create a large string value
    large_value = 'X' * SIZE
    
    # Send the SET command with the large value
    result, transaction_ok = send_and_receive(s, 'set', 'large_key', large_value)
    
    if not transaction_ok:
        print("✗ FATAL: Transaction failed before reading response (server closed immediately).")
        return False
    
    # Check the response
    error_check_passed = False
    if result and result[0] == SER_ERR:
        code, msg = result[1]
        print("\n--- Error Validation ---")
        
        if code == EXPECTED_CODE and msg.strip() == EXPECTED_MSG:
            print(f"✓ SUCCESS: Received expected error code {EXPECTED_CODE} and message: '{msg}'")
            error_check_passed = True
        else:
            print(f"✗ FAILURE: Received error code {code} ({get_error_name(code)}) with message: '{msg}'")
            print(f"  Expected: Code {EXPECTED_CODE} ({get_error_name(EXPECTED_CODE)}) and message: '{EXPECTED_MSG}'")
    else:
        print(f"✗ FAILURE: Did not receive an error response. Got: {result}")
    
    # If the error message check failed, we stop here.
    if not error_check_passed:
        return False

    # Test 2: Check if the connection is closed after sending the error.
    return check_closure(s, "Oversized Request")

# =============================================================================
# Pipelining Test Functions (Added to demonstrate the single-buffer bug)
# =============================================================================

def send_only(s, command, *args):
    """Encodes and sends a request, but does not wait for a response."""
    request_data = create_request(command, *args)
    cmd_str = f"{command} {' '.join(str(arg)[:10] for arg in args)}..."
    print(f"PIPELINE SEND: Request {cmd_str} ({len(request_data)} bytes)")
    s.sendall(request_data)

def read_response_only(s, expected_command):
    """Reads and parses the next response from the socket."""
    print(f"\nPIPELINE READ: Waiting for response to '{expected_command}'...")
    try:
        # Re-use the response-reading logic from send_and_receive
        header = s.recv(4)
        if len(header) != 4:
            if len(header) == 0:
                print("Server closed connection (EOF).")
            else:
                print(f"Failed to read complete 4-byte response header.")
            return None # Connection closed or error

        total_len = struct.unpack('!I', header)[0]
        
        payload = b''
        bytes_left = total_len
        while bytes_left > 0:
            chunk = s.recv(min(bytes_left, 8192))
            if not chunk:
                raise EOFError(f"Server closed connection while reading payload for {expected_command}.")
            payload += chunk
            bytes_left -= len(chunk)

        full_response_data = header + payload
        
        print(f"--- Received Raw Response Data ({len(full_response_data)} bytes) ---")
        parsed_result = parse_response(full_response_data)
        return parsed_result
        
    except EOFError as e:
        print(f"ERROR: {e}")
        return None
    except Exception as e:
        print(f"An error occurred during transaction: {e}")
        return None

def test_pipelining_failure_suite():
    """
    Demonstrates the failure of the single-response-buffer design under pipelining.
    
    1. Send A: SET small_key (to get a small +OK response)
    2. Send B: SET large_key (This response is small, but sets up the large data)
    3. Send C: GET large_key (This generates a large 10KB response and locks conn->wbuf)
    4. Send D: DEL small_key (This request is processed while Response C is still in conn->wbuf)
    
    EXPECTED OUTCOME: Response to D will be C_ERR_MALFORMED ("write buffer full") 
                      and the connection will close after sending the error.
    """
    s = None
    large_value = 'X' * 200000 # 10KB value, same as oversized test

    print(f"\n{'='*70}")
    print("TESTING PIPELINING FAILURE (Single Response Buffer)")
    print(f"{'='*70}")
    
    try:
        s = socket.create_connection((SERVER_HOST, SERVER_PORT))
        
        # --- PHASE 1: Send All Requests Pipelined ---
        
        # A. SET 'key_s' 'small' (Setup)
        send_only(s, 'SET', 'key_s', 'small')
        
        # B. SET 'key_l' '10KB_data' (Setup the large value)
        send_only(s, 'SET', 'key_l', large_value)

        # C. GET 'key_l'. Response is 10KB. This is the one that locks conn->wbuf.
        send_only(s, 'GET', 'key_l')
        
        # D. DEL 'key_s'. This command is processed by the server while 
        #    Response C is buffered but not yet fully sent.
        send_only(s, 'DEL', 'key_s')
        
        # E. GET 'key_s'. Will definitely be rejected.
        send_only(s, 'GET', 'key_s')
        
        print("\nAll 5 requests sent in one batch. Waiting for responses...")

        # --- PHASE 2: Read All Responses Sequentially ---

        # 1. Read Response A (SET 'key_s'): Expected NIL
        result = read_response_only(s, "SET 'key_s'")
        print(f"Result A: {result}")
        
        # 2. Read Response B (SET 'key_l'): Expected NIL
        result = read_response_only(s, "SET 'key_l'")
        print(f"Result B: {result}")

        # 3. Read Response C (GET 'key_l'): Expected STR (10KB)
        result = read_response_only(s, "GET 'key_l'")
        print(f"Result C: {result[0]} ({len(result[1]):,} bytes)")

        # 4. Read Response D (DEL 'key_s'): 
        #    *** EXPECTED FAILURE: C_ERR_UNKNOWN (write buffer full) ***
        #    NOTE: The server code uses ERR_UNKNOWN for "response too large" from cache_execute,
        #    which is what the "write buffer full" path calls.
        result = read_response_only(s, "DEL 'key_s'")
        print(f"Result D: {result}")
        if result and result[0] == SER_ERR:
            code, msg = result[1]
            if code == C_ERR_UNKNOWN and "write buffer full" in msg:
                print("✓ SUCCESS: Pipelined request rejected with 'write buffer full' error.")
            else:
                print(f"✗ FAILURE: Expected 'write buffer full' error, got code {code} msg '{msg}'.")
        else:
            print(f"✗ FAILURE: Pipelined request D was not rejected as expected. Got: {result}")

        # 5. Read Response E (GET 'key_s'): Will likely be EOF
        result = read_response_only(s, "GET 'key_s'")
        print(f"Result E: {result}")
        
        # Final closure check
        check_closure(s, "Pipelining Test")
        
    except ConnectionRefusedError:
        print("\nFATAL ERROR: Connection refused.")
        return False
    except Exception as e:
        print(f"\nFATAL ERROR: An error occurred: {e}")
        return False
    finally:
        if s: s.close()
    
    return True
# =============================================================================
# Main Execution
# =============================================================================

if __name__ == '__main__':
    s = None
    try:
        # Establish a single persistent connection for the basic tests
        print(f"Attempting to connect to {SERVER_HOST}:{SERVER_PORT}...")
        s = socket.create_connection((SERVER_HOST, SERVER_PORT))
        print("Connection established. Running command sequence...")

        # --- Basic Commands (Pre-condition Check) ---
        print(f"\n{'='*70}")
        print("BASIC FUNCTIONALITY TESTS (Pre-condition Check)")
        print(f"{'='*70}")
        
        # 1. SET a = "hello_world" (NIL response)
        result, ok = send_and_receive(s, 'set', 'a', 'hello_world')
        if not ok or result[0] != SER_NIL: print("Basic SET failed."); sys.exit(1)

        # 2. GET a (STR response)
        result, ok = send_and_receive(s, 'get', 'a')
        if not ok or result[0] != SER_STR or result[1] != 'hello_world': print("Basic GET failed."); sys.exit(1)
        
        # --- Oversized Request Test ---
        # This test is run on the persistent socket and is expected to close it.
        #if not test_oversized_request_and_closure(s): sys.exit(1)
        
        # Close the client side explicitly after the server is expected to have closed.
        if s: 
            s.close()
            s = None
            
        # --- Malformed Request Suite ---
        # This suite handles its own connections internally for isolation.
        #if not test_malformed_requests_suite(): sys.exit(1)
        # --- Pipelining Failure Test (NEW) ---
        if not test_pipelining_failure_suite(): sys.exit(1)
        print("\n--- All Tests Complete ---")

    except ConnectionRefusedError:
        print(f"\nFATAL ERROR: Connection refused. Is the server running on {SERVER_HOST}:{SERVER_PORT}?")
        sys.exit(1)
    except Exception as e:
        print(f"\nFATAL ERROR: A critical error occurred: {e}")
        sys.exit(1)
    finally:
        # Ensures any lingering connection from the main block is closed.
        if s:
            try:
                s.close()
                print("Client connection closed.")
            except:
                pass
