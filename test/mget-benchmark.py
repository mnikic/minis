import socket
import struct
import time
import sys
import random

# --- Configuration ---
SERVER_HOST = '127.0.0.1'
SERVER_PORT = 1234  # Custom port for your server
NUM_KEYS =  100 # REDUCED to 3500 keys for a safe payload size (~56KB)
KEY_PREFIX = "benchkey_"
VALUE_SIZE = 1024   # Size of the value in bytes (1KB)
ITERATIONS = 10000    # How many times to repeat the MGET command for measurement

# Constants matching your server's protocol
SER_NIL = 0
SER_ERR = 1
SER_STR = 2
SER_INT = 3
SER_DBL = 4
SER_ARR = 5

# --- Protocol Reading Utilities (From your provided code) ---

def _parse_element(payload, offset):
    """Recursively parses a single serialized element (T, L, V...) starting from offset."""
    if offset >= len(payload):
        raise ValueError("Payload exhausted while trying to read element type.")

    response_type = payload[offset]
    offset += 1

    if response_type == SER_NIL:
        return ("Success (NIL)", offset)
    elif response_type == SER_ERR:
        if offset + 8 > len(payload): raise ValueError("Payload exhausted while reading error code/string length.")
        err_code_host = struct.unpack('!I', payload[offset:offset + 4])[0]
        offset += 4
        str_len_host = struct.unpack('!I', payload[offset:offset + 4])[0]
        offset += 4
        if offset + str_len_host > len(payload): raise ValueError("Payload exhausted while reading error message data.")
        data = payload[offset:offset + str_len_host].decode('utf-8', errors='ignore').strip()
        offset += 4
        return (f"Error Code {err_code_host}: {data}", offset)
    elif response_type == SER_STR:
        if offset + 4 > len(payload): raise ValueError("Payload exhausted while reading string length.")
        str_len_host = struct.unpack('!I', payload[offset:offset + 4])[0]
        offset += 4
        if offset + str_len_host > len(payload): raise ValueError("Payload exhausted while reading string data.")
        data = payload[offset:offset + str_len_host].decode('utf-8', errors='ignore')
        offset += str_len_host
        return (data, offset)
    elif response_type == SER_INT:
        int64_size = 8
        if offset + int64_size > len(payload): raise ValueError("Payload exhausted while reading 64-bit integer.")
        data = struct.unpack('!q', payload[offset:offset + int64_size])[0]
        offset += int64_size
        return (data, offset)
    elif response_type == SER_ARR:
        if offset + 4 > len(payload): raise ValueError("Payload exhausted while reading array element count.")
        num_elements = struct.unpack('!I', payload[offset:offset + 4])[0]
        offset += 4
        arr_data = []
        for i in range(num_elements):
            if offset >= len(payload): raise ValueError(f"Payload exhausted before reading element {i+1} of {num_elements}.")
            element_value, offset = _parse_element(payload, offset)
            arr_data.append(element_value)
        return (arr_data, offset)
    return (f"Unknown Type Code {response_type}", offset)


def parse_response(response_data):
    """Processes the full response, including the initial length prefix."""
    if len(response_data) < 4:
        return ("ERROR: Response too short to contain a length header.", 0)

    total_len_host = struct.unpack('!I', response_data[0:4])[0]
    payload = response_data[4:4 + total_len_host]

    if len(payload) != total_len_host:
        return (f"ERROR: Received {len(payload)} bytes of payload, but header expected {total_len_host}.", 0)

    try:
        parsed_value, bytes_consumed = _parse_element(payload, 0)
        return (parsed_value, total_len_host)
    except Exception as e:
        return (f"Parsing error: {e}", total_len_host)


def read_full_response(s):
    """Reads a complete response from the socket based on the length prefix."""
    # Read the 4-byte length header L
    header = s.recv(4)
    if not header:
        return None 
    if len(header) != 4:
        raise IOError("Failed to read complete 4-byte response header.")

    total_len_response = struct.unpack('!I', header)[0]
    
    # Read the remaining payload (L bytes)
    payload = b''
    bytes_left = total_len_response
    while bytes_left > 0:
        chunk = s.recv(bytes_left)
        if not chunk:
            raise IOError("Server closed connection prematurely while reading payload.")
        payload += chunk
        bytes_left -= len(chunk)

    return header + payload

# --- Custom Encoding Function ---

def encode_custom_command(parts):
    """
    Encodes a list of command parts into the custom length-encoded binary protocol payload.
    Format: N (count, 4 bytes) + [L (len, 4 bytes) + D (data, L bytes)] * N
    """
    N = struct.pack('!I', len(parts)) # Array element count N
    payload = N
    
    for part in parts:
        part_bytes = str(part).encode('utf-8')
        L = struct.pack('!I', len(part_bytes)) # Length L
        D = part_bytes                       # Data D
        payload += L + D
        
    return payload 

def connect_to_server():
    """Establishes a connection to the server."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.connect((SERVER_HOST, SERVER_PORT))
        print(f"Connected to {SERVER_HOST}:{SERVER_PORT}")
        sock.settimeout(10.0) 
        return sock
    except ConnectionRefusedError:
        print(f"Error: Could not connect to {SERVER_HOST}:{SERVER_PORT}. Is the server running on port {SERVER_PORT}?")
        sys.exit(1)
    except Exception as e:
        print(f"An unexpected error occurred during connection: {e}")
        sys.exit(1)

def run_transaction_and_verify(sock, command_parts, expected_success='Success (NIL)'):
    """Sends a command, reads response, and verifies success."""
    raw_payload_bytes = encode_custom_command(command_parts)
    
    # Prefix the payload with the 4-byte total length L
    total_len = len(raw_payload_bytes)
    length_prefix = struct.pack('!I', total_len)
    full_request = length_prefix + raw_payload_bytes
    
    sock.sendall(full_request)
    
    full_response_data = read_full_response(sock)
    if full_response_data is None:
        raise ConnectionError("Server closed the connection unexpectedly.")

    parsed_result, _ = parse_response(full_response_data)
    
    # For SET, we expect the SER_NIL type, which the parser turns into 'Success (NIL)'
    if parsed_result != expected_success:
        # Check if it was an integer result, which some commands (like DEL) return
        if isinstance(parsed_result, int):
            return True # Assume INT result is okay for verification
        
        print(f"Command {' '.join(command_parts)} failed.")
        print(f"Error Response: {parsed_result}")
        return False
    return True

def setup_keys(sock, count, size):
    """Sets a large number of keys with specific value size."""
    print(f"\n--- 1. Setting up {count} keys with {size/1024:.0f}KB value size... ---")
    start_time = time.time()
    
    # Generate the value once
    value = 'X' * size
    
    for i in range(count):
        key = f"{KEY_PREFIX}{i}"
        command_parts = ['SET', key, value]
        
        if not run_transaction_and_verify(sock, command_parts):
            print("Stopping setup due to server error.")
            return False

    elapsed = time.time() - start_time
    print(f"Key setup completed in {elapsed:.2f} seconds.")
    return True

def benchmark_mget(sock, count, iterations):
    """Runs MGET benchmark repeatedly."""
    print(f"\n--- 2. Running MGET benchmark ({iterations} iterations) ---")
    
    # 1. Construct the MGET command argument list
    keys = [f"{KEY_PREFIX}{i}" for i in range(count)]
    print (f'keys size: {len(keys)}')
    mget_args = ['MGET'] + keys
    
    # 2. Encode the full MGET command request
    raw_payload_bytes = encode_custom_command(mget_args)
    total_len = len(raw_payload_bytes)
    length_prefix = struct.pack('!I', total_len)
    mget_command_request = length_prefix + raw_payload_bytes
    
    command_size_bytes = len(mget_command_request)
    # Payload size for 3500 keys is approximately: 4 (L prefix) + 4 (N) + 8 (MGET L+D) + 3500 * (~16 per key) ≈ 56 KB
    print(f"MGET request size (3500 keys): {command_size_bytes / 1024:.1f} KB")
    
    # 3. Start timing
    start_time = time.time()
    
    # Loop for iterations - raw send/receive for maximal throughput measurement
    for _ in range(iterations):
        # Send the command
        sock.sendall(mget_command_request)
        
        # Read the full, large response from the server, avoiding costly Python parsing
        try:
            full_response_data = read_full_response(sock)
            if full_response_data is None:
                 raise ConnectionError("Server closed during MGET response read.")
        except Exception as e:
            print(f"Error during MGET response read: {e}. Skipping iteration.")
            continue
            
    # 4. End timing and calculate results
    elapsed = time.time() - start_time
    total_requests = iterations
    total_keys_retrieved = iterations * count
    total_data_size_bytes = iterations * count * VALUE_SIZE
    
    print("\n--- Results ---")
    print(f"Total time elapsed: {elapsed:.4f} seconds")
    print(f"Total MGET requests: {total_requests}")
    print(f"Total keys retrieved: {total_keys_retrieved:,}")
    print(f"Total data transferred (estimated payload): {total_data_size_bytes / (1024*1024):.2f} MB")
    
    if elapsed > 0:
        rps = total_requests / elapsed
        mb_per_sec = (total_data_size_bytes / (1024 * 1024)) / elapsed
        
        print(f"MGET Throughput: {rps:.2f} requests/second (QPS)")
        print(f"Data Throughput: {mb_per_sec:.2f} MB/second")


def main():
    sock = connect_to_server()
    
    # 1. Setup keys
    if setup_keys(sock, NUM_KEYS, VALUE_SIZE):
        # 2. Run benchmark
        
        # Close and reopen the connection to clear any network buffer state 
        # that might skew the benchmark timing.
        sock.close()
        sock = connect_to_server() 
        
        benchmark_mget(sock, NUM_KEYS, ITERATIONS)
        
    sock.close()
    print("\nBenchmark finished.")


if __name__ == "__main__":
    main()
