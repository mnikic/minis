# =============================================================================
# TLV Protocol Concurrency Test Client
# This script launches multiple threads, each establishing an independent 
# connection to stress-test the C server's epoll/concurrency handling.
# =============================================================================

import socket
import struct
import threading
import time
import random
import sys

# --- Protocol Constants (Shared) ---
SER_NIL = 0
SER_ERR = 1
SER_STR = 2
ERR_UNKNOWN_CMD = 1

SERVER_HOST = '127.0.0.1'
SERVER_PORT = 1234
NUM_THREADS = 10 # Number of concurrent clients to simulate
NUM_ITERATIONS_PER_THREAD = 5 # How many commands each client sends

# --- Protocol Utility Functions (Copied from previous file) ---

def create_request(command, *args):
    """Encodes a single command into the full protocol format (Big-Endian)."""
    parts = [command] + list(args)
    n_args = len(parts)
    payload_parts = [struct.pack('!I', n_args)]
    for part in parts:
        data = part.encode('utf-8')
        payload_parts.append(struct.pack('!I', len(data)))
        payload_parts.append(data)
    payload = b''.join(payload_parts)
    length_prefix = struct.pack('!I', len(payload))
    return length_prefix + payload

def read_full_response(s):
    """Reads a complete response from the socket based on the length prefix."""
    
    # 1. Read the 4-byte length header L
    header = s.recv(4)
    if len(header) != 4:
        raise EOFError("Connection closed while reading header or header is truncated.")
    
    # Determine the total length L
    total_len = struct.unpack('!I', header)[0]
    
    # 2. Read the remaining payload (L bytes)
    payload = b''
    bytes_left = total_len
    while bytes_left > 0:
        chunk = s.recv(min(bytes_left, 4096)) # Read in chunks
        if not chunk: 
            raise EOFError("Connection closed while reading payload.")
        payload += chunk
        bytes_left -= len(chunk)
        
    return header + payload

def parse_response_type(full_response_data):
    """
    Quickly parses the response to get the type and content for verification.
    Returns (Type_Code, Content_Value)
    """
    if len(full_response_data) < 5:
        return (SER_ERR, "Malformed Response")

    response_type = full_response_data[4]
    
    if response_type == SER_NIL:
        return (SER_NIL, "NIL")
    
    payload = full_response_data[5:] # Data after Type (T) byte

    if response_type == SER_ERR:
        # Error response contains: EC (4 bytes) + SL (4 bytes) + Error String
        if len(payload) >= 8:
            str_len = struct.unpack('!I', payload[4:8])[0]
            if len(payload) >= 8 + str_len:
                error_msg = payload[8:8+str_len].decode('utf-8', errors='ignore')
                return (SER_ERR, f"ERR: {error_msg}")
        return (SER_ERR, "ERROR: Parsing Failed")

    elif response_type == SER_STR:
        # String response contains: SL (4 bytes) + String Data
        if len(payload) >= 4:
            str_len = struct.unpack('!I', payload[0:4])[0]
            if len(payload) >= 4 + str_len:
                data = payload[4:4+str_len].decode('utf-8', errors='ignore')
                return (SER_STR, data)
        return (SER_ERR, "STR: Parsing Failed")
    
    return (SER_ERR, f"Unknown Type {response_type}")

# =============================================================================
# Thread Logic
# =============================================================================

# Simple Lock for thread-safe printing
print_lock = threading.Lock()
success_count = 0
failure_count = 0

def client_thread(thread_id):
    """The function executed by each concurrent client thread."""
    global success_count, failure_count

    thread_name = f"Client-{thread_id}"
    key = f"k_{thread_id}" # Use a unique key for each thread to avoid conflicts

    with print_lock:
        print(f"[{thread_name}] Starting up...")

    s = None
    try:
        # 1. Establish independent connection
        s = socket.create_connection((SERVER_HOST, SERVER_PORT))
        
        for i in range(NUM_ITERATIONS_PER_THREAD):
            # 2. Random operation to stress the server
            
            # Use SET to store the current iteration number for the key
            value = str(i)
            req_data = create_request('set', key, value)
            
            s.sendall(req_data)
            response_data = read_full_response(s)
            
            res_type, res_content = parse_response_type(response_data)

            # Verification: SET should always return NIL
            if res_type == SER_NIL:
                with print_lock:
                    print(f"[{thread_name}] Iteration {i}: SET {key}={value} -> OK")
                success_count += 1
            else:
                with print_lock:
                    print(f"[{thread_name}] Iteration {i}: SET FAILED. Expected NIL, got {res_content}")
                failure_count += 1
                continue # Skip GET if SET failed

            # Give the server a small breather, simulating network delay
            time.sleep(0.005) 

            # Use GET to check if the value was stored correctly
            req_data = create_request('get', key)
            s.sendall(req_data)
            response_data = read_full_response(s)
            
            res_type, res_content = parse_response_type(response_data)

            # Verification: GET should return the value we just set
            expected_content = str(i)
            if res_type == SER_STR and res_content == expected_content:
                with print_lock:
                    print(f"[{thread_name}] Iteration {i}: GET {key} -> OK (Value: {res_content})")
                success_count += 1
            else:
                with print_lock:
                    print(f"[{thread_name}] Iteration {i}: GET FAILED. Expected '{expected_content}', got {res_content}")
                failure_count += 1

            # Introduce a small random delay between command pairs
            time.sleep(random.uniform(0.01, 0.05))

    except ConnectionRefusedError:
        with print_lock:
            print(f"[{thread_name}] FATAL ERROR: Connection refused. Is the server running?")
        global_failure_flag.set() # Signal main thread to stop gracefully
    except EOFError as e:
        with print_lock:
            print(f"[{thread_name}] ERROR: {e}. Server may have closed the connection.")
    except Exception as e:
        with print_lock:
            print(f"[{thread_name}] UNEXPECTED ERROR: {e}")
    finally:
        if s:
            s.close()
            with print_lock:
                print(f"[{thread_name}] Connection closed.")

# =============================================================================
# Main Execution
# =============================================================================

global_failure_flag = threading.Event()

if __name__ == '__main__':
    print(f"--- Launching {NUM_THREADS} concurrent clients for {NUM_ITERATIONS_PER_THREAD} iterations each ---")
    
    threads = []
    start_time = time.time()

    try:
        for i in range(NUM_THREADS):
            t = threading.Thread(target=client_thread, args=(i,))
            threads.append(t)
            t.start()
            # Stagger the connection slightly to avoid thundering herd on server accept()
            time.sleep(0.05) 
            
        # Wait for all threads to complete
        for t in threads:
            t.join()
            
    except KeyboardInterrupt:
        print("\nTest interrupted by user. Waiting for threads to finish...")
        global_failure_flag.set()

    end_time = time.time()

    # Final summary
    print("\n" + "="*50)
    print(f"CONCURRENCY TEST RESULTS")
    print(f"Total time elapsed: {end_time - start_time:.2f} seconds")
    print(f"Total transactions attempted: {NUM_THREADS * NUM_ITERATIONS_PER_THREAD * 2}")
    print(f"Successful transactions: {success_count}")
    print(f"Failed transactions: {failure_count}")
    print("="*50)

    if failure_count == 0:
        print("CONGRATULATIONS! Your epoll server handled the concurrent load successfully.")
    else:
        print("WARNING: Failures detected. Check server logs for concurrency issues or race conditions.")
