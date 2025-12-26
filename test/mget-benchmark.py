import socket
import struct
import time
import sys

# --- Configuration ---
SERVER_HOST = '127.0.0.1'
SERVER_PORT = 1234
NUM_KEYS = 100
KEY_PREFIX = "benchkey_"
VALUE_SIZE = 1024       # 1KB values
ITERATIONS = 50000      # Total requests to simulate
PIPELINE_DEPTH = 100     # How many MGETs to batch per network write

# --- Protocol Constants ---
SER_NIL = 0
SER_ERR = 1
SER_STR = 2
SER_INT = 3
SER_DBL = 4
SER_ARR = 5

# --- Protocol Reading Utilities ---

def _parse_element(payload, offset):
    if offset >= len(payload):
        raise ValueError("Payload exhausted.")

    response_type = payload[offset]
    offset += 1

    if response_type == SER_NIL:
        return ("(nil)", offset)
    elif response_type == SER_ERR:
        err_code_host = struct.unpack('!I', payload[offset:offset + 4])[0]
        offset += 4
        str_len_host = struct.unpack('!I', payload[offset:offset + 4])[0]
        offset += 4
        data = payload[offset:offset + str_len_host].decode('utf-8', errors='ignore').strip()
        offset += str_len_host
        return (f"Error {err_code_host}: {data}", offset)
    elif response_type == SER_STR:
        str_len_host = struct.unpack('!I', payload[offset:offset + 4])[0]
        offset += 4
        # Optimization: Don't decode huge strings for benchmarks, just skip
        # data = payload[offset:offset + str_len_host].decode('utf-8')
        offset += str_len_host
        return ("(string data hidden)", offset)
    elif response_type == SER_INT:
        data = struct.unpack('!q', payload[offset:offset + 8])[0]
        offset += 8
        return (data, offset)
    elif response_type == SER_ARR:
        num_elements = struct.unpack('!I', payload[offset:offset + 4])[0]
        offset += 4
        arr_data = []
        for _ in range(num_elements):
            element_value, offset = _parse_element(payload, offset)
            arr_data.append(element_value)
        return (arr_data, offset)
    
    return (f"Unknown Type {response_type}", offset)

def read_full_response(s):
    """Reads exactly one full response from the socket."""
    # 1. Read Header (4 bytes)
    header = s.recv(4, socket.MSG_WAITALL)
    if not header or len(header) != 4:
        return None

    total_len = struct.unpack('!I', header)[0]
    
    # 2. Read Payload (L bytes) using MSG_WAITALL for efficiency
    # This avoids the slow while loop in Python
    payload = s.recv(total_len, socket.MSG_WAITALL)
    if not payload or len(payload) != total_len:
         raise IOError("Incomplete payload read.")

    return header + payload

# --- Encoding ---

def encode_custom_command(parts):
    N = struct.pack('!I', len(parts))
    payload = [N]
    
    for part in parts:
        part_bytes = str(part).encode('utf-8')
        L = struct.pack('!I', len(part_bytes))
        payload.append(L)
        payload.append(part_bytes)
        
    raw_payload = b''.join(payload)
    # Add Total Length Prefix
    return struct.pack('!I', len(raw_payload)) + raw_payload

# --- Operations ---

def connect_to_server():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((SERVER_HOST, SERVER_PORT))
    return sock

def setup_keys_mset(sock):
    """Uses MSET to set all keys in one go."""
    print(f"\n--- 1. Setting up {NUM_KEYS} keys ({VALUE_SIZE/1024:.0f}KB each) via MSET... ---")
    
    value = 'X' * VALUE_SIZE
    args = ['MSET']
    for i in range(NUM_KEYS):
        args.append(f"{KEY_PREFIX}{i}")
        args.append(value)

    req_data = encode_custom_command(args)
    print(f"Sending MSET payload: {len(req_data)/1024:.2f} KB")
    
    t0 = time.time()
    sock.sendall(req_data)
    read_full_response(sock) # Read the NIL/OK response
    print(f"Setup complete in {time.time() - t0:.4f}s")

def cleanup_keys_mdel(sock):
    """Uses variadic MDEL to remove keys."""
    print(f"\n--- 3. Cleaning up {NUM_KEYS} keys via MDEL... ---")
    args = ['MDEL'] + [f"{KEY_PREFIX}{i}" for i in range(NUM_KEYS)]
    
    sock.sendall(encode_custom_command(args))
    read_full_response(sock)
    print("Cleanup complete.")

def benchmark_mget_pipeline(sock):
    print(f"\n--- 2. Running MGET Pipelined Benchmark ---")
    print(f"Keys: {NUM_KEYS} | Iterations: {ITERATIONS} | Pipeline Depth: {PIPELINE_DEPTH}")

    # 1. Prepare Single Request
    keys = [f"{KEY_PREFIX}{i}" for i in range(NUM_KEYS)]
    mget_args = ['MGET'] + keys
    single_req = encode_custom_command(mget_args)
    
    # 2. Create Pipeline Batch
    # We concatenate 'PIPELINE_DEPTH' requests into one giant buffer
    batch_payload = single_req * PIPELINE_DEPTH
    
    print(f"Single Request Size: {len(single_req)/1024:.2f} KB")
    print(f"Batch Payload Size:  {len(batch_payload)/1024:.2f} KB")

    # 3. Execution Loop
    batches = ITERATIONS // PIPELINE_DEPTH
    start_time = time.time()

    for _ in range(batches):
        # A. Send Batch
        sock.sendall(batch_payload)
        i = 0;
        # B. Read Batch
        for _ in range(PIPELINE_DEPTH):
            read_full_response(sock)
            i = i + 1
            # We skip parsing for pure throughput measurement
            # (Parsing in Python is slow and distorts the network benchmark)

    elapsed = time.time() - start_time
    
    # 4. Stats
    total_reqs = batches * PIPELINE_DEPTH
    total_data = total_reqs * NUM_KEYS * VALUE_SIZE
    
    print("\n--- Results ---")
    print(f"Time: {elapsed:.4f}s")
    print(f"Throughput: {total_reqs / elapsed:.2f} ops/sec")
    print(f"Data Rate:  {(total_data / 1024 / 1024) / elapsed:.2f} MB/sec")

def main():
    try:
        sock = connect_to_server()
        
        setup_keys_mset(sock)
        
        # Reconnect to ensure clean state for benchmark
        sock.close()
        sock = connect_to_server()
        
        benchmark_mget_pipeline(sock)
        
        cleanup_keys_mdel(sock)
        sock.close()
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
