import socket
import threading
import time
import random
import string
import struct
import sys
import argparse

# --- Defaults ---
DEFAULT_HOST = '127.0.0.1'
DEFAULT_PORT = 1234
DEFAULT_THREADS = 16        
BATCH_SIZE = 500        
DEFAULT_PREFIX = "soak"    

# --- Protocol Constants ---
SER_NIL = 0
SER_ERR = 1
SER_STR = 2
SER_INT = 3
SER_DBL = 4
SER_ARR = 5

# --- Core Protocol Functions ---

def create_request(command, *args):
    """Encodes a single command into the full protocol format (Big-Endian)."""
    parts = [command] + list(args)
    
    # 1. Build the payload (N + arguments)
    n_args = len(parts)
    payload_parts = [struct.pack('!I', n_args)]
    
    for part in parts:
        data = str(part).encode('utf-8')
        payload_parts.append(struct.pack('!I', len(data)))
        payload_parts.append(data)
        
    payload = b''.join(payload_parts)
    
    # 2. Add the overall length prefix L
    length_prefix = struct.pack('!I', len(payload))
    return length_prefix + payload

def parse_response_element(payload, offset):
    """Recursive helper to parse TLV elements."""
    if offset >= len(payload):
        raise ValueError("Payload exhausted")

    response_type = payload[offset]
    offset += 1

    if response_type == SER_NIL:
        return (None, offset)
        
    elif response_type == SER_ERR:
        offset += 4 
        str_len = struct.unpack('!I', payload[offset:offset+4])[0]
        offset += 4
        data = payload[offset:offset+str_len].decode('utf-8', errors='ignore')
        return (Exception(f"Server Error: {data}"), offset + str_len)

    elif response_type == SER_STR:
        str_len = struct.unpack('!I', payload[offset:offset+4])[0]
        offset += 4
        data = payload[offset:offset+str_len].decode('utf-8', errors='ignore')
        return (data, offset + str_len)
        
    elif response_type == SER_INT:
        val = struct.unpack('!q', payload[offset:offset+8])[0]
        return (val, offset + 8)

    elif response_type == SER_DBL:
        val = struct.unpack('!d', payload[offset:offset+8])[0]
        return (val, offset + 8)

    elif response_type == SER_ARR:
        count = struct.unpack('!I', payload[offset:offset+4])[0]
        offset += 4
        arr = []
        for _ in range(count):
            elem, offset = parse_response_element(payload, offset)
            arr.append(elem)
        return (arr, offset)
        
    else:
        raise ValueError(f"Unknown Type Code: {response_type}")

def read_full_response(s):
    """Reads [Len] + [Payload] and parses."""
    header = s.recv(4)
    if not header: raise ConnectionError("Server closed connection")
    
    total_len = struct.unpack('!I', header)[0]
    
    payload = b''
    while len(payload) < total_len:
        chunk = s.recv(min(total_len - len(payload), 8192))
        if not chunk: raise ConnectionError("Server closed during payload")
        payload += chunk

    val, _ = parse_response_element(payload, 0)
    if isinstance(val, Exception):
        raise val
    return val

# --- Test Worker ---

class RedisClient:
    def __init__(self, host, port):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((host, port))

    def send(self, cmd, *args):
        req = create_request(cmd, *args)
        self.sock.sendall(req)
        return read_full_response(self.sock)

    def close(self):
        self.sock.close()

# Global Stats
total_ops = 0
errors = 0
lock = threading.Lock()

def random_string(length=10):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

def worker(thread_id, host, port, prefix_base):
    global total_ops, errors
    client = None
    try:
        client = RedisClient(host, port)
        # Unique prefix: <CLI_PREFIX>:<THREAD_ID>
        # e.g., "shell1:0", "shell1:1"
        prefix = f"{prefix_base}:{thread_id}"
        
        while True:
            # 1. Generate Batch
            data = {}
            for i in range(BATCH_SIZE):
                key = f"{prefix}:{i}" 
                val = random_string(20)
                data[key] = val
            
            # 2. MSET
            mset_flattened = []
            for k, v in data.items():
                mset_flattened.extend([k, v])
            
            req_mset = create_request("mset", *mset_flattened)
            req_mget = create_request("mget", *list(data.keys()))
            req_mdel = create_request("mdel", *list(data.keys()))

            # 2. Send ONE massive packet (The Pipeline)
            # This triggers your server's batch processing logic
            client.sock.sendall(req_mset + req_mget + req_mdel)

            # --- PIPELINE READ ---
            
            # 3. Read responses sequentially
            # Response 1: MSET -> Expect "+OK" (or equivalent)
            res_mset = read_full_response(client.sock)
            
            # Response 2: MGET -> Expect List
            res_mget = read_full_response(client.sock)

            # Response 3: MDEL -> Expect Integer
            res_mdel = read_full_response(client.sock)

            # --- VALIDATION ---
            
            # Verify MGET
            if not isinstance(res_mget, list):
                 raise Exception(f"MGET expected list, got {type(res_mget)}")
            if len(res_mget) != len(data):
                 raise Exception(f"MGET size mismatch. Expected {len(data)}, got {len(res_mget)}")
            
            keys = list(data.keys())
            for i, val in enumerate(res_mget):
                expected = data[keys[i]]
                if val != expected:
                    raise Exception(f"Corruption! Key {keys[i]}: Expected '{expected}', Got '{val}'")

            # Verify DEL
            if res_mdel != len(data):
                 raise Exception(f"DEL count mismatch. Expected {len(data)}, got {res_mdel}")

            # 4. Ghost Check (Separate round-trip is fine here to be safe)
            check_key = keys[random.randint(0, len(keys)-1)]
            val = client.send("get", check_key)
            if val is not None:
                raise Exception(f"Ghost Key found! {check_key} returned '{val}'")

            with lock:
                total_ops += (BATCH_SIZE * 3) + 1
                mset_flattened = []
            for k, v in data.items():
                mset_flattened.extend([k, v])
            
            req_mset = create_request("mset", *mset_flattened)
            req_mget = create_request("mget", *list(data.keys()))
            req_mdel = create_request("mdel", *list(data.keys()))

            # 2. Send ONE massive packet (The Pipeline)
            # This triggers your server's batch processing logic
            client.sock.sendall(req_mset + req_mget + req_mdel)

            # --- PIPELINE READ ---
            
            # 3. Read responses sequentially
            # Response 1: MSET -> Expect "+OK" (or equivalent)
            res_mset = read_full_response(client.sock)
            
            # Response 2: MGET -> Expect List
            res_mget = read_full_response(client.sock)

            # Response 3: MDEL -> Expect Integer
            res_mdel = read_full_response(client.sock)

            # --- VALIDATION ---
            
            # Verify MGET
            if not isinstance(res_mget, list):
                 raise Exception(f"MGET expected list, got {type(res_mget)}")
            if len(res_mget) != len(data):
                 raise Exception(f"MGET size mismatch. Expected {len(data)}, got {len(res_mget)}")
            
            keys = list(data.keys())
            for i, val in enumerate(res_mget):
                expected = data[keys[i]]
                if val != expected:
                    raise Exception(f"Corruption! Key {keys[i]}: Expected '{expected}', Got '{val}'")

            # Verify DEL
            if res_mdel != len(data):
                 raise Exception(f"DEL count mismatch. Expected {len(data)}, got {res_mdel}")

            # 4. Ghost Check (Separate round-trip is fine here to be safe)
            check_key = keys[random.randint(0, len(keys)-1)]
            val = client.send("get", check_key)
            if val is not None:
                raise Exception(f"Ghost Key found! {check_key} returned '{val}'")

            with lock:
                total_ops += (BATCH_SIZE * 3) + 1
                mset_flattened = []
            for k, v in data.items():
                mset_flattened.extend([k, v])
            
            req_mset = create_request("mset", *mset_flattened)
            req_mget = create_request("mget", *list(data.keys()))
            req_mdel = create_request("mdel", *list(data.keys()))

            # 2. Send ONE massive packet (The Pipeline)
            # This triggers your server's batch processing logic
            client.sock.sendall(req_mset + req_mget + req_mdel)

            # --- PIPELINE READ ---
            
            # 3. Read responses sequentially
            # Response 1: MSET -> Expect "+OK" (or equivalent)
            res_mset = read_full_response(client.sock)
            
            # Response 2: MGET -> Expect List
            res_mget = read_full_response(client.sock)

            # Response 3: MDEL -> Expect Integer
            res_mdel = read_full_response(client.sock)

            # --- VALIDATION ---
            
            # Verify MGET
            if not isinstance(res_mget, list):
                 raise Exception(f"MGET expected list, got {type(res_mget)}")
            if len(res_mget) != len(data):
                 raise Exception(f"MGET size mismatch. Expected {len(data)}, got {len(res_mget)}")
            
            keys = list(data.keys())
            for i, val in enumerate(res_mget):
                expected = data[keys[i]]
                if val != expected:
                    raise Exception(f"Corruption! Key {keys[i]}: Expected '{expected}', Got '{val}'")

            # Verify DEL
            if res_mdel != len(data):
                 raise Exception(f"DEL count mismatch. Expected {len(data)}, got {res_mdel}")

            # 4. Ghost Check (Separate round-trip is fine here to be safe)
            check_key = keys[random.randint(0, len(keys)-1)]
            val = client.send("get", check_key)
            if val is not None:
                raise Exception(f"Ghost Key found! {check_key} returned '{val}'")

            with lock:
                total_ops += (BATCH_SIZE * 3) + 1
                mset_flattened = []
            for k, v in data.items():
                mset_flattened.extend([k, v])
            
            req_mset = create_request("mset", *mset_flattened)
            req_mget = create_request("mget", *list(data.keys()))
            req_mdel = create_request("mdel", *list(data.keys()))

            # 2. Send ONE massive packet (The Pipeline)
            # This triggers your server's batch processing logic
            client.sock.sendall(req_mset + req_mget + req_mdel)

            # --- PIPELINE READ ---
            
            # 3. Read responses sequentially
            # Response 1: MSET -> Expect "+OK" (or equivalent)
            res_mset = read_full_response(client.sock)
            
            # Response 2: MGET -> Expect List
            res_mget = read_full_response(client.sock)

            # Response 3: MDEL -> Expect Integer
            res_mdel = read_full_response(client.sock)

            # --- VALIDATION ---
            
            # Verify MGET
            if not isinstance(res_mget, list):
                 raise Exception(f"MGET expected list, got {type(res_mget)}")
            if len(res_mget) != len(data):
                 raise Exception(f"MGET size mismatch. Expected {len(data)}, got {len(res_mget)}")
            
            keys = list(data.keys())
            for i, val in enumerate(res_mget):
                expected = data[keys[i]]
                if val != expected:
                    raise Exception(f"Corruption! Key {keys[i]}: Expected '{expected}', Got '{val}'")

            # Verify DEL
            if res_mdel != len(data):
                 raise Exception(f"DEL count mismatch. Expected {len(data)}, got {res_mdel}")

            # 4. Ghost Check (Separate round-trip is fine here to be safe)
            check_key = keys[random.randint(0, len(keys)-1)]
            val = client.send("get", check_key)
            if val is not None:
                raise Exception(f"Ghost Key found! {check_key} returned '{val}'")

            with lock:
                total_ops += (BATCH_SIZE * 3) + 1
                mset_flattened = []
            for k, v in data.items():
                mset_flattened.extend([k, v])
            
            req_mset = create_request("mset", *mset_flattened)
            req_mget = create_request("mget", *list(data.keys()))
            req_mdel = create_request("mdel", *list(data.keys()))

            # 2. Send ONE massive packet (The Pipeline)
            # This triggers your server's batch processing logic
            client.sock.sendall(req_mset + req_mget + req_mdel)

            # --- PIPELINE READ ---
            
            # 3. Read responses sequentially
            # Response 1: MSET -> Expect "+OK" (or equivalent)
            res_mset = read_full_response(client.sock)
            
            # Response 2: MGET -> Expect List
            res_mget = read_full_response(client.sock)

            # Response 3: MDEL -> Expect Integer
            res_mdel = read_full_response(client.sock)

            # --- VALIDATION ---
            
            # Verify MGET
            if not isinstance(res_mget, list):
                 raise Exception(f"MGET expected list, got {type(res_mget)}")
            if len(res_mget) != len(data):
                 raise Exception(f"MGET size mismatch. Expected {len(data)}, got {len(res_mget)}")
            
            keys = list(data.keys())
            for i, val in enumerate(res_mget):
                expected = data[keys[i]]
                if val != expected:
                    raise Exception(f"Corruption! Key {keys[i]}: Expected '{expected}', Got '{val}'")

            # Verify DEL
            if res_mdel != len(data):
                 raise Exception(f"DEL count mismatch. Expected {len(data)}, got {res_mdel}")

            # 4. Ghost Check (Separate round-trip is fine here to be safe)
            check_key = keys[random.randint(0, len(keys)-1)]
            val = client.send("get", check_key)
            if val is not None:
                raise Exception(f"Ghost Key found! {check_key} returned '{val}'")

            with lock:
                total_ops += (BATCH_SIZE * 3) + 1
                mset_flattened = []
            for k, v in data.items():
                mset_flattened.extend([k, v])
            
            req_mset = create_request("mset", *mset_flattened)
            req_mget = create_request("mget", *list(data.keys()))
            req_mdel = create_request("mdel", *list(data.keys()))

            # 2. Send ONE massive packet (The Pipeline)
            # This triggers your server's batch processing logic
            client.sock.sendall(req_mset + req_mget + req_mdel)

            # --- PIPELINE READ ---
            
            # 3. Read responses sequentially
            # Response 1: MSET -> Expect "+OK" (or equivalent)
            res_mset = read_full_response(client.sock)
            
            # Response 2: MGET -> Expect List
            res_mget = read_full_response(client.sock)

            # Response 3: MDEL -> Expect Integer
            res_mdel = read_full_response(client.sock)

            # --- VALIDATION ---
            
            # Verify MGET
            if not isinstance(res_mget, list):
                 raise Exception(f"MGET expected list, got {type(res_mget)}")
            if len(res_mget) != len(data):
                 raise Exception(f"MGET size mismatch. Expected {len(data)}, got {len(res_mget)}")
            
            keys = list(data.keys())
            for i, val in enumerate(res_mget):
                expected = data[keys[i]]
                if val != expected:
                    raise Exception(f"Corruption! Key {keys[i]}: Expected '{expected}', Got '{val}'")

            # Verify DEL
            if res_mdel != len(data):
                 raise Exception(f"DEL count mismatch. Expected {len(data)}, got {res_mdel}")

            # 4. Ghost Check (Separate round-trip is fine here to be safe)
            check_key = keys[random.randint(0, len(keys)-1)]
            val = client.send("get", check_key)
            if val is not None:
                raise Exception(f"Ghost Key found! {check_key} returned '{val}'")

            with lock:
                total_ops += (BATCH_SIZE * 3) + 1
                mset_flattened = []
            for k, v in data.items():
                mset_flattened.extend([k, v])
            
            req_mset = create_request("mset", *mset_flattened)
            req_mget = create_request("mget", *list(data.keys()))
            req_mdel = create_request("mdel", *list(data.keys()))

            # 2. Send ONE massive packet (The Pipeline)
            # This triggers your server's batch processing logic
            client.sock.sendall(req_mset + req_mget + req_mdel)

            # --- PIPELINE READ ---
            
            # 3. Read responses sequentially
            # Response 1: MSET -> Expect "+OK" (or equivalent)
            res_mset = read_full_response(client.sock)
            
            # Response 2: MGET -> Expect List
            res_mget = read_full_response(client.sock)

            # Response 3: MDEL -> Expect Integer
            res_mdel = read_full_response(client.sock)

            # --- VALIDATION ---
            
            # Verify MGET
            if not isinstance(res_mget, list):
                 raise Exception(f"MGET expected list, got {type(res_mget)}")
            if len(res_mget) != len(data):
                 raise Exception(f"MGET size mismatch. Expected {len(data)}, got {len(res_mget)}")
            
            keys = list(data.keys())
            for i, val in enumerate(res_mget):
                expected = data[keys[i]]
                if val != expected:
                    raise Exception(f"Corruption! Key {keys[i]}: Expected '{expected}', Got '{val}'")

            # Verify DEL
            if res_mdel != len(data):
                 raise Exception(f"DEL count mismatch. Expected {len(data)}, got {res_mdel}")

            # 4. Ghost Check (Separate round-trip is fine here to be safe)
            check_key = keys[random.randint(0, len(keys)-1)]
            val = client.send("get", check_key)
            if val is not None:
                raise Exception(f"Ghost Key found! {check_key} returned '{val}'")

            with lock:
                total_ops += (BATCH_SIZE * 3) + 1
                mset_flattened = []
            for k, v in data.items():
                mset_flattened.extend([k, v])
            
            req_mset = create_request("mset", *mset_flattened)
            req_mget = create_request("mget", *list(data.keys()))
            req_mdel = create_request("mdel", *list(data.keys()))

            # 2. Send ONE massive packet (The Pipeline)
            # This triggers your server's batch processing logic
            client.sock.sendall(req_mset + req_mget + req_mdel)

            # --- PIPELINE READ ---
            
            # 3. Read responses sequentially
            # Response 1: MSET -> Expect "+OK" (or equivalent)
            res_mset = read_full_response(client.sock)
            
            # Response 2: MGET -> Expect List
            res_mget = read_full_response(client.sock)

            # Response 3: MDEL -> Expect Integer
            res_mdel = read_full_response(client.sock)

            # --- VALIDATION ---
            
            # Verify MGET
            if not isinstance(res_mget, list):
                 raise Exception(f"MGET expected list, got {type(res_mget)}")
            if len(res_mget) != len(data):
                 raise Exception(f"MGET size mismatch. Expected {len(data)}, got {len(res_mget)}")
            
            keys = list(data.keys())
            for i, val in enumerate(res_mget):
                expected = data[keys[i]]
                if val != expected:
                    raise Exception(f"Corruption! Key {keys[i]}: Expected '{expected}', Got '{val}'")

            # Verify DEL
            if res_mdel != len(data):
                 raise Exception(f"DEL count mismatch. Expected {len(data)}, got {res_mdel}")

            # 4. Ghost Check (Separate round-trip is fine here to be safe)
            check_key = keys[random.randint(0, len(keys)-1)]
            val = client.send("get", check_key)
            if val is not None:
                raise Exception(f"Ghost Key found! {check_key} returned '{val}'")

            with lock:
                total_ops += (BATCH_SIZE * 3) + 1
                mset_flattened = []
            for k, v in data.items():
                mset_flattened.extend([k, v])
            
            req_mset = create_request("mset", *mset_flattened)
            req_mget = create_request("mget", *list(data.keys()))
            req_mdel = create_request("mdel", *list(data.keys()))

            # 2. Send ONE massive packet (The Pipeline)
            # This triggers your server's batch processing logic
            client.sock.sendall(req_mset + req_mget + req_mdel)

            # --- PIPELINE READ ---
            
            # 3. Read responses sequentially
            # Response 1: MSET -> Expect "+OK" (or equivalent)
            res_mset = read_full_response(client.sock)
            
            # Response 2: MGET -> Expect List
            res_mget = read_full_response(client.sock)

            # Response 3: MDEL -> Expect Integer
            res_mdel = read_full_response(client.sock)

            # --- VALIDATION ---
            
            # Verify MGET
            if not isinstance(res_mget, list):
                 raise Exception(f"MGET expected list, got {type(res_mget)}")
            if len(res_mget) != len(data):
                 raise Exception(f"MGET size mismatch. Expected {len(data)}, got {len(res_mget)}")
            
            keys = list(data.keys())
            for i, val in enumerate(res_mget):
                expected = data[keys[i]]
                if val != expected:
                    raise Exception(f"Corruption! Key {keys[i]}: Expected '{expected}', Got '{val}'")

            # Verify DEL
            if res_mdel != len(data):
                 raise Exception(f"DEL count mismatch. Expected {len(data)}, got {res_mdel}")

            # 4. Ghost Check (Separate round-trip is fine here to be safe)
            check_key = keys[random.randint(0, len(keys)-1)]
            val = client.send("get", check_key)
            if val is not None:
                raise Exception(f"Ghost Key found! {check_key} returned '{val}'")

            with lock:
                total_ops += (BATCH_SIZE * 3) + 1
                mset_flattened = []
            for k, v in data.items():
                mset_flattened.extend([k, v])
            
            req_mset = create_request("mset", *mset_flattened)
            req_mget = create_request("mget", *list(data.keys()))
            req_mdel = create_request("mdel", *list(data.keys()))

            # 2. Send ONE massive packet (The Pipeline)
            # This triggers your server's batch processing logic
            client.sock.sendall(req_mset + req_mget + req_mdel)

            # --- PIPELINE READ ---
            
            # 3. Read responses sequentially
            # Response 1: MSET -> Expect "+OK" (or equivalent)
            res_mset = read_full_response(client.sock)
            
            # Response 2: MGET -> Expect List
            res_mget = read_full_response(client.sock)

            # Response 3: MDEL -> Expect Integer
            res_mdel = read_full_response(client.sock)

            # --- VALIDATION ---
            
            # Verify MGET
            if not isinstance(res_mget, list):
                 raise Exception(f"MGET expected list, got {type(res_mget)}")
            if len(res_mget) != len(data):
                 raise Exception(f"MGET size mismatch. Expected {len(data)}, got {len(res_mget)}")
            
            keys = list(data.keys())
            for i, val in enumerate(res_mget):
                expected = data[keys[i]]
                if val != expected:
                    raise Exception(f"Corruption! Key {keys[i]}: Expected '{expected}', Got '{val}'")

            # Verify DEL
            if res_mdel != len(data):
                 raise Exception(f"DEL count mismatch. Expected {len(data)}, got {res_mdel}")

            # 4. Ghost Check (Separate round-trip is fine here to be safe)
            check_key = keys[random.randint(0, len(keys)-1)]
            val = client.send("get", check_key)
            if val is not None:
                raise Exception(f"Ghost Key found! {check_key} returned '{val}'")

            with lock:
                total_ops += (BATCH_SIZE * 3) + 1
                mset_flattened = []
            for k, v in data.items():
                mset_flattened.extend([k, v])
            
            req_mset = create_request("mset", *mset_flattened)
            req_mget = create_request("mget", *list(data.keys()))
            req_mdel = create_request("mdel", *list(data.keys()))

            # 2. Send ONE massive packet (The Pipeline)
            # This triggers your server's batch processing logic
            client.sock.sendall(req_mset + req_mget + req_mdel)

            # --- PIPELINE READ ---
            
            # 3. Read responses sequentially
            # Response 1: MSET -> Expect "+OK" (or equivalent)
            res_mset = read_full_response(client.sock)
            
            # Response 2: MGET -> Expect List
            res_mget = read_full_response(client.sock)

            # Response 3: MDEL -> Expect Integer
            res_mdel = read_full_response(client.sock)

            # --- VALIDATION ---
            
            # Verify MGET
            if not isinstance(res_mget, list):
                 raise Exception(f"MGET expected list, got {type(res_mget)}")
            if len(res_mget) != len(data):
                 raise Exception(f"MGET size mismatch. Expected {len(data)}, got {len(res_mget)}")
            
            keys = list(data.keys())
            for i, val in enumerate(res_mget):
                expected = data[keys[i]]
                if val != expected:
                    raise Exception(f"Corruption! Key {keys[i]}: Expected '{expected}', Got '{val}'")

            # Verify DEL
            if res_mdel != len(data):
                 raise Exception(f"DEL count mismatch. Expected {len(data)}, got {res_mdel}")

            # 4. Ghost Check (Separate round-trip is fine here to be safe)
            check_key = keys[random.randint(0, len(keys)-1)]
            val = client.send("get", check_key)
            if val is not None:
                raise Exception(f"Ghost Key found! {check_key} returned '{val}'")

            with lock:
                total_ops += (BATCH_SIZE * 3) + 1
                mset_flattened = []
            for k, v in data.items():
                mset_flattened.extend([k, v])
            
            req_mset = create_request("mset", *mset_flattened)
            req_mget = create_request("mget", *list(data.keys()))
            req_mdel = create_request("mdel", *list(data.keys()))

            # 2. Send ONE massive packet (The Pipeline)
            # This triggers your server's batch processing logic
            client.sock.sendall(req_mset + req_mget + req_mdel)

            # --- PIPELINE READ ---
            
            # 3. Read responses sequentially
            # Response 1: MSET -> Expect "+OK" (or equivalent)
            res_mset = read_full_response(client.sock)
            
            # Response 2: MGET -> Expect List
            res_mget = read_full_response(client.sock)

            # Response 3: MDEL -> Expect Integer
            res_mdel = read_full_response(client.sock)

            # --- VALIDATION ---
            
            # Verify MGET
            if not isinstance(res_mget, list):
                 raise Exception(f"MGET expected list, got {type(res_mget)}")
            if len(res_mget) != len(data):
                 raise Exception(f"MGET size mismatch. Expected {len(data)}, got {len(res_mget)}")
            
            keys = list(data.keys())
            for i, val in enumerate(res_mget):
                expected = data[keys[i]]
                if val != expected:
                    raise Exception(f"Corruption! Key {keys[i]}: Expected '{expected}', Got '{val}'")

            # Verify DEL
            if res_mdel != len(data):
                 raise Exception(f"DEL count mismatch. Expected {len(data)}, got {res_mdel}")

            # 4. Ghost Check (Separate round-trip is fine here to be safe)
            check_key = keys[random.randint(0, len(keys)-1)]
            val = client.send("get", check_key)
            if val is not None:
                raise Exception(f"Ghost Key found! {check_key} returned '{val}'")

            with lock:
                total_ops += (BATCH_SIZE * 3) + 1
                mset_flattened = []
            for k, v in data.items():
                mset_flattened.extend([k, v])
            
            req_mset = create_request("mset", *mset_flattened)
            req_mget = create_request("mget", *list(data.keys()))
            req_mdel = create_request("mdel", *list(data.keys()))

            # 2. Send ONE massive packet (The Pipeline)
            # This triggers your server's batch processing logic
            client.sock.sendall(req_mset + req_mget + req_mdel)

            # --- PIPELINE READ ---
            
            # 3. Read responses sequentially
            # Response 1: MSET -> Expect "+OK" (or equivalent)
            res_mset = read_full_response(client.sock)
            
            # Response 2: MGET -> Expect List
            res_mget = read_full_response(client.sock)

            # Response 3: MDEL -> Expect Integer
            res_mdel = read_full_response(client.sock)

            # --- VALIDATION ---
            
            # Verify MGET
            if not isinstance(res_mget, list):
                 raise Exception(f"MGET expected list, got {type(res_mget)}")
            if len(res_mget) != len(data):
                 raise Exception(f"MGET size mismatch. Expected {len(data)}, got {len(res_mget)}")
            
            keys = list(data.keys())
            for i, val in enumerate(res_mget):
                expected = data[keys[i]]
                if val != expected:
                    raise Exception(f"Corruption! Key {keys[i]}: Expected '{expected}', Got '{val}'")

            # Verify DEL
            if res_mdel != len(data):
                 raise Exception(f"DEL count mismatch. Expected {len(data)}, got {res_mdel}")

            # 4. Ghost Check (Separate round-trip is fine here to be safe)
            check_key = keys[random.randint(0, len(keys)-1)]
            val = client.send("get", check_key)
            if val is not None:
                raise Exception(f"Ghost Key found! {check_key} returned '{val}'")

            with lock:
                total_ops += (BATCH_SIZE * 3) + 1
                mset_flattened = []
            for k, v in data.items():
                mset_flattened.extend([k, v])
            
            req_mset = create_request("mset", *mset_flattened)
            req_mget = create_request("mget", *list(data.keys()))
            req_mdel = create_request("mdel", *list(data.keys()))

            # 2. Send ONE massive packet (The Pipeline)
            # This triggers your server's batch processing logic
            client.sock.sendall(req_mset + req_mget + req_mdel)

            # --- PIPELINE READ ---
            
            # 3. Read responses sequentially
            # Response 1: MSET -> Expect "+OK" (or equivalent)
            res_mset = read_full_response(client.sock)
            
            # Response 2: MGET -> Expect List
            res_mget = read_full_response(client.sock)

            # Response 3: MDEL -> Expect Integer
            res_mdel = read_full_response(client.sock)

            # --- VALIDATION ---
            
            # Verify MGET
            if not isinstance(res_mget, list):
                 raise Exception(f"MGET expected list, got {type(res_mget)}")
            if len(res_mget) != len(data):
                 raise Exception(f"MGET size mismatch. Expected {len(data)}, got {len(res_mget)}")
            
            keys = list(data.keys())
            for i, val in enumerate(res_mget):
                expected = data[keys[i]]
                if val != expected:
                    raise Exception(f"Corruption! Key {keys[i]}: Expected '{expected}', Got '{val}'")

            # Verify DEL
            if res_mdel != len(data):
                 raise Exception(f"DEL count mismatch. Expected {len(data)}, got {res_mdel}")

            # 4. Ghost Check (Separate round-trip is fine here to be safe)
            check_key = keys[random.randint(0, len(keys)-1)]
            val = client.send("get", check_key)
            if val is not None:
                raise Exception(f"Ghost Key found! {check_key} returned '{val}'")

            with lock:
                total_ops += (BATCH_SIZE * 3) + 1
                mset_flattened = []
            for k, v in data.items():
                mset_flattened.extend([k, v])
            
            req_mset = create_request("mset", *mset_flattened)
            req_mget = create_request("mget", *list(data.keys()))
            req_mdel = create_request("mdel", *list(data.keys()))

            # 2. Send ONE massive packet (The Pipeline)
            # This triggers your server's batch processing logic
            client.sock.sendall(req_mset + req_mget + req_mdel)

            # --- PIPELINE READ ---
            
            # 3. Read responses sequentially
            # Response 1: MSET -> Expect "+OK" (or equivalent)
            res_mset = read_full_response(client.sock)
            
            # Response 2: MGET -> Expect List
            res_mget = read_full_response(client.sock)

            # Response 3: MDEL -> Expect Integer
            res_mdel = read_full_response(client.sock)

            # --- VALIDATION ---
            
            # Verify MGET
            if not isinstance(res_mget, list):
                 raise Exception(f"MGET expected list, got {type(res_mget)}")
            if len(res_mget) != len(data):
                 raise Exception(f"MGET size mismatch. Expected {len(data)}, got {len(res_mget)}")
            
            keys = list(data.keys())
            for i, val in enumerate(res_mget):
                expected = data[keys[i]]
                if val != expected:
                    raise Exception(f"Corruption! Key {keys[i]}: Expected '{expected}', Got '{val}'")

            # Verify DEL
            if res_mdel != len(data):
                 raise Exception(f"DEL count mismatch. Expected {len(data)}, got {res_mdel}")

            # 4. Ghost Check (Separate round-trip is fine here to be safe)
            check_key = keys[random.randint(0, len(keys)-1)]
            val = client.send("get", check_key)
            if val is not None:
                raise Exception(f"Ghost Key found! {check_key} returned '{val}'")

            with lock:
                total_ops += (BATCH_SIZE * 3) + 1
                mset_flattened = []
            for k, v in data.items():
                mset_flattened.extend([k, v])
            
            req_mset = create_request("mset", *mset_flattened)
            req_mget = create_request("mget", *list(data.keys()))
            req_mdel = create_request("mdel", *list(data.keys()))

            # 2. Send ONE massive packet (The Pipeline)
            # This triggers your server's batch processing logic
            client.sock.sendall(req_mset + req_mget + req_mdel)

            # --- PIPELINE READ ---
            
            # 3. Read responses sequentially
            # Response 1: MSET -> Expect "+OK" (or equivalent)
            res_mset = read_full_response(client.sock)
            
            # Response 2: MGET -> Expect List
            res_mget = read_full_response(client.sock)

            # Response 3: MDEL -> Expect Integer
            res_mdel = read_full_response(client.sock)

            # --- VALIDATION ---
            
            # Verify MGET
            if not isinstance(res_mget, list):
                 raise Exception(f"MGET expected list, got {type(res_mget)}")
            if len(res_mget) != len(data):
                 raise Exception(f"MGET size mismatch. Expected {len(data)}, got {len(res_mget)}")
            
            keys = list(data.keys())
            for i, val in enumerate(res_mget):
                expected = data[keys[i]]
                if val != expected:
                    raise Exception(f"Corruption! Key {keys[i]}: Expected '{expected}', Got '{val}'")

            # Verify DEL
            if res_mdel != len(data):
                 raise Exception(f"DEL count mismatch. Expected {len(data)}, got {res_mdel}")

            # 4. Ghost Check (Separate round-trip is fine here to be safe)
            check_key = keys[random.randint(0, len(keys)-1)]
            val = client.send("get", check_key)
            if val is not None:
                raise Exception(f"Ghost Key found! {check_key} returned '{val}'")

            with lock:
                total_ops += (BATCH_SIZE * 3) + 1

    except Exception as e:
        with lock:
            errors += 1
        print(f"Thread {thread_id} CRASHED: {e}")
    finally:
        if client: client.close()

def monitor(num_threads):
    print(f"Starting Soak Test with {num_threads} threads...")
    last_ops = 0
    while True:
        time.sleep(1)
        with lock:
            current = total_ops
            curr_err = errors
        diff = current - last_ops
        last_ops = current
        print(f"Throughput: {diff} ops/sec | Total: {current} | Errors: {curr_err}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Custom Protocol Soak Tester")
    parser.add_argument("-t", "--threads", type=int, default=DEFAULT_THREADS, help="Number of concurrent threads")
    parser.add_argument("-p", "--prefix", type=str, default=DEFAULT_PREFIX, help="Key prefix string")
    parser.add_argument("--host", type=str, default=DEFAULT_HOST, help="Server Host")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT, help="Server Port")
    
    args = parser.parse_args()

    threading.Thread(target=monitor, args=(args.threads,), daemon=True).start()
    
    threads = []
    for i in range(args.threads):
        t = threading.Thread(target=worker, args=(i, args.host, args.port, args.prefix))
        t.start()
        threads.append(t)
    
    for t in threads:
        t.join()
