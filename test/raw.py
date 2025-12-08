import socket
import struct
import sys
from time import sleep

SERVER_HOST = '127.0.0.1'
SERVER_PORT = 1234

# Constants matching the test suite for reference
SER_NIL = 0
SER_ERR = 1
SER_STR = 2
SER_INT = 3
SER_DBL = 4
SER_ARR = 5

# --- Protocol Reading Utilities ---

def _parse_element(payload, offset):
    """
    Recursively parses a single serialized element (T, L, V...) starting from offset.
    Returns (parsed_value, new_offset).
    """
    if offset >= len(payload):
        raise ValueError("Payload exhausted while trying to read element type.")

    # 1. Read Type T (1 byte)
    response_type = payload[offset]
    offset += 1

    if response_type == SER_NIL:
        # SER_NIL returns no value, just the type byte
        return ("Success (NIL)", offset)

    elif response_type == SER_ERR:
        if offset + 8 > len(payload):
            raise ValueError("Payload exhausted while reading error code/string length.")

        err_code_host = struct.unpack('!I', payload[offset:offset + 4])[0]
        offset += 4

        str_len_host = struct.unpack('!I', payload[offset:offset + 4])[0]
        offset += 4

        if offset + str_len_host > len(payload):
            raise ValueError("Payload exhausted while reading error message data.")

        data = payload[offset:offset + str_len_host].decode('utf-8', errors='ignore').strip()
        offset += str_len_host

        return (f"Error Code {err_code_host}: {data}", offset)

    elif response_type == SER_STR:
        if offset + 4 > len(payload):
            raise ValueError("Payload exhausted while reading string length.")

        str_len_host = struct.unpack('!I', payload[offset:offset + 4])[0]
        offset += 4

        if offset + str_len_host > len(payload):
            raise ValueError("Payload exhausted while reading string data.")

        data = payload[offset:offset + str_len_host].decode('utf-8', errors='ignore')
        offset += str_len_host

        return (data, offset)

    elif response_type == SER_INT:
        # SER_INT is a 64-bit signed integer (8 bytes)
        int64_size = 8
        if offset + int64_size > len(payload):
            raise ValueError("Payload exhausted while reading 64-bit integer.")

        # !q is Big-Endian 64-bit signed long long
        data = struct.unpack('!q', payload[offset:offset + int64_size])[0]
        offset += int64_size

        return (data, offset)

    elif response_type == SER_ARR:
        if offset + 4 > len(payload):
            raise ValueError("Payload exhausted while reading array element count.")

        num_elements = struct.unpack('!I', payload[offset:offset + 4])[0]
        offset += 4
        print (f'num elements: {num_elements}')
        arr_data = []
        for i in range(num_elements):
            if offset >= len(payload):
                raise ValueError(f"Payload exhausted before reading element {i+1} of {num_elements}.")

            # RECURSIVE CALL
            element_value, offset = _parse_element(payload, offset)
            arr_data.append(element_value)

        return (arr_data, offset)

    return (f"Unknown Type Code {response_type}", offset)


def parse_response(response_data):
    """Processes the full response, including the initial length prefix."""
    if len(response_data) < 4:
        return ("ERROR: Response too short to contain a length header.", 0)

    total_len_net = response_data[0:4]
    total_len_host = struct.unpack('!I', total_len_net)[0]
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
        return None # Server closed connection gracefully (or timed out)
    if len(header) != 4:
        raise IOError("Failed to read complete 4-byte response header.")

    total_len_response = struct.unpack('!I', header)[0]

    print (f'received 4 bytes for the header {header} and its {total_len_response}')
    # Read the remaining payload (L bytes)
    payload = b''
    bytes_left = total_len_response
    while bytes_left > 0:
        chunk = s.recv(bytes_left)
        if not chunk:
            # Server closed connection prematurely during payload read
            raise IOError("Server closed connection prematurely while reading payload.")
        payload += chunk
        bytes_left -= len(chunk)

    return header + payload

# --- Transaction Function ---

def send_and_parse_transaction(s, raw_payload_bytes, command_name):
    """
    Sends a request over the persistent socket and processes the response.
    """
    total_len = len(raw_payload_bytes)
    length_prefix = struct.pack('!I', total_len)

    full_request = length_prefix + raw_payload_bytes

    print(f"\n--- Sending {command_name} Request ({len(full_request)} bytes) ---")
    
    # Don't print hex dumps inside a rapid loop unless debugging is critical
    # print(f"Raw Payload (N, L, D...): {raw_payload_bytes.hex()}")

    s.sendall(full_request)

    # 2. Receive and process response
    full_response_data = read_full_response(s)
    
    if full_response_data is None:
        return "FATAL ERROR: Server closed the connection unexpectedly."

    # print(f"\n--- Received Response ({len(full_response_data)} bytes) ---")
    # print(f"Response Header (L): {struct.unpack('!I', full_response_data[0:4])[0]}")
    # print(f"Hex Dump: {full_response_data.hex()}")

    parsed_result, parsed_len = parse_response(full_response_data)
    return parsed_result


if __name__ == '__main__':
    s = None
    key_name = 'a'
    value = 'hello'

    try:
        print(f"Establishing SINGLE persistent connection to {SERVER_HOST}:{SERVER_PORT}...")
        s = socket.create_connection((SERVER_HOST, SERVER_PORT))
        s.settimeout(10.0) # Set a generous client timeout
        print("Connection established. Running rapid fire commands...")

        # === 1. SET command (Expected: Success (NIL)) ===
        N = struct.pack('!I', 3)
        L1 = struct.pack('!I', 3); D1 = b'SET'
        L2 = struct.pack('!I', len(key_name)); D2 = key_name.encode()
        L3 = struct.pack('!I', len(value)); D3 = value.encode()
        set_payload = N + L1 + D1 + L2 + D2 + L3 + D3
        
        print(f"\n\n=== 1. Running SET '{key_name}' '{value}' ===")
        result = send_and_parse_transaction(s, set_payload, 'SET')
        print(f"PARSED RESULT (1): {result}")

        # === 2. GET command (Expected: 'hello') ===
        N_get = struct.pack('!I', 2)
        L1_get = struct.pack('!I', 3); D1_get = b'GET'
        L2_get = struct.pack('!I', len(key_name)); D2_get = key_name.encode()
        get_payload = N_get + L1_get + D1_get + L2_get + D2_get

        print("\n\n=== 2. Running GET 'a' (Expected: 'hello') ===")
        result = send_and_parse_transaction(s, get_payload, 'GET')
        print(f"PARSED RESULT (2): {result}")

        # === 3. KEYS command (Expected: Array including 'a') ===
        N_keys = struct.pack('!I', 1)
        L1_keys = struct.pack('!I', 4); D1_keys = b'KEYS'
        keys_payload = N_keys + L1_keys + D1_keys

        print("\n\n=== 3. Running KEYS (Expected: Array including 'a') ===")
        result = send_and_parse_transaction(s, keys_payload, 'KEYS')
        print(f"PARSED RESULT (3): {result}")

        # === 4. DEL command (Expected: SER_INT 1 (deleted count)) ===
        N_del = struct.pack('!I', 2)
        L1_del = struct.pack('!I', 3); D1_del = b'DEL'
        L2_del = struct.pack('!I', len(key_name)); D2_del = key_name.encode()
        del_payload = N_del + L1_del + D1_del + L2_del + D2_del

        print("\n\n=== 4. Running DEL 'a' (Expected: 1 (int)) ===")
        result = send_and_parse_transaction(s, del_payload, 'DEL')
        print(f"PARSED RESULT (4): {result}")

    except ConnectionRefusedError:
        print("\nFATAL ERROR: Connection refused. Is the server running?")
    except IOError as e:
        print(f"\nI/O Error during transaction: {e}")
    except Exception as e:
        print(f"\nAn unexpected error occurred: {e}")
    finally:
        if s:
            print("\nClosing persistent connection.")
            s.close()
