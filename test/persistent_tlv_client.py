# =============================================================================
# TLV Protocol Persistent Client
# This version sends multiple commands over a single socket connection.
# =============================================================================

import socket
import struct
import sys
from time import sleep

# --- Protocol Constants ---
SER_NIL = 0    # No data (e.g., SET success)
SER_ERR = 1    # Error response
SER_STR = 2    # String data (e.g., GET success)
ERR_UNKNOWN_CMD = 1

SERVER_HOST = '127.0.0.1'
SERVER_PORT = 1234 

# =============================================================================
# Core Protocol Functions (Unchanged)
# =============================================================================

def create_request(command, *args):
    """Encodes a single command into the full protocol format (Big-Endian)."""
    parts = [command] + list(args)
    
    # 1. Build the payload (N + arguments)
    n_args = len(parts)
    # The first 4 bytes are N (number of arguments)
    payload_parts = [struct.pack('!I', n_args)]
    
    for part in parts:
        data = part.encode('utf-8')
        # 4-byte length prefix (Network Byte Order: !I)
        payload_parts.append(struct.pack('!I', len(data)))
        # String data
        payload_parts.append(data)
        
    payload = b''.join(payload_parts)
    
    # 2. Add the overall length prefix L
    length_prefix = struct.pack('!I', len(payload))
    return length_prefix + payload

def format_hex_dump(data):
    """Formats binary data into readable hex lines."""
    output = []
    for i in range(0, len(data)):
        output.append(data[i:i+1].hex())
    
    lines = []
    # Display 16 bytes per line for easy reading
    for i in range(0, len(output), 16):
        lines.append(' '.join(output[i:i+16]))
    return '\n'.join(lines)

def get_error_name(code):
    """Maps the error code to a readable name."""
    if code == ERR_UNKNOWN_CMD:
        return "ERR_UNKNOWN_CMD"
    return "Unknown Error Code"

def parse_response(response_data):
    """
    Parses the response using the C client's TLV logic:
    [4-byte Total Length L] [1-byte Type T] [Type-Specific Data]
    """
    
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
    
    # Payload starts at index 1 after the Type byte
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

        data = payload[data_start : data_start + str_len_host].decode('utf-8', errors='ignore')
        
        print(f"4. Data (Bytes 9+): '{data}'")
        print(f"SUCCESS: Successfully parsed string response.")
        
    else:
        print(f"WARNING: Unknown Type Code: {response_type}")

# =============================================================================
# Persistent Connection Handler (New Logic)
# =============================================================================

def send_and_receive(s, command, *args):
    """Sends a request on an existing socket and reads the full response."""
    
    cmd_str = f"{command} {' '.join(args)}"
    print(f"\n--- COMMAND: {cmd_str} ---")
    
    request_data = create_request(command, *args)
    
    try:
        # 1. Send the request
        s.sendall(request_data)
        
        # 2. Read the 4-byte length header for the response
        header = s.recv(4)
        if len(header) != 4:
            raise EOFError(f"Server closed connection while reading header for command: {cmd_str}")
        
        # Determine the total length L
        total_len = struct.unpack('!I', header)[0]
        
        # 3. Read the remaining payload (L bytes)
        payload = b''
        bytes_left = total_len
        while bytes_left > 0:
            chunk = s.recv(bytes_left)
            if not chunk: 
                raise EOFError(f"Server closed connection while reading payload for command: {cmd_str}")
            payload += chunk
            bytes_left -= len(chunk)
            
        full_response_data = header + payload
        
        print("\n--- Received Raw Response Data (Hex Dump) ---")
        print(f"Total Bytes Received: {len(full_response_data)}")
        print(format_hex_dump(full_response_data))
        
        print("\n--- Interpretation Attempt (TLV Protocol) ---")
        parse_response(full_response_data)
            
    except EOFError as e:
        print(f"ERROR: {e}")
        return False
    except Exception as e:
        print(f"An error occurred during transaction: {e}")
        return False
    
    return True

if __name__ == '__main__':
    s = None
    try:
        # Establish a single connection
        print(f"Attempting to connect to {SERVER_HOST}:{SERVER_PORT}...")
        s = socket.create_connection((SERVER_HOST, SERVER_PORT))
        print("Connection established. Running command sequence...")

        # --- Command Sequence (Multiple requests on one socket) ---
        
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
            # The test continues even if an error is encountered, 
            # unless the server closes the connection.
            pass

        # 6. GET a again to ensure the server state is maintained
        send_and_receive(s, 'get', 'a')
        
        print("\n--- Sequence Complete ---")

    except ConnectionRefusedError:
        print(f"\nFATAL ERROR: Connection refused. Is the server running on {SERVER_HOST}:{SERVER_PORT}?")
    except Exception as e:
        print(f"\nFATAL ERROR: A critical error occurred: {e}")
    finally:
        if s:
            s.close()
            print("Connection closed.")
