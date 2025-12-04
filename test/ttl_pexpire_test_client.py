import socket
import struct
import time
import sys

# --- Protocol Constants (Adjusted for easier reference) ---
SER_NIL = 0    # No data (e.g., SET success)
SER_ERR = 1    # Error response
SER_STR = 2    # String data (e.g., GET success)
SER_INT = 3
SER_DBL = 4
SER_ARR = 5


class CustomCacheClient:
    """
    Client using the confirmed persistent TLV protocol (Big-Endian).
    Response framing MUST be: [4-byte Total Length L] + [1-byte Type T] + [Type Data]
    """
    def __init__(self, host='127.0.0.1', port=1234):
        self.host = host
        self.port = port
        self.socket = None
        self.connect()

    def connect(self):
        """Establishes a connection to the server."""
        try:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.settimeout(5)
            self.socket.connect((self.host, self.port))
            self.socket.settimeout(None) 
            print(f"Connected to {self.host}:{self.port}")
        except socket.error as e:
            print(f"Connection error: {e}")
            self.socket = None
            raise

    def close(self):
        """Closes the connection."""
        if self.socket:
            self.socket.close()
            print("Connection closed.")
        self.socket = None

    def _create_request(self, cmd_array):
        """Encodes a command array into the full protocol format."""
        if not self.socket:
            raise ConnectionError("Not connected to the server.")
            
        # All arguments must be strings for the protocol encoding
        parts = [str(p) for p in cmd_array]
        
        # 1. Build the payload (N + arguments)
        n_args = len(parts)
        payload_parts = [struct.pack('!I', n_args)] # N (number of arguments)
        
        for part in parts:
            data = part.encode('utf-8')
            payload_parts.append(struct.pack('!I', len(data))) # Arg Length L_i
            payload_parts.append(data) # Arg Data
            
        payload = b''.join(payload_parts)
        
        # 2. Add the overall length prefix L
        length_prefix = struct.pack('!I', len(payload))
        return length_prefix + payload

    def _send_command(self, cmd_array):
        """Serializes and sends a command array."""
        serialized_data = self._create_request(cmd_array)
        self.socket.sendall(serialized_data)


    def _read_data(self, length, timeout=5.0):
        """Reads exactly 'length' bytes from the socket."""
        data = b''
        timeout_start = time.time()
        while len(data) < length:
            try:
                # Calculate remaining time for socket timeout
                remaining_time = timeout - (time.time() - timeout_start)
                if remaining_time <= 0 and len(data) == 0:
                    raise socket.timeout("Read timeout exceeded before reading any data.")
                
                # Use a small timeout for subsequent reads to check for server closure
                self.socket.settimeout(max(0.1, remaining_time) if len(data) < length else None)

                chunk = self.socket.recv(length - len(data))
                
            except socket.timeout:
                raise ConnectionError("Timeout: Server failed to send expected response data.")
            except socket.error as e:
                raise ConnectionError(f"Socket error during read: {e}")

            if not chunk:
                # Server closed connection unexpectedly
                raise ConnectionError("Server closed connection unexpectedly (0 bytes read).")
            data += chunk
            
        self.socket.settimeout(None) # Reset to blocking after successful read
        return data

    def _read_response(self):
        """
        Reads and deserializes the single response item from the server.
        Response framing: [4-byte Total Length L] + [1-byte Type T] + [Type Data]
        """
        if not self.socket:
            raise ConnectionError("Not connected to the server.")
        
        # 1. Read the 4-byte length prefix (L)
        try:
            length_header = self._read_data(4)
            total_payload_length = struct.unpack('!I', length_header)[0]
        except ConnectionError as e:
            # If connection is lost during L read, we return None gracefully to terminate the test
            print(f"DEBUG: Failed to read length header L: {e}", file=sys.stderr)
            return None 

        # 2. Read the full payload (T + D)
        if total_payload_length == 0:
            return None 
            
        full_payload = self._read_data(total_payload_length)

        # 3. Parse Type T (1 byte)
        type_byte = full_payload[0:1]
        data_type = struct.unpack('B', type_byte)[0]
        payload_data = full_payload[1:] # Data starts after the Type byte

        # Handle SER_NIL (Type T = 0, Payload Length L=1)
        if data_type == SER_NIL:
            if total_payload_length != 1:
                print(f"WARNING: NIL response expected length 1, got {total_payload_length}", file=sys.stderr)
            return None # Python None maps to NIL

        # Handle SER_ERR (Type T = 1, Payload Length is variable)
        elif data_type == SER_ERR:
            # We assume the payload_data is the full error message string for simplicity
            error_message = payload_data.decode('utf-8', errors='ignore')
            raise RuntimeError(f"Server Error (Type={data_type}): {error_message}")


        # Handle SER_STR (Type T = 2, Payload Length is 4 + String Length)
        elif data_type == SER_STR: 
            # String payload structure: [4-byte String Length SL] [String Data]
            length_bytes = payload_data[0:4]
            length = struct.unpack('!I', length_bytes)[0]
            
            # Data starts after the 4-byte length field
            data = payload_data[4 : 4 + length].decode('utf-8', errors='ignore')
            return data.rstrip('\x00') # Safely strip any potential trailing null byte

        # Handle SER_INT (Type T = 3, Payload Length L=9)
        elif data_type == SER_INT: 
            # Integer payload structure: [8-byte Signed Long Long]
            int_bytes = payload_data[0:8]
            return struct.unpack('!q', int_bytes)[0]

        # Handle SER_DBL (Type T = 4, Payload Length L=9)
        elif data_type == SER_DBL: 
            # Double payload structure: [8-byte Double]
            double_bytes = payload_data[0:8]
            return struct.unpack('!d', double_bytes)[0]
            
        # Handle SER_ARR (Type T = 5)
        elif data_type == SER_ARR: 
            # Array payload structure: [4-byte Array Count N]
            # Since an array can contain mixed types, we recursively call _read_response
            # The next response item in the stream MUST start with its own [L] [T] sequence.
            
            count_bytes = payload_data[0:4]
            array_len = struct.unpack('!I', count_bytes)[0]
            
            arr = []
            for _ in range(array_len):
                arr.append(self._read_response())
            return arr

        else:
            raise ValueError(f"Unknown response type received: {data_type} (from payload length {total_payload_length})")


    # --- Public API Methods ---

    def set(self, key, value):
        """SET key value"""
        self._send_command(['set', key, value])
        return self._read_response()

    def get(self, key):
        """GET key"""
        self._send_command(['get', key])
        return self._read_response()

    def del_(self, key):
        """DEL key"""
        self._send_command(['del', key])
        return self._read_response()

    def pttl(self, key):
        """PTTL key"""
        self._send_command(['pttl', key])
        return self._read_response()

    def pexpire(self, key, milliseconds):
        """PEXPIRE key milliseconds"""
        self._send_command(['pexpire', key, milliseconds])
        return self._read_response()

    def run_tests(self):
        print("\n--- Running Client Tests ---")
        
        try:
            # Cleanup keys
            self.del_("test_key")
            self.del_("expiring_key")

            # 1. Basic SET and GET
            print("\nTest 1: Basic SET and GET")
            self.set("test_key", "test_value_123")
            get_result = self.get("test_key")
            print(f"GET 'test_key' -> {get_result} (Expected: 'test_value_123')")
            
            # 2. DEL and subsequent GET
            print("\nTest 2: DEL and subsequent GET")
            del_count = self.del_("test_key")
            print(f"DEL 'test_key' -> {del_count} (Expected: 1)")
            
            del_result = self.get("test_key")
            print(f"GET 'test_key' after DEL -> {del_result} (Expected: None)")

            # 3. PEXPIRE and PTTL Test (Adjusted timing to avoid server timeout)
            print("\nTest 3: PEXPIRE and PTTL (Expiration in 5 seconds)")
            key_name = "expiring_key"
            expiry_ms = 5000  # 5 seconds
            
            # Set the key
            self.set(key_name, "time_sensitive_data")
            print(f"SET '{key_name}'")
            
            # Set the expiration
            pexpire_result = self.pexpire(key_name, expiry_ms)
            print(f"PEXPIRE '{key_name}' {expiry_ms}ms -> {pexpire_result} (Expected: 1 or success code)")

            # Check PTTL immediately
            pttl_result_1 = self.pttl(key_name)
            print(f"PTTL (1) '{key_name}' -> {pttl_result_1} (Expected: int, 0 < value <= {expiry_ms})")
            
            # Wait for 1 second
            time.sleep(1)
            
            # Check PTTL again
            pttl_result_2 = self.pttl(key_name)
            print(f"PTTL (2) '{key_name}' after 1s -> {pttl_result_2} (Expected: value < {pttl_result_1})")

            # Calculate remaining wait time (approx 4 seconds + 0.1s buffer)
            remaining_wait = (expiry_ms / 1000) - 1 + 0.1 
            print(f"Waiting {remaining_wait:.1f} seconds for key to expire...")
            time.sleep(remaining_wait) 
            
            # GET after expiry
            get_after_expire = self.get(key_name)
            print(f"GET '{key_name}' after expiry -> {get_after_expire} (Expected: None)")

            # PTTL after expiry (Should return -2 or -1 depending on server implementation)
            pttl_after_expire = self.pttl(key_name)
            print(f"PTTL '{key_name}' after expiry -> {pttl_after_expire} (Expected: -1 or -2)")

        except ConnectionError as e:
            print(f"\nCaught Connection Error: {e}")
            
        except RuntimeError as e:
            print(f"\nCaught Server Error: {e}")
            
        except Exception as e:
            print(f"\nAn error occurred during test execution: {e}")

        print("\n--- Tests Complete ---")

# Example Usage:
if __name__ == '__main__':
    client = None
    try:
        # NOTE: Make sure your server is running on port 1234
        client = CustomCacheClient(port=1234)
        client.run_tests()
    except Exception as e:
        print(f"\nAn error occurred during client initialization: {e}")
    finally:
        if client:
            client.close()
