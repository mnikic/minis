# =============================================================================
# TLV Protocol Persistent Client with Large Request Test (Focusing on Closure)
# =============================================================================

import socket
import struct
import sys
from time import sleep

# --- Protocol Constants ---
SER_NIL = 0 	# No data (e.g., SET success)
SER_ERR = 1 	# Error response
SER_STR = 2 	# String data (e.g., GET success)
SER_INT = 3 	# Integer data
SER_DBL = 4 	# Double data
SER_ARR = 5 	# Array data

# Error Codes
ERR_UNKNOWN_CMD = 1
ERR_2BIG = 2 	# For requests exceeding K_MAX_MSG (e.g., 4096 bytes)
ERR_TYPE = 3
ERR_ARG = 4
ERR_MALFORMED = 5

	# The intended protocol code for Request Too Large

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
	"""Maps the error code to a readable name."""
	error_names = {
		ERR_UNKNOWN_CMD: "ERR_UNKNOWN_CMD",
		ERR_MALFORMED: "ERR_MALFORMED",
		ERR_2BIG: "ERR_2BIG (Request Too Large)"
	}
	return error_names.get(code, f"Unknown Error Code ({code})")

def parse_response(response_data):
	"""Parses the response using the TLV protocol and returns (type, value) or None."""
	if len(response_data) < 4:
		print("ERROR: Response too short for 4-byte length prefix.")
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
		
	else:
		print(f"WARNING: Unknown Type Code: {response_type}")
		return None

def send_and_receive(s, command, *args):
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


def test_oversized_request_and_closure(s):
	"""
	Tests the server's handling of an oversized request (10KB value).
	Expects ERR_2BIG (code 2 based on your previous output) response, 
	followed by connection closure.
	"""
	SIZE = 10000
	EXPECTED_CODE = ERR_2BIG
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
	
	# Check the response (The server is currently sending code 2)
	error_check_passed = False
	if result and result[0] == SER_ERR:
		code, msg = result[1]
		print("\n--- Error Validation ---")
		# Note: We check against the exact message provided by the user's test output
		if code == EXPECTED_CODE and msg == EXPECTED_MSG:
			print(f"✓ SUCCESS: Received expected error code {EXPECTED_CODE} and message: '{msg}'")
			error_check_passed = True
		else:
			print(f"✗ FAILURE: Received error code {code} ({get_error_name(code)}) with message: '{msg}'")
			print(f"   Expected: Code {EXPECTED_CODE} ({get_error_name(EXPECTED_CODE)}) and message: '{EXPECTED_MSG}'")
	else:
		print(f"✗ FAILURE: Did not receive an error response. Got: {result}")
	
	# If the error message check failed, we stop here.
	if not error_check_passed:
		return False

	# Test 2: Check if the connection is closed after sending the error.
	print("\n--- Connection Closure Check ---")
	sleep(0.1) # Give the server a moment to close the FD
	
	try:
		# Attempt a tiny command that should fail if the socket is closed
		s.sendall(create_request('get', 'a'))
		# If sendall succeeds, try to read the response header
		s.settimeout(1.0) # Set a small timeout for the read
		header = s.recv(4)
		s.settimeout(None) # Clear timeout
		
		if len(header) == 0:
			print("✓ SUCCESS: Connection confirmed closed (read returned EOF).")
			return True
		else:
			print("✗ FAILURE: Server accepted a subsequent command. Connection was NOT closed.")
			print("   Received next command header: ", header.hex())
			return False
	except socket.timeout:
		print("✗ FAILURE: Read timed out, but send succeeded. Connection status is ambiguous (likely still open).")
		return False
	except Exception as e:
		# This is the expected result when the server closes the socket gracefully before client sends
		if 'broken pipe' in str(e).lower() or 'connection reset' in str(e).lower():
			print("✓ SUCCESS: Connection confirmed closed (got 'Broken Pipe' or 'Connection Reset' on send).")
			return True
		print(f"✗ FAILURE: Error during final closure check: {e}")
		return False
	
	return True


if __name__ == '__main__':
	s = None
	try:
		# Establish a single connection
		print(f"Attempting to connect to {SERVER_HOST}:{SERVER_PORT}...")
		s = socket.create_connection((SERVER_HOST, SERVER_PORT))
		print("Connection established. Running command sequence...")

		# --- Basic Commands (Pre-condition Check) ---
		print(f"\n{'='*70}")
		print("BASIC FUNCTIONALITY TESTS (Pre-condition Check)")
		print(f"{'='*70}")
		
		# 1. SET a = "hello_world" (NIL response)
		if send_and_receive(s, 'set', 'a', 'hello_world')[1] is False: sys.exit(1)

		# 2. GET a (STR response)
		if send_and_receive(s, 'get', 'a')[1] is False: sys.exit(1)
		
		# --- Oversized Request Test ---
		test_oversized_request_and_closure(s)
		
		print("\n--- All Tests Complete ---")

	except ConnectionRefusedError:
		print(f"\nFATAL ERROR: Connection refused. Is the server running on {SERVER_HOST}:{SERVER_PORT}?")
		sys.exit(1)
	except Exception as e:
		print(f"\nFATAL ERROR: A critical error occurred: {e}")
		sys.exit(1)
	finally:
		if s:
			try:
				s.close()
				print("Client connection closed.")
			except:
				pass
