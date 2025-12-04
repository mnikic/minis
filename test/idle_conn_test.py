#!/usr/bin/env python3
"""
Stress test for idle connection timeouts.
Opens many connections and keeps them idle to test server timeout handling.
"""

import socket
import time
import sys
import argparse

def create_idle_connection(host, port, conn_id):
    """Create a connection and keep it idle."""
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((host, port))
        print(f"[{conn_id}] Connected to {host}:{port}")
        return sock
    except Exception as e:
        print(f"[{conn_id}] Failed to connect: {e}")
        return None

def test_idle_connections(host, port, num_connections, idle_time):
    """Open many connections and keep them idle."""
    connections = []
    
    print(f"Opening {num_connections} connections to {host}:{port}...")
    
    # Open all connections
    for i in range(num_connections):
        sock = create_idle_connection(host, port, i)
        if sock:
            connections.append(sock)
        time.sleep(0.01)  # Small delay to avoid overwhelming server
    
    print(f"Successfully opened {len(connections)} connections")
    print(f"Keeping connections idle for {idle_time} seconds...")
    print("Server should start dropping connections after timeout (default 5s)")
    
    # Keep connections idle and check which ones get closed
    start_time = time.time()
    closed_count = 0
    
    while time.time() - start_time < idle_time:
        elapsed = int(time.time() - start_time)
        still_open = 0
        
        for i, sock in enumerate(connections):
            if sock is None:
                continue
                
            try:
                # Try to peek at the socket to see if it's still connected
                sock.setblocking(False)
                data = sock.recv(1, socket.MSG_PEEK | socket.MSG_DONTWAIT)
                if len(data) == 0:
                    # Connection closed by server
                    print(f"[{i}] Connection closed by server at {elapsed}s")
                    sock.close()
                    connections[i] = None
                    closed_count += 1
                else:
                    still_open += 1
            except BlockingIOError:
                # No data available, connection still open
                still_open += 1
            except Exception as e:
                # Connection died
                print(f"[{i}] Connection error at {elapsed}s: {e}")
                try:
                    sock.close()
                except:
                    pass
                connections[i] = None
                closed_count += 1
        
        # Print status every second
        if elapsed % 1 == 0:
            print(f"[{elapsed}s] Open: {still_open}, Closed: {closed_count}")
        
        time.sleep(0.1)
    
    # Clean up remaining connections
    for sock in connections:
        if sock:
            try:
                sock.close()
            except:
                pass
    
    print(f"\nTest complete!")
    print(f"Total connections: {num_connections}")
    print(f"Closed by server: {closed_count}")
    print(f"Still open: {num_connections - closed_count}")

def test_connection_survival(host, port, active_interval):
    """
    Test that active connections survive while idle ones die.
    Opens two connections: one active, one idle.
    """
    print(f"\nTesting active vs idle connections...")
    
    # Create two connections
    active_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    idle_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    
    active_sock.connect((host, port))
    idle_sock.connect((host, port))
    
    print("Created 1 active and 1 idle connection")
    print(f"Active connection will send commands every {active_interval}s")
    print("Idle connection will do nothing")
    
    start = time.time()
    duration = 20  # Run for 20 seconds
    
    while time.time() - start < duration:
        elapsed = int(time.time() - start)
        
        # Send a command on active connection every interval
        if elapsed % active_interval == 0 and elapsed > 0:
            try:
                # Send KEYS command
                # Protocol: [msg_len][num_args][arg1_len][arg1]
                # Use network byte order (big-endian) if server uses NBO
                import struct
                arg = b'keys'
                msg_content = struct.pack('>I', 1)  # num_args (network byte order)
                msg_content += struct.pack('>I', len(arg))  # arg length
                msg_content += arg
                
                cmd = struct.pack('>I', len(msg_content)) + msg_content
                
                print(f"[{elapsed}s] Active connection: sending {len(cmd)} bytes: {cmd.hex()}")
                active_sock.sendall(cmd)
                
                # Set timeout for reading
                active_sock.settimeout(2.0)
                
                print(f"[{elapsed}s] Active connection: waiting for response header...")
                # Read response header (4 bytes for length) - network byte order
                header = active_sock.recv(4)
                if len(header) == 0:
                    print(f"[{elapsed}s] Active connection: server closed connection!")
                    break
                if len(header) < 4:
                    print(f"[{elapsed}s] Active connection: incomplete header ({len(header)} bytes)")
                    break
                    
                # Read the rest of the response - use network byte order (big-endian)
                resp_len = struct.unpack('>I', header)[0]
                print(f"[{elapsed}s] Active connection: response length = {resp_len} bytes")
                
                if resp_len > 0 and resp_len < 100000:
                    response = b''
                    remaining = resp_len
                    while remaining > 0:
                        chunk = active_sock.recv(min(remaining, 4096))
                        if not chunk:
                            print(f"[{elapsed}s] Active connection: connection closed while reading response")
                            break
                        response += chunk
                        remaining -= len(chunk)
                    print(f"[{elapsed}s] Active connection: got full response ({len(response)}/{resp_len} bytes)")
                elif resp_len == 0:
                    print(f"[{elapsed}s] Active connection: empty response")
                else:
                    print(f"[{elapsed}s] Active connection: suspicious response length {resp_len}")
                    break
                    
            except socket.timeout:
                print(f"[{elapsed}s] Active connection: timeout waiting for response")
                break
            except Exception as e:
                print(f"[{elapsed}s] Active connection died: {e}")
                import traceback
                traceback.print_exc()
                break
        
        # Check if idle connection is still alive
        if idle_sock:
            try:
                idle_sock.setblocking(False)
                # Use MSG_PEEK to check without consuming data
                data = idle_sock.recv(1, socket.MSG_PEEK | socket.MSG_DONTWAIT)
                if len(data) == 0:
                    print(f"[{elapsed}s] Idle connection: CLOSED by server")
                    idle_sock.close()
                    idle_sock = None
            except BlockingIOError:
                # No data and no error = still connected
                pass
            except ConnectionResetError:
                print(f"[{elapsed}s] Idle connection: CLOSED (connection reset)")
                idle_sock = None
            except OSError as e:
                if e.errno == 9:  # Bad file descriptor
                    print(f"[{elapsed}s] Idle connection: already closed")
                    idle_sock = None
                else:
                    print(f"[{elapsed}s] Idle connection: error {e}")
                    idle_sock = None
            except Exception as e:
                print(f"[{elapsed}s] Idle connection: CLOSED ({e})")
                idle_sock = None
        
        time.sleep(1)
    
    # Cleanup
    try:
        active_sock.close()
    except:
        pass
    if idle_sock:
        try:
            idle_sock.close()
        except:
            pass
    
    print("\nActive vs idle test complete!")

def main():
    parser = argparse.ArgumentParser(description='Stress test Redis server idle timeouts')
    parser.add_argument('--host', default='127.0.0.1', help='Server host (default: 127.0.0.1)')
    parser.add_argument('--port', type=int, default=1234, help='Server port (default: 1234)')
    parser.add_argument('--connections', type=int, default=100, help='Number of connections (default: 100)')
    parser.add_argument('--idle-time', type=int, default=10, help='How long to keep idle (default: 10s)')
    parser.add_argument('--test-active', action='store_true', help='Test active vs idle connections')
    
    args = parser.parse_args()
    
    if args.test_active:
        test_connection_survival(args.host, args.port, active_interval=3)
    else:
        test_idle_connections(args.host, args.port, args.connections, args.idle_time)

if __name__ == '__main__':
    main()
