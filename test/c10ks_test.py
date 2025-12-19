import asyncio
import struct
import random
import time
import socket

SERVER_HOST = '127.0.0.1'
SERVER_PORT = 1234
NUM_CLIENTS = 5000  # The C10k Goal
PIPELINE_DEPTH = 1   # Increase to test pipelining

# Protocol Helper: Serialize "set key value"
def pack_command(args):
    # 1. Build the body: [NumArgs (4B)] + [ArgLen (4B) + ArgData]...
    body = bytearray()
    body.extend(struct.pack('!I', len(args))) # Num Args (Network Byte Order)
    
    for arg in args:
        arg_bytes = arg.encode('utf-8')
        body.extend(struct.pack('!I', len(arg_bytes))) # Arg Len
        body.extend(arg_bytes)                         # Arg Data
        
    # 2. Add the Total Length Header
    header = struct.pack('!I', len(body))
    return header + body

async def client_task(client_id, stats):
    try:
        reader, writer = await asyncio.open_connection(SERVER_HOST, SERVER_PORT)
    except Exception as e:
        stats['errors'] += 1
        return

    stats['connected'] += 1
    
    # Payload: SET key_id xxxx
    key = f"k{client_id}"
    value = "x" * 10 # Small payload
    payload = pack_command(["set", key, value])
    
    try:
        while True:
            # Send Request
            writer.write(payload)
            await writer.drain()
            
            # Read Response (Length-prefixed)
            # 1. Read 4 bytes length
            len_data = await reader.readexactly(4)
            res_len = struct.unpack('!I', len_data)[0]
            
            # 2. Read the body
            await reader.readexactly(res_len)
            
            stats['requests'] += 1
            
            # Sleep a tiny bit to simulate real user pacing (prevent flooding localhost)
            # Remove this await to verify max throughput
            await asyncio.sleep(1) 
            
    except Exception:
        stats['errors'] += 1
        stats['connected'] -= 1
        writer.close()
        try:
            await writer.wait_closed()
        except:
            pass

async def monitor(stats):
    """Prints stats every second"""
    while True:
        await asyncio.sleep(1)
        print(f"Clients: {stats['connected']} | "
              f"Req/s: {stats['requests']} | "
              f"Errors: {stats['errors']}")
        stats['requests'] = 0

async def main():
    stats = {'connected': 0, 'requests': 0, 'errors': 0}
    
    # Start Monitor
    asyncio.create_task(monitor(stats))
    
    # Ramp up connections slowly to avoid SYN flooding yourself
    print(f"Ramping up to {NUM_CLIENTS} clients...")
    tasks = []
    for i in range(NUM_CLIENTS):
        tasks.append(asyncio.create_task(client_task(i, stats)))
        if i % 100 == 0:
            await asyncio.sleep(0.01) # Yield to let event loop catch up
            
    print("All clients launched. Running load test...")
    await asyncio.gather(*tasks)

if __name__ == '__main__':
    # Increase Python's open file limit just in case
    import resource
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    resource.setrlimit(resource.RLIMIT_NOFILE, (50000, hard))
    
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nTest stopped.")
