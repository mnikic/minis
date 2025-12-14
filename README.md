# **Minis: Tiny Redis Clone in C**

Minis is a high-performance, single-threaded, non-blocking Redis-inspired key-value store. This project has evolved into a **fully custom, production-grade C implementation** focused on extreme I/O efficiency, leveraging modern Linux kernel features and robust C programming practices.

## **Key Architectural Divergence & High-Performance I/O**

Minis has moved far beyond the tutorial structure with the following critical changes and advanced I/O capabilities:

| Feature | Implementation Detail | Rationale |
| :---- | :---- | :---- |
| **I/O Foundation** | Swapped from poll() to the more efficient Linux-specific **epoll** in **Edge-Triggered (EPOLLET) mode**. | Superior scalability and performance by minimizing system calls and ensuring full buffer draining. |
| **Protocol** | Implemented **manual network byte order handling** (hton\_u32, hton\_u64). | Ensures correct protocol serialization across different machine architectures. |
| **Response Pipelining** | I/O is managed using a dedicated, lockless **Ring Buffer** for responses. | Enables full command pipelining. The server can process new commands while simultaneously flushing prior responses, maximizing throughput. |
| **Zero-Copy Writes** | **Hybrid Zero-Copy (ZC)** using `MSG_ZEROCOPY` for large responses (currently > 200KB). | Eliminates memory copies between user space, kernel space, and the NIC, drastically reducing CPU overhead for high-bandwidth transfers. |
| **Data Structures** | Building custom, advanced data structures (e.g., **AVL Tree** for sorted sets). | Full control over performance characteristics and memory layout. |
| **Development** | Custom testing and build systems with full **ASan/TSan/UBSan** support. | Guarantees code correctness, memory safety, and thread synchronization. |

## **Design Notes: Why is TSan even needed?**

Although the main network processing uses a single-threaded, non-blocking I/O event loop (epoll), the server employs a dedicated **thread pool** (thread\_pool.c) to offload operations that might otherwise block the main thread (e.g., expiry of large objects, heavy computation, persistent storage writes).  
Because the application is fundamentally multi-threaded, running **TSan (Thread Sanitizer)** is crucial. It guarantees that all data structures shared between the main event loop and the background worker threads are correctly synchronized and free of dangerous data races.

## **Server Memory Architecture: Achieving Zero-Allocation in the Core Loop**

The architecture ensures that the critical path—the command processing loop—runs entirely without dynamic memory allocation (malloc/free).

### **1\. Heap Allocation (Connection Management)**

Dynamic allocation is confined exclusively to the creation of a persistent connection context.

| Aspect | Detail | Purpose |
| :---- | :---- | :---- |
| **Allocation** | A single malloc/calloc occurs **only when a new client connects.** | Necessary trade-off to enable high scalability (10,000+ connections). |
| **Contents** | The allocation includes file descriptors, state flags, and the large Request and Response I/O buffers, **including the ring buffer slots**. | Long-lived data required for the duration of the connection. |
| **Rationale** | Placing this structure on the heap allows the server to manage thousands of concurrent connections efficiently, preventing exhaustion of the single event loop thread's limited stack space. |  |
| **Deallocation** | Occurs **only** when the client connection is closed (socket disconnect). |  |

### **2\. Stack Allocation (Core Command Processing)**

All operations within the do\_request or command execution function are handled using fast, stack-allocated memory. This is the source of the high performance and low latency.

* **Argument Vector:** The array of command arguments (char \*cmd\[32\]) is allocated directly on the stack, utilizing the known small limit of command arguments (num \<= 32). This completely eliminates the need for calloc and free for every command.  
* **Zero-Copy Logic:** Temporary variables used for in-place string null-termination (e.g., storing the original character before overwriting it with \\0) are simple local stack variables (char original\_char), completely avoiding heap interaction during parsing.
* **Response Slot Setup:** The command execution deposits results directly into the pre-allocated, heap-based response ring buffer, avoiding intermediate copies.

This architecture guarantees that the critical path—reading, parsing, and executing a command—is completely isolated from the overhead and latency fluctuations associated with the system heap.

## **Network Byte Order**

Data is transmitted between the client and server using **Network Byte Order (Big Endian)**. This required custom conversion functions (hton\_u32, hton\_u64) to handle multi-byte integer types correctly.  
**Note on Floating Point:** A conversion for the double type was intentionally omitted. The IEEE 754 standard does not mandate a single network byte order for floating-point numbers. Transmitting double as raw bytes introduces a potential portability issue (different float endianness) but is often accepted for simplicity in common architectures.

## **Usage**

### **1\. Build and Run the Standard Server**

Build:  
make

*This creates a build folder containing all compiled objects (build/obj) and executables (build/bin).*  
Start the Server:  
```
./build/bin/server
```

Use the Client (in a separate shell):  
```
./build/bin/client set k 12  
./build/bin/client get k
./build/bin/client set a 11
./build/bin/client mget k a  
```
\# etc.

There is also an interactive_client, which is there for, well, interactive usage of redis. Very alpha though.
```
./build/bin/interactive_client 
minis> set a 23
```
And fire away.

### **2\. Run Tests**

To run the full suite of integration tests (the server must be running on the default port):  
make test  
\# or  
python src/test\_cmds\_extra.py

All tests should be passing. If not, please submit a PR or open an issue\!

### **3\. Build with Sanitizers (Debugging)**

You can repeat the process with various sanitizers to check for deeper issues.  
**Important:** Before switching between sanitizer builds and the regular build, always run make clean\!

#### **Address Sanitizer (ASan)**

| Action | Command |
| :---- | :---- |
| **Build** | make asan |
| **Start Server** | ./build/bin/server\_asan |
| **Run Tests** | make test-asan |

#### **Undefined Behavior Sanitizer (UBSan)**

| Action | Command |
| :---- | :---- |
| **Build** | make ubsan |
| **Start Server** | ./build/bin/server\_ubsan |
| **Run Tests** | make test-ubsan |

#### **Thread Sanitizer (TSan)**

| Action | Command |
| :---- | :---- |
| **Build** | make tsan |
| **Start Server** | ./build/bin/server\_tsan |
| **Run Tests** | make test-tsan |

If any of the sanitizer builds reveal issues (crashes, errors, or data races), please open an issue\!  
Thanks,  
M.
