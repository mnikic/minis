**Minis: Tiny Redis Clone in C**

This project was initially inspired by the concepts in the build-your-own.org/redis/ guide.

However, it has evolved significatly into an implementation written entirely in pure C, diverging significantly through key architectural changes: swapping the networking foundation from poll to epoll for better non-blocking performance, implementing manual network byte order handling, and developing custom testing and build systems with full ASan/TSan/UBSan support.

Swapping the networking foundation from poll to epoll for better non-blocking performance.

Implementing manual network byte order handling for protocol serialization.

Building custom data structures (like the AVL tree for sorted sets).

Developing custom testing and build systems with full ASan/TSan/UBSan support.

**Design Notes: Why TSan?**

Although the main network processing uses a single-threaded, non-blocking I/O event loop (EPOLL), the server uses a dedicated thread pool (thread_pool.c) to offload operations that might otherwise block the main thread, such as expiry of large objects or other heavy computation.

Because the application is fundamentally multi-threaded, running TSan (Thread Sanitizer) is crucial to guarantee that all data structures shared between the main event loop and the background worker threads are correctly synchronized and free of data races.

**Network Byte Order**

Bytes are passed between the client and the server in Network Byte Order (big endian). This required some fiddling with custom conversions (hton_u32, hton_u64).

I did not implement a conversion for the double type because the IEEE 754 standard does not mandate a single network byte order for floating-point numbers. Transmitting double as raw bytes introduces a portability issue in cases where the client and server have different float endianness, though it is often accepted for simplicity.

**Usage**

1. Build and Run the Standard Server

Build:

```
make
```

This creates a build folder containing all compiled objects (build/obj) and executables (build/bin).

Start the Server:

```
./build/bin/server
```


Use the Client (in a separate shell):

```
./build/bin/client set k 12
./build/bin/client get k
# etc.
```



2. Run Tests

To run the full suite of integration tests (server must be running on the default port):

```
make test
# or
python src/test_cmds_extra.py
```


All tests should be passing. If not, please submit a PR or open an issue!

3. Build with Sanitizers (Debugging)

You can repeat the process with various sanitizers to check for deeper issues. Note: Before switching between sanitizer builds and the regular build, it is important to run make clean!

Address Sanitizer (ASan)

Build: ```make asan```

Start Server: ```./build/bin/server_asan```

Run Tests: ```make test-asan```

Undefined Behavior Sanitizer (UBSan)

Build: ```make ubsan```

Start Server: ```./build/bin/server_ubsan```

Run Tests:```make test-ubsan```

Thread Sanitizer (TSan)

Build: ```make tsan```

Start Server: ```./build/bin/server_tsan```

Run Tests: ```make test-tsan```

If any of the sanitizer builds reveal issues (crashes, errors, or data races), please open an issue!

Thanks,
M.
