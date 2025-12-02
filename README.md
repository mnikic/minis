minis

Tiny redis clone in C

This project was initially inspired by the concepts in the https://build-your-own.org/redis/ guide. However, it has evolved significatly into an implementation written entirely in pure C, diverging significantly through key architectural changes: swapping the networking foundation from poll to epoll for better non-blocking performance, implementing manual network byte order handling, and developing custom testing and build systems with full ASan/TSan/UBSan support.

Design Notes: Why TSan?

Although the main network processing uses a single-threaded, non-blocking I/O event loop (EPOLL), the server uses a dedicated thread pool (thread_pool.c) to offload operations that might otherwise block the main thread, such as expiry of large objects or other  heavy computation.

Because the application is fundamentally multi-threaded, running TSan is crucial to guarantee that all data structures shared between the main event loop and the background worker threads are correctly synchronized and free of data races.

Network Byte Order

Bytes are passed between the client and the server in Network Byte Order (big endian). This required some fiddling with custom conversions (hton_u32, hton_u64). I did not implement a conversion for the double type because the IEEE 754 standard does not mandate a single network byte order for floating-point numbers. Transmitting double as raw bytes introduces a portability issue in cases where the client and server have different float endianness, though it is often accepted for simplicity.

Usage:

make

This will create a build folder and place all *.o files (build/obj) and executable files (in build/bin) there. Then start the server with:

./build/bin/server

Open another shell and start issuing commands with the client such as:

./build/bin/client set k 12

./build/bin/client get k

etc.

or (optionally)

run the suite of tests with
python src/test_cmds_extra.py

or 
make test

(server needs to be running on the default port for this work!!!)

They should all be passing. if not either submit a PR or open an issue.

one can repeat it all but with

Build with: make asan

Start the server with: build/bin/server_asan

Run tests with: make test-asan

Tests should still pass, server should not crash, everything should still work (otherwise we have memory issues)

or alternatively

make ubsan

build/bin/server_ubsan

make test-ubsan

Same story, everything should just work.

or

make tsan

build/bin/server_tsan

Run tests with: make test-tsan

Again all should be squeeky clean.

Note: before switching from asan/ubsan/tsan or regular build it is important to run make clean!

If not send a PR or open and issue :).

Thanks
M.
