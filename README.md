# minis
Tiny redis clone in C

This is my attempt to play around with plain C and recreate a tiny slice of redis.
It mostly from the https://build-your-own.org/redis/, except that is all written in C++ over there
and i instead wrote everything in pure C, this required changes and writing some utilities that are otherwise readily available in C++.
I have also swapped out the usage of syscall POLL for EPOLL for networking. That required some changes as well.

I have also included a very crude make file that leaves a lot to be desired.

Usage:
1) make

This will create a build folder and place all *.o files (build/obj) and executable files (in build/bin) there. Then start the server with:

2) ./build/bin/server

Open another shell and start issuing commands with the client such as:

./build/bin/client set k 12

./build/bin/client get k

etc.


