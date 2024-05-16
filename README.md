# minis
Tiny redis clone in C

This is my attempt to play around with plain C and recreate a tiny slice of redis.
It mosly from the https://build-your-own.org/redis/, except that is all written in C++
and i instead wrote everything in c, this required changes and writing some utilities that are otherwise available in C++.
I have also swapped out the usage of syscall POLL for EPOLL for networking. That required some changes as well.
The only thing that is left in C++ is the client.

I have also included a very crude make file that leaves a lot to be desired.

Usage:
cd src
make

then start the server with:
./server

Open another shell and start issuing commands with the client such as:
./client set k 12
./client get k

etc.


