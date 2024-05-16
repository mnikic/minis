# minis
Tiny redis clone in C

This is my attempt to play around with plain C and recreate a tiny slice of redis.
It mostly from the https://build-your-own.org/redis/, except that is all written in C++ over there
and i instead wrote everything in pure C, this required changes and writing some utilities that are otherwise readily available in C++.
I have also swapped out the usage of syscall POLL for EPOLL for networking. That required some changes as well.

I have also included a very crude make file that leaves a lot to be desired.

Usage:
1) cd src
2) make

then start the server with:

3) ./server

Open another shell and start issuing commands with the client such as:

./client set k 12

./client get k

etc.


