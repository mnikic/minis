#ifndef BLOCKING_H_
#define BLOCKING_H_

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "io/connection.h"

/*
 * Clients waiting in BLPOP/BRPOP, oldest first, which is the order they
 * are served in. A waiting client's request stays unconsumed in its read
 * buffer, and serving it means running that request again, like Redis 7
 * does.
 */

// Marks conn as waiting, for timeout_us (0 is forever). A client that
// already waits keeps its place and its deadline.
void blocking_park (Conn * conn, uint64_t timeout_us, uint64_t now_us);

void blocking_unpark (Conn * conn);

size_t blocking_count (void);

// Moves the oldest waiting client to the back and returns it, NULL if
// none waits. Calling it blocking_count () times visits every client
// once, in order, and keeps the order of those still waiting.
Conn *blocking_rotate (void);

// Earliest deadline of a waiting client, (uint64_t) -1 if none
uint64_t blocking_next_deadline (void);

// True while waiting clients are served. Pushes made then do not make
// the pushing client yield, since serving goes on until nothing changes.
bool blocking_is_serving (void);
void blocking_set_serving (bool serving);

#endif /* BLOCKING_H_ */
