#ifndef SERVER_LOOP_H
#define SERVER_LOOP_H

#include <stdint.h>

/**
 * @brief Starts the main event loop for the server.
 * * @param port The TCP port number to bind the server socket to.
 * @return int Returns 0 on successful shutdown (not expected in this design),
 * or an error code if initialization fails.
 */
int server_run(uint16_t port);

#endif //SERVER_LOOP_H
