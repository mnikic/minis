#ifndef ZERO_COPY_H
#define ZERO_COPY_H

/*
 *============================================================================
 * Name             : zero_copy.h
 * Author           : Milos
 * Description      : Handling of Linux MSG_ZEROCOPY notifications.
 *
 * This module interacts with the kernel's error queue to receive notifications
 * when a zero-copy send operation has finished DMA transfer. This is the
 * signal that allows us to reuse the memory buffer.
 *============================================================================
 */

#include <stdbool.h>
#include <stdint.h>

#include "common/macros.h"
#include "io/connection.h"

/*
 * Checks the kernel error queue for ZeroCopy completion notifications.
 * If completions are found, it updates the Connection state (slots) via
 * the helper functions in connection.h.
 *
 * Returns: true if a notification was processed (progress was made).
 */
bool zc_process_completions (Conn * conn);

/*
 * Drains all pending errors/notifications from the socket's error queue.
 * This is used during connection teardown to ensure the socket is clean
 * before closing, preventing "resource temporarily unavailable" or leaks.
 */
COLD void zc_drain_errors (int file_desc);

#endif // ZERO_COPY_H
