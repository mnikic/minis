/*
 * thread_pool.h
 *
 *  Created on: Jun 26, 2023
 *      Author: loshmi
 */

#ifndef THREAD_POOL_H_
#define THREAD_POOL_H_

#include <stdint.h>
#include <stddef.h>
#include <pthread.h>
#include "deque.h"


typedef struct work {
    void (*f)(void *);
    void *arg;
} Work;

typedef  struct thead_pool {
    pthread_t* threads;
    t_deque* queue;
    pthread_mutex_t mu;
    pthread_cond_t not_empty;
} ThreadPool;

void thread_pool_init(ThreadPool *tp, size_t num_threads);
void thread_pool_queue(ThreadPool *tp, void (*f)(void *), void *arg);

#endif /* THREAD_POOL_H_ */
