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


typedef struct work
{
  void (*f) (void *);
  void *arg;
} Work;

typedef struct thead_pool
{
  pthread_t *threads;
  t_deque *queue;
  pthread_mutex_t mu;
  pthread_cond_t not_empty;
  size_t num_threads; // Store number of threads for join operation
  int stop;           // Flag to signal workers to exit
} ThreadPool;


void thread_pool_init (ThreadPool * thp, size_t num_threads);
void thread_pool_queue (ThreadPool * thp, void (*func) (void *), void *arg);
void thread_pool_destroy (ThreadPool * thp); // New function declaration

#endif /* THREAD_POOL_H_ */
