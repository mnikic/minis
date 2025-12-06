/*
 * thread_poll.c
 *
 * Created on: Jun 26, 2023
 * Author: loshmi
 */
#include <assert.h>
#include <pthread.h>
#include <stdlib.h>
#include "thread_pool.h"
#include "common/common.h"


static void *
worker (void *arg)
{
  ThreadPool *thp = (ThreadPool *) arg;
  while (1)
    {
      pthread_mutex_lock (&thp->mu);

      // Wait for the condition: a non-empty queue OR shutdown signal
      while (dq_empty (thp->queue) && !thp->stop)
	{
	  pthread_cond_wait (&thp->not_empty, &thp->mu);
	}

      // Check if the thread should exit (shutdown signal received)
      if (thp->stop)
	{
	  pthread_mutex_unlock (&thp->mu);
	  return NULL;		// Exit the thread
	}

      // got the job
      Work *wrk = (Work *) dq_peek_front (thp->queue);
      dq_pop_front (thp->queue);
      pthread_mutex_unlock (&thp->mu);

      // do the work
      wrk->f (wrk->arg);
      // clean up!
      free (wrk);
    }
  return NULL;
}

void
thread_pool_init (ThreadPool *thp, size_t num_threads)
{
  assert (num_threads > 0);

  thp->queue = dq_init ();
  thp->num_threads = num_threads;	// Store the number of threads
  thp->stop = 0;		// Initialize stop flag

  int ret_val = pthread_mutex_init (&thp->mu, NULL);
  assert (ret_val == 0);
  ret_val = pthread_cond_init (&thp->not_empty, NULL);
  assert (ret_val == 0);

  thp->threads = calloc (num_threads, sizeof (pthread_t));
  if (!thp->threads)
    die ("Out of memory thread_pool_init.");
  for (size_t i = 0; i < num_threads; ++i)
    {
      // Removed pthread_detach for controlled shutdown using pthread_join
      ret_val = pthread_create (&thp->threads[i], NULL, &worker, thp);
      assert (ret_val == 0);
    }
}

void
thread_pool_queue (ThreadPool *thp, void (*func) (void *), void *arg)
{
  // Do not allow queuing new work if the pool is stopping
  if (thp->stop)
    {
      // In a real scenario, this would return an error code or handle the work item
      // but for simple cleanup, we assume the caller stops queuing work first.
      return;
    }

  Work *wrk = malloc (sizeof (Work));
  if (!wrk)
    die ("Out of memory at thread_pool_queue.");
  wrk->f = func;
  wrk->arg = arg;

  pthread_mutex_lock (&thp->mu);
  dq_push_back (thp->queue, wrk);
  // Signal one thread to pick up the work
  pthread_cond_signal (&thp->not_empty);
  pthread_mutex_unlock (&thp->mu);
}

// Function to safely destroy the thread pool
void
thread_pool_destroy (ThreadPool *thp)
{
  // 1. Signal all workers to stop
  pthread_mutex_lock (&thp->mu);
  thp->stop = 1;
  // Wake up all waiting threads so they can check the stop flag
  pthread_cond_broadcast (&thp->not_empty);
  pthread_mutex_unlock (&thp->mu);

  // 2. Wait for all threads to finish their current work and exit
  for (size_t i = 0; i < thp->num_threads; ++i)
    {
      pthread_join (thp->threads[i], NULL);
    }

  // 3. Clean up remaining resources
  // Free the array of thread handles
  free (thp->threads);
  thp->threads = NULL;

  // Destroy synchronization primitives
  pthread_mutex_destroy (&thp->mu);
  pthread_cond_destroy (&thp->not_empty);

  // Clean up the queue structure. Note: any remaining items (Work*) in the queue
  // at this point are leaks unless the caller ensures the queue is empty.
  // We assume the caller (cache_free) ensures no more work is queued.
  // The dq_dispose function should ideally handle freeing memory allocated for the queue itself.
  dq_dispose (thp->queue);
}
