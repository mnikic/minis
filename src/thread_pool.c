/*
 * thread_poll.c
 *
 *  Created on: Jun 26, 2023
 *      Author: loshmi
 */
#include <assert.h>
#include <pthread.h>
#include "thread_pool.h"
#include "common.h"


static void *
worker (void *arg)
{
  ThreadPool *tp = (ThreadPool *) arg;
  while (1)
    {
      pthread_mutex_lock (&tp->mu);
      // wait for the condition: a non-empty queue
      while (dq_empty (tp->queue))
	{
	  pthread_cond_wait (&tp->not_empty, &tp->mu);
	}

      // got the job
      Work *w = (Work *) dq_peek_front (tp->queue);
      dq_pop_front (tp->queue);
      pthread_mutex_unlock (&tp->mu);

      // do the work
      w->f (w->arg);
      // clean up!
      free (w);
    }
  return NULL;
}

void
thread_pool_init (ThreadPool *tp, size_t num_threads)
{
  assert (num_threads > 0);

  tp->queue = dq_init ();
  int rv = pthread_mutex_init (&tp->mu, NULL);
  assert (rv == 0);
  rv = pthread_cond_init (&tp->not_empty, NULL);
  assert (rv == 0);

  tp->threads = calloc (num_threads, sizeof (pthread_t));
  if (!tp->threads)
    die ("Out of memory thread_pool_init.");
  for (size_t i = 0; i < num_threads; ++i)
    {
      rv = pthread_create (&tp->threads[i], NULL, &worker, tp);
      pthread_detach (tp->threads[i]);
      assert (rv == 0);
    }
}

void
thread_pool_queue (ThreadPool *tp, void (*f) (void *), void *arg)
{
  Work *w = malloc (sizeof (Work));
  if (!w)
    die ("Out of memory at thread_pool_queue.");
  w->f = f;
  w->arg = arg;

  pthread_mutex_lock (&tp->mu);
  dq_push_back (tp->queue, w);
  pthread_cond_signal (&tp->not_empty);
  pthread_mutex_unlock (&tp->mu);
}
