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
  ThreadPool *thp = (ThreadPool *) arg;
  while (1)
    {
      pthread_mutex_lock (&thp->mu);
      // wait for the condition: a non-empty queue
      while (dq_empty (thp->queue))
	{
	  pthread_cond_wait (&thp->not_empty, &thp->mu);
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
  int ret_val = pthread_mutex_init (&thp->mu, NULL);
  assert (ret_val == 0);
  ret_val = pthread_cond_init (&thp->not_empty, NULL);
  assert (ret_val == 0);

  thp->threads = calloc (num_threads, sizeof (pthread_t));
  if (!thp->threads)
    die ("Out of memory thread_pool_init.");
  for (size_t i = 0; i < num_threads; ++i)
    {
      ret_val = pthread_create (&thp->threads[i], NULL, &worker, thp);
      pthread_detach (thp->threads[i]);
      assert (ret_val == 0);
    }
}

void
thread_pool_queue (ThreadPool *thp, void (*func) (void *), void *arg)
{
  Work *wrk = malloc (sizeof (Work));
  if (!wrk)
    die ("Out of memory at thread_pool_queue.");
  wrk->f = func;
  wrk->arg = arg;

  pthread_mutex_lock (&thp->mu);
  dq_push_back (thp->queue, wrk);
  pthread_cond_signal (&thp->not_empty);
  pthread_mutex_unlock (&thp->mu);
}
