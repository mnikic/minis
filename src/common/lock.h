/*
 * lock.h
 *
 *  Created on: Dec 29, 2025
 *      Author: loshmi
 */
#ifndef LOCK_H_
#define LOCK_H_

#ifdef MINIS_EMBEDDED
#include <pthread.h>
#define ENGINE_LOCK_T pthread_mutex_t
#define ENGINE_LOCK_INIT(l) pthread_mutex_init(l, NULL)
#define ENGINE_LOCK(l) pthread_mutex_lock(l)
#define ENGINE_UNLOCK(l) pthread_mutex_unlock(l)
#define ENGINE_LOCK_DESTROY(l) pthread_mutex_destroy(l)
#else
    // Server mode: No-ops only 
#define ENGINE_LOCK_T int
#define ENGINE_LOCK_INIT(l)
#define ENGINE_LOCK(l)
#define ENGINE_UNLOCK(l)
#define ENGINE_LOCK_DESTROY(l)
#endif

#endif // LOCK_H_
