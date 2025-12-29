#ifndef _MINIS_PRIVATE_H_
#define _MINIS_PRIVATE_H_

// Include internal dependencies needed for the struct layout
#include "cache/hashtable.h"
#include "cache/heap.h"
#include "cache/thread_pool.h"
#include "common/lock.h"

struct Minis
{
  HMap db;
  Heap heap;
  ThreadPool tp;
  uint64_t dirty_count;
  uint64_t last_save_dirty_count;
  ENGINE_LOCK_T lock;
};

#endif // _MINIS_PRIVATE_H_
