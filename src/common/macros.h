// common/macros.h
#ifndef MACROS_H
#define MACROS_H

#include <stddef.h>		// For offsetof if not using builtin

#if defined(__GNUC__) || defined(__clang__)
#define likely(x)   __builtin_expect(!!(x), 1)
#define unlikely(x) __builtin_expect(!!(x), 0)
#else
#define likely(x)   (x)
#define unlikely(x) (x)
#endif

#define COLD __attribute__((cold))
#define HOT __attribute__((hot))
#define ALWAYS_INLINE __attribute__((always_inline)) inline
#define NOINLINE __attribute__((noinline))
#define NORETURN __attribute__((noreturn))

#define container_of(ptr, type, member) __extension__ ({                     \
    /* Type check: ensure 'ptr' is a pointer to the member's type */         \
    __typeof__(((type *)0)->member) *__mptr = (ptr);                  \
                                                                            \
    /* Calculate the address of the containing structure. Using char* for */\
    /* byte-level arithmetic is safe and avoids integer cast warnings. */    \
    (type *)((char *)__mptr - offsetof(type, member));                      \
})

#endif // MACROS_H
