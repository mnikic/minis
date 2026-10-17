#ifndef GLOB_H_
#define GLOB_H_

#include "stdbool.h"

#include <stddef.h>

// Redis's glob rules; pattern and string may hold NUL bytes
bool glob_match_n (const char *pattern, size_t pattern_len,
		   const char *string, size_t string_len);

// glob_match_n for C strings
bool glob_match (const char *pattern, const char *string);

#endif // GLOB_H_
