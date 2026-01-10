#include <stdbool.h>
#include <stddef.h>

#include "common/glob.h"

/*
 * Simple glob matching.
 * Supported operators:
 * * - Match any sequence of characters (including empty)
 * ? - Match exactly one character
 */
bool
glob_match (const char *pattern, const char *string)
{
  const char *last_wildcard_idx = NULL;
  const char *backtrack_s_idx = NULL;

  while (*string)
    {
      if (*pattern == '?' || *pattern == *string)
	{
	  pattern++;
	  string++;
	}
      else if (*pattern == '*')
	{
	  last_wildcard_idx = pattern;
	  backtrack_s_idx = string;
	  pattern++;
	}
      else if (last_wildcard_idx)
	{
	  pattern = last_wildcard_idx + 1;
	  backtrack_s_idx++;
	  string = backtrack_s_idx;
	}
      else
	{
	  return false;
	}
    }

  while (*pattern == '*')
    pattern++;

  return *pattern == '\0';
}
