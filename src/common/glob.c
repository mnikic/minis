#include <stdbool.h>
#include <stddef.h>

#include "glob.h"

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
	  // Case 1: Simple match or '?' wildcard
	  pattern++;
	  string++;
	}
      else if (*pattern == '*')
	{
	  // Case 2: '*' wildcard
	  // Remember where we are so we can backtrack if needed.
	  // Initially assume '*' matches NOTHING (skip pattern only).
	  last_wildcard_idx = pattern;
	  backtrack_s_idx = string;
	  pattern++;
	}
      else if (last_wildcard_idx)
	{
	  // Case 3: Mismatch, but we have a previous '*' to fall back on.
	  // Backtrack the pattern to the '*'
	  pattern = last_wildcard_idx + 1;
	  // Consume one more character from the string for the '*'
	  backtrack_s_idx++;
	  string = backtrack_s_idx;
	}
      else
	{
	  // Case 4: Mismatch and no '*' to save us.
	  return false;
	}
    }

  // Ignore trailing '*' in pattern (e.g. "abc*" matches "abc")
  while (*pattern == '*')
    {
      pattern++;
    }

  // Return true only if we reached the end of the pattern
  return *pattern == '\0';
}
