#include <stdbool.h>
#include <stddef.h>
#include <string.h>

#include "common/glob.h"

/*
 * Glob matching with Redis semantics (stringmatchlen).
 * Supported operators:
 * *       - Match any sequence of characters (including empty)
 * ?       - Match exactly one character
 * [abc]   - Match one of the characters, [^abc] one not in the set
 * [a-z]   - Ranges inside a set, reversed ranges work too
 * \x      - Match x literally, also inside a set
 * Patterns and strings may hold any bytes, NUL included.
 */

// Matches the token at *pattern (anything but '*', before end) against chr
// and moves *pattern past it
static bool
match_token (const char **pattern, const char *end, unsigned char chr)
{
  const char *pat = *pattern;

  if (*pat == '?')
    {
      *pattern = pat + 1;
      return true;
    }

  if (*pat == '[')
    {
      pat++;
      bool negate = pat < end && *pat == '^';
      if (negate)
	pat++;

      bool match = false;
      // An unclosed set runs to the end of the pattern, like in Redis
      while (pat < end && *pat != ']')
	{
	  if (*pat == '\\' && pat + 1 < end)
	    {
	      pat++;
	      if ((unsigned char) *pat == chr)
		match = true;
	      pat++;
	    }
	  else if (pat + 2 < end && pat[1] == '-')
	    {
	      unsigned char low = (unsigned char) pat[0];
	      unsigned char high = (unsigned char) pat[2];
	      if (low > high)
		{
		  unsigned char tmp = low;
		  low = high;
		  high = tmp;
		}
	      if (chr >= low && chr <= high)
		match = true;
	      pat += 3;
	    }
	  else
	    {
	      if ((unsigned char) *pat == chr)
		match = true;
	      pat++;
	    }
	}
      if (pat < end && *pat == ']')
	pat++;
      *pattern = pat;
      return negate ? !match : match;
    }

  // A trailing backslash matches itself
  if (*pat == '\\' && pat + 1 < end)
    pat++;
  *pattern = pat + 1;
  return (unsigned char) *pat == chr;
}

bool
glob_match_n (const char *pattern, size_t pattern_len, const char *string,
	      size_t string_len)
{
  const char *pat_end = pattern + pattern_len;
  const char *str_end = string + string_len;

  // Every token but '*' matches exactly one character, so on a mismatch
  // it is enough to retry from the last '*', one character further on
  const char *star_pattern = NULL;
  const char *star_string = NULL;

  while (string < str_end)
    {
      if (pattern < pat_end && *pattern == '*')
	{
	  while (pattern < pat_end && *pattern == '*')
	    pattern++;
	  if (pattern == pat_end)
	    return true;
	  star_pattern = pattern;
	  star_string = string;
	  continue;
	}

      const char *next = pattern;
      if (pattern < pat_end
	  && match_token (&next, pat_end, (unsigned char) *string))
	{
	  pattern = next;
	  string++;
	}
      else if (star_pattern)
	{
	  pattern = star_pattern;
	  string = ++star_string;
	}
      else
	{
	  return false;
	}
    }

  while (pattern < pat_end && *pattern == '*')
    pattern++;

  return pattern == pat_end;
}

bool
glob_match (const char *pattern, const char *string)
{
  return glob_match_n (pattern, strlen (pattern), string, strlen (string));
}
