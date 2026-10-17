#include <stdbool.h>
#include <stddef.h>

#include "common/glob.h"

/*
 * Glob matching with Redis semantics (stringmatchlen).
 * Supported operators:
 * *       - Match any sequence of characters (including empty)
 * ?       - Match exactly one character
 * [abc]   - Match one of the characters, [^abc] one not in the set
 * [a-z]   - Ranges inside a set, reversed ranges work too
 * \x      - Match x literally, also inside a set
 */

// Matches the token at *pattern (anything but '*') against chr and moves
// *pattern past it
static bool
match_token (const char **pattern, unsigned char chr)
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
      bool negate = *pat == '^';
      if (negate)
	pat++;

      bool match = false;
      // An unclosed set runs to the end of the pattern, like in Redis
      while (*pat && *pat != ']')
	{
	  if (*pat == '\\' && pat[1])
	    {
	      pat++;
	      if ((unsigned char) *pat == chr)
		match = true;
	      pat++;
	    }
	  else if (pat[1] == '-' && pat[2])
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
      if (*pat == ']')
	pat++;
      *pattern = pat;
      return negate ? !match : match;
    }

  // A trailing backslash matches itself
  if (*pat == '\\' && pat[1])
    pat++;
  *pattern = pat + 1;
  return (unsigned char) *pat == chr;
}

bool
glob_match (const char *pattern, const char *string)
{
  // Every token but '*' matches exactly one character, so on a mismatch
  // it is enough to retry from the last '*', one character further on
  const char *star_pattern = NULL;
  const char *star_string = NULL;

  while (*string)
    {
      if (*pattern == '*')
	{
	  while (*pattern == '*')
	    pattern++;
	  if (*pattern == '\0')
	    return true;
	  star_pattern = pattern;
	  star_string = string;
	  continue;
	}

      const char *next = pattern;
      if (*pattern && match_token (&next, (unsigned char) *string))
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

  while (*pattern == '*')
    pattern++;

  return *pattern == '\0';
}
