#!/usr/bin/env python3

import shlex
import subprocess
import sys
import re
import os

# The relative path to the client executable as defined in the test cases
CLIENT_EXECUTABLE_REL_PATH = '../build/bin/client'

# --- Path Invariance Setup ---
# 1. Get the directory of the currently executing script
script_dir = os.path.dirname(os.path.abspath(__file__))
# 2. Construct the absolute, invariant path to the client executable
# os.path.normpath resolves the '..' segments correctly
client_executable_abs_path = os.path.normpath(
    os.path.join(script_dir, CLIENT_EXECUTABLE_REL_PATH)
)
# ------------------------------


CASES = r'''
# Basic zset operations
$ ../build/bin/client zscore asdf n1
(nil)
$ ../build/bin/client zquery xxx 1 asdf 1 10
(arr) len=0
(arr) end
$ ../build/bin/client zadd zset 1 n1
(int) 1
$ ../build/bin/client zadd zset 2 n2
(int) 1
$ ../build/bin/client zadd zset 1.1 n1
(int) 0
$ ../build/bin/client zscore zset n1
(dbl) 1.1
$ ../build/bin/client zquery zset 1 "" 0 10
(arr) len=4
(str) n1
(dbl) 1.1
(str) n2
(dbl) 2
(arr) end
$ ../build/bin/client zquery zset 1.1 "" 1 10
(arr) len=2
(str) n2
(dbl) 2
(arr) end
$ ../build/bin/client zquery zset 1.1 "" 2 10
(arr) len=0
(arr) end
$ ../build/bin/client zrem zset adsf
(int) 0
$ ../build/bin/client zrem zset n1
(int) 1
$ ../build/bin/client zquery zset 1 "" 0 10
(arr) len=2
(str) n2
(dbl) 2
(arr) end
$ ../build/bin/client del zset
(int) 1

# Test basic key-value operations
$ ../build/bin/client get key1
(nil)
$ ../build/bin/client set key1 value1
(nil)
$ ../build/bin/client get key1
(str) value1
$ ../build/bin/client set key1 value2
(nil)
$ ../build/bin/client get key1
(str) value2
$ ../build/bin/client del key1
(int) 1
$ ../build/bin/client get key1
(nil)
$ ../build/bin/client del key1
(int) 0

# Test multiple keys
$ ../build/bin/client set k1 v1
(nil)
$ ../build/bin/client set k2 v2
(nil)
$ ../build/bin/client set k3 v3
(nil)
$ ../build/bin/client get k1
(str) v1
$ ../build/bin/client get k2
(str) v2
$ ../build/bin/client get k3
(str) v3
$ ../build/bin/client del k1
(int) 1
$ ../build/bin/client del k2
(int) 1
$ ../build/bin/client del k3
(int) 1

# Test keys command
$ ../build/bin/client set a 1
(nil)
$ ../build/bin/client set b 2
(nil)
$ ../build/bin/client set c 3
(nil)
$ ../build/bin/client keys
(arr) len=3
(str) c
(str) b
(str) a
(arr) end
$ ../build/bin/client del a
(int) 1
$ ../build/bin/client del b
(int) 1
$ ../build/bin/client del c
(int) 1

# Test pexpire and pttl
$ ../build/bin/client set expkey value
(nil)
$ ../build/bin/client pexpire expkey 10000
(int) 1
$ ../build/bin/client get expkey
(str) value
$ ../build/bin/client del expkey
(int) 1

# Test pexpire on non-existent key
$ ../build/bin/client pexpire nokey 5000
(int) 0
$ ../build/bin/client pttl nokey
(int) -2

# Test zset with negative scores
$ ../build/bin/client zadd negset -5 n1
(int) 1
$ ../build/bin/client zadd negset -2.5 n2
(int) 1
$ ../build/bin/client zadd negset 0 n3
(int) 1
$ ../build/bin/client zquery negset -10 "" 0 10
(arr) len=6
(str) n1
(dbl) -5
(str) n2
(dbl) -2.5
(str) n3
(dbl) 0
(arr) end
$ ../build/bin/client zscore negset n1
(dbl) -5
$ ../build/bin/client zscore negset n2
(dbl) -2.5
$ ../build/bin/client del negset
(int) 1

# Test zset with same scores (lexicographic ordering)
$ ../build/bin/client zadd sameset 1 apple
(int) 1
$ ../build/bin/client zadd sameset 1 banana
(int) 1
$ ../build/bin/client zadd sameset 1 cherry
(int) 1
$ ../build/bin/client zquery sameset 0 "" 0 10
(arr) len=6
(str) apple
(dbl) 1
(str) banana
(dbl) 1
(str) cherry
(dbl) 1
(arr) end
$ ../build/bin/client del sameset
(int) 1

# Test zset update existing member
$ ../build/bin/client zadd updset 1 member
(int) 1
$ ../build/bin/client zscore updset member
(dbl) 1
$ ../build/bin/client zadd updset 5 member
(int) 0
$ ../build/bin/client zscore updset member
(dbl) 5
$ ../build/bin/client del updset
(int) 1

# Test large zset
$ ../build/bin/client zadd large 1 a
(int) 1
$ ../build/bin/client zadd large 2 b
(int) 1
$ ../build/bin/client zadd large 3 c
(int) 1
$ ../build/bin/client zadd large 4 d
(int) 1
$ ../build/bin/client zadd large 5 e
(int) 1
$ ../build/bin/client zquery large 0 "" 0 3
(arr) len=4
(str) a
(dbl) 1
(str) b
(dbl) 2
(arr) end
$ ../build/bin/client zquery large 0 "" 3 10
(arr) len=4
(str) d
(dbl) 4
(str) e
(dbl) 5
(arr) end
$ ../build/bin/client del large
(int) 1

# Test empty string values
$ ../build/bin/client set emptykey ""
(nil)
$ ../build/bin/client get emptykey
(str) 

$ ../build/bin/client del emptykey
(int) 1

# Test overwriting different types
$ ../build/bin/client set mixkey normalvalue
(nil)
$ ../build/bin/client get mixkey
(str) normalvalue
$ ../build/bin/client zadd mixkey 1 member
(err) 3 expect zset
$ ../build/bin/client del mixkey
(int) 1
$ ../build/bin/client zadd mixkey 1 member
(int) 1
$ ../build/bin/client zscore mixkey member
(dbl) 1
$ ../build/bin/client del mixkey
(int) 1

# Test zrem multiple times
$ ../build/bin/client zadd remset 1 a
(int) 1
$ ../build/bin/client zadd remset 2 b
(int) 1
$ ../build/bin/client zadd remset 3 c
(int) 1
$ ../build/bin/client zrem remset a
(int) 1
$ ../build/bin/client zrem remset a
(int) 0
$ ../build/bin/client zrem remset b
(int) 1
$ ../build/bin/client zrem remset c
(int) 1
$ ../build/bin/client del remset
(int) 1

# Test query with offset beyond size
$ ../build/bin/client zadd offtest 1 a
(int) 1
$ ../build/bin/client zadd offtest 2 b
(int) 1
$ ../build/bin/client zquery offtest 0 "" 10 10
(arr) len=0
(arr) end
$ ../build/bin/client del offtest
(int) 1

# Test fractional scores
$ ../build/bin/client zadd fracset 0.1 a
(int) 1
$ ../build/bin/client zadd fracset 0.2 b
(int) 1
$ ../build/bin/client zadd fracset 0.15 c
(int) 1
$ ../build/bin/client zquery fracset 0 "" 0 10
(arr) len=6
(str) a
(dbl) 0.1
(str) c
(dbl) 0.15
(str) b
(dbl) 0.2
(arr) end
$ ../build/bin/client del fracset
(int) 1

# Test special characters in keys and values
$ ../build/bin/client set "key with spaces" "value with spaces"
(nil)
$ ../build/bin/client get "key with spaces"
(str) value with spaces
$ ../build/bin/client del "key with spaces"
(int) 1

# Test pttl on key without expiry
$ ../build/bin/client set noexp value
(nil)
$ ../build/bin/client pttl noexp
(int) -1
$ ../build/bin/client del noexp
(int) 1

# Test sequential operations
$ ../build/bin/client set seq1 val1
(nil)
$ ../build/bin/client set seq2 val2
(nil)
$ ../build/bin/client get seq1
(str) val1
$ ../build/bin/client del seq1
(int) 1
$ ../build/bin/client set seq1 newval1
(nil)
$ ../build/bin/client get seq1
(str) newval1
$ ../build/bin/client get seq2
(str) val2
$ ../build/bin/client del seq1
(int) 1
$ ../build/bin/client del seq2
(int) 1

# Test zset boundaries
$ ../build/bin/client zadd boundset 0 zero
(int) 1
$ ../build/bin/client zadd boundset 100 hundred
(int) 1
$ ../build/bin/client zquery boundset 0 "" 0 1
(arr) len=2
(str) zero
(dbl) 0
(arr) end
$ ../build/bin/client zquery boundset 50 "" 0 10
(arr) len=2
(str) hundred
(dbl) 100
(arr) end
$ ../build/bin/client del boundset
(int) 1

# Test deleting while querying
$ ../build/bin/client zadd delquery 1 a
(int) 1
$ ../build/bin/client zadd delquery 2 b
(int) 1
$ ../build/bin/client zadd delquery 3 c
(int) 1
$ ../build/bin/client zrem delquery b
(int) 1
$ ../build/bin/client zquery delquery 0 "" 0 10
(arr) len=4
(str) a
(dbl) 1
(str) c
(dbl) 3
(arr) end
$ ../build/bin/client del delquery
(int) 1
'''


# Special validation rules for commands with non-deterministic output
def validate_output(cmd, expected, actual):
    # Handle pttl - check if value is in reasonable range
    if 'pttl' in cmd and '(int)' in expected:
        match_expected = re.search(r'\(int\) (-?\d+)', expected)
        match_actual = re.search(r'\(int\) (-?\d+)', actual)
        if match_expected and match_actual:
            expected_val = int(match_expected.group(1))
            actual_val = int(match_actual.group(1))
            # For positive expected values (TTL), actual should be <= expected and > 0
            if expected_val > 0:
                # Allow a small window for execution time
                return 0 < actual_val <= expected_val
            # For -1 (no expiry) or -2 (non-existent key), expect exact match
            else:
                return actual_val == expected_val
    
    # Default: exact match
    return actual == expected

cmds = []
outputs = []
lines = CASES.splitlines()
for i, x in enumerate(lines):
    # Preserve original spacing - don't strip if it's an output line
    if x.strip().startswith('$ ') or x.strip().startswith('#') or not x.strip():
        x = x.strip()
        if not x or x.startswith('#'):
            continue
        if x.startswith('$ '):
            cmds.append(x[2:])
            outputs.append('')
    else:
        # For output lines, only strip leading spaces to preserve trailing ones
        outputs[-1] = outputs[-1] + x.lstrip() + '\n'

assert len(cmds) == len(outputs)

passed = 0
failed = 0

for i, (cmd, expect) in enumerate(zip(cmds, outputs)):
    # Inject the absolute path into the command string
    invariant_cmd = cmd.replace(CLIENT_EXECUTABLE_REL_PATH, client_executable_abs_path)

    try:
        out = subprocess.check_output(shlex.split(invariant_cmd), stderr=subprocess.STDOUT).decode('utf-8')
        if validate_output(cmd, expect, out):
            passed += 1
            print(f"  Test {i+1}/{len(cmds)}: PASSED")
        else:
            failed += 1
            print(f"  Test {i+1}/{len(cmds)}: FAILED")
            print(f"  Command: {invariant_cmd}")
            print(f"  Expected:\n{expect}")
            print(f"  Got:\n{out}")
    except subprocess.CalledProcessError as e:
        failed += 1
        print(f"✗ Test {i+1}/{len(cmds)}: ERROR")
        print(f"  Command: {invariant_cmd}")
        print(f"  Error: {e}")
    except FileNotFoundError:
        failed += 1
        print(f"✗ Test {i+1}/{len(cmds)}: FATAL ERROR")
        print(f"  Could not find client executable at: {client_executable_abs_path}")
        print(f"  Please ensure the client is built at the expected relative path: {CLIENT_EXECUTABLE_REL_PATH} (relative to this script).")


print(f"\n{'='*60}")
print(f"Results: {passed} passed, {failed} failed out of {len(cmds)} tests")
print(f"{'='*60}")

if failed > 0:
    sys.exit(1)
