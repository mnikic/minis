#!/usr/bin/env python3

import shlex
import subprocess
import sys
import re
import os

# --- Dynamic Path Setup ---
# Read the client executable name from an environment variable, defaulting to 'client'.
CLIENT_BIN_NAME = os.environ.get('CLIENT_BIN_NAME', 'client')
# The relative path to the client executable as defined in the test cases
CLIENT_EXECUTABLE_REL_PATH = f'../build/bin/{CLIENT_BIN_NAME}'
# The literal placeholder string used in the CASES raw string
CLIENT_PLACEHOLDER = '{CLIENT_EXECUTABLE_REL_PATH}'

# 1. Get the directory of the currently executing script
script_dir = os.path.dirname(os.path.abspath(__file__))
# 2. Construct the absolute, invariant path to the client executable
# os.path.normpath resolves the '..' segments correctly
client_executable_abs_path = os.path.normpath(
    os.path.join(script_dir, CLIENT_EXECUTABLE_REL_PATH)
)
# ------------------------------

# *** DEBUGGING OUTPUT ***
print(f"--- Client Configuration ---")
print(f"CLIENT_BIN_NAME: {CLIENT_BIN_NAME}")
print(f"Calculated Path: {client_executable_abs_path}")
print(f"----------------------------")
# **************************


CASES = r'''
# Basic zset operations
$ {CLIENT_EXECUTABLE_REL_PATH} zscore asdf n1
(nil)
$ {CLIENT_EXECUTABLE_REL_PATH} zquery xxx 1 asdf 1 10
(arr) len=0
(arr) end
$ {CLIENT_EXECUTABLE_REL_PATH} zadd zset 1 n1
(int) 1
$ {CLIENT_EXECUTABLE_REL_PATH} zadd zset 2 n2
(int) 1
$ {CLIENT_EXECUTABLE_REL_PATH} zadd zset 1.1 n1
(int) 0
$ {CLIENT_EXECUTABLE_REL_PATH} zscore zset n1
(dbl) 1.1
$ {CLIENT_EXECUTABLE_REL_PATH} zquery zset 1 "" 0 10
(arr) len=4
(str) n1
(dbl) 1.1
(str) n2
(dbl) 2
(arr) end
$ {CLIENT_EXECUTABLE_REL_PATH} zquery zset 1.1 "" 1 10
(arr) len=2
(str) n2
(dbl) 2
(arr) end
$ {CLIENT_EXECUTABLE_REL_PATH} zquery zset 1.1 "" 2 10
(arr) len=0
(arr) end
$ {CLIENT_EXECUTABLE_REL_PATH} zrem zset adsf
(int) 0
$ {CLIENT_EXECUTABLE_REL_PATH} zrem zset n1
(int) 1
$ {CLIENT_EXECUTABLE_REL_PATH} zquery zset 1 "" 0 10
(arr) len=2
(str) n2
(dbl) 2
(arr) end
$ {CLIENT_EXECUTABLE_REL_PATH} del zset
(int) 1

# Test basic key-value operations
$ {CLIENT_EXECUTABLE_REL_PATH} get key1
(nil)
$ {CLIENT_EXECUTABLE_REL_PATH} set key1 value1
(nil)
$ {CLIENT_EXECUTABLE_REL_PATH} get key1
(str) value1
$ {CLIENT_EXECUTABLE_REL_PATH} set key1 value2
(nil)
$ {CLIENT_EXECUTABLE_REL_PATH} get key1
(str) value2
$ {CLIENT_EXECUTABLE_REL_PATH} del key1
(int) 1
$ {CLIENT_EXECUTABLE_REL_PATH} get key1
(nil)
$ {CLIENT_EXECUTABLE_REL_PATH} del key1
(int) 0

# Test multiple keys
$ {CLIENT_EXECUTABLE_REL_PATH} set k1 v1
(nil)
$ {CLIENT_EXECUTABLE_REL_PATH} set k2 v2
(nil)
$ {CLIENT_EXECUTABLE_REL_PATH} set k3 v3
(nil)
$ {CLIENT_EXECUTABLE_REL_PATH} get k1
(str) v1
$ {CLIENT_EXECUTABLE_REL_PATH} get k2
(str) v2
$ {CLIENT_EXECUTABLE_REL_PATH} get k3
(str) v3
$ {CLIENT_EXECUTABLE_REL_PATH} del k1
(int) 1
$ {CLIENT_EXECUTABLE_REL_PATH} del k2
(int) 1
$ {CLIENT_EXECUTABLE_REL_PATH} del k3
(int) 1

# Test keys command
$ {CLIENT_EXECUTABLE_REL_PATH} set a 1
(nil)
$ {CLIENT_EXECUTABLE_REL_PATH} set b 2
(nil)
$ {CLIENT_EXECUTABLE_REL_PATH} set c 3
(nil)
$ {CLIENT_EXECUTABLE_REL_PATH} keys \*
(arr) len=3
(str) c
(str) a
(str) b
(arr) end
$ {CLIENT_EXECUTABLE_REL_PATH} keys d
(arr) len=0
(arr) end
$ {CLIENT_EXECUTABLE_REL_PATH} keys c
(arr) len=1
(str) c
(arr) end
$ {CLIENT_EXECUTABLE_REL_PATH} keys c?
(arr) len=0
(arr) end
$ {CLIENT_EXECUTABLE_REL_PATH} set ca 4
(nil)
$ {CLIENT_EXECUTABLE_REL_PATH} keys c
(arr) len=1
(str) c
(arr) end
$ {CLIENT_EXECUTABLE_REL_PATH} keys c
(arr) len=1
(str) c
(arr) end
$ {CLIENT_EXECUTABLE_REL_PATH} keys c?
(arr) len=1
(str) ca
(arr) end
$ {CLIENT_EXECUTABLE_REL_PATH} keys c\*
(arr) len=2
(str) c
(str) ca
(arr) end
$ {CLIENT_EXECUTABLE_REL_PATH} keys \*
(arr) len=4
(str) c
(str) a
(str) ca
(str) b
(arr) end
$ {CLIENT_EXECUTABLE_REL_PATH} del a
(int) 1
$ {CLIENT_EXECUTABLE_REL_PATH} del b
(int) 1
$ {CLIENT_EXECUTABLE_REL_PATH} del c
(int) 1
$ {CLIENT_EXECUTABLE_REL_PATH} del ca
(int) 1

# Test pexpire and pttl
$ {CLIENT_EXECUTABLE_REL_PATH} set expkey value
(nil)
$ {CLIENT_EXECUTABLE_REL_PATH} pexpire expkey 10000
(int) 1
$ {CLIENT_EXECUTABLE_REL_PATH} get expkey
(str) value
$ {CLIENT_EXECUTABLE_REL_PATH} del expkey
(int) 1

# Test pexpire on non-existent key
$ {CLIENT_EXECUTABLE_REL_PATH} pexpire nokey 5000
(int) 0
$ {CLIENT_EXECUTABLE_REL_PATH} pttl nokey
(int) -2

# Test zset with negative scores
$ {CLIENT_EXECUTABLE_REL_PATH} zadd negset -5 n1
(int) 1
$ {CLIENT_EXECUTABLE_REL_PATH} zadd negset -2.5 n2
(int) 1
$ {CLIENT_EXECUTABLE_REL_PATH} zadd negset 0 n3
(int) 1
$ {CLIENT_EXECUTABLE_REL_PATH} zquery negset -10 "" 0 10
(arr) len=6
(str) n1
(dbl) -5
(str) n2
(dbl) -2.5
(str) n3
(dbl) 0
(arr) end
$ {CLIENT_EXECUTABLE_REL_PATH} zscore negset n1
(dbl) -5
$ {CLIENT_EXECUTABLE_REL_PATH} zscore negset n2
(dbl) -2.5
$ {CLIENT_EXECUTABLE_REL_PATH} del negset
(int) 1

# Test zset with same scores (lexicographic ordering)
$ {CLIENT_EXECUTABLE_REL_PATH} zadd sameset 1 apple
(int) 1
$ {CLIENT_EXECUTABLE_REL_PATH} zadd sameset 1 banana
(int) 1
$ {CLIENT_EXECUTABLE_REL_PATH} zadd sameset 1 cherry
(int) 1
$ {CLIENT_EXECUTABLE_REL_PATH} zquery sameset 0 "" 0 10
(arr) len=6
(str) apple
(dbl) 1
(str) banana
(dbl) 1
(str) cherry
(dbl) 1
(arr) end
$ {CLIENT_EXECUTABLE_REL_PATH} del sameset
(int) 1

# Test zset update existing member
$ {CLIENT_EXECUTABLE_REL_PATH} zadd updset 1 member
(int) 1
$ {CLIENT_EXECUTABLE_REL_PATH} zscore updset member
(dbl) 1
$ {CLIENT_EXECUTABLE_REL_PATH} zadd updset 5 member
(int) 0
$ {CLIENT_EXECUTABLE_REL_PATH} zscore updset member
(dbl) 5
$ {CLIENT_EXECUTABLE_REL_PATH} del updset
(int) 1

# Test large zset
$ {CLIENT_EXECUTABLE_REL_PATH} zadd large 1 a
(int) 1
$ {CLIENT_EXECUTABLE_REL_PATH} zadd large 2 b
(int) 1
$ {CLIENT_EXECUTABLE_REL_PATH} zadd large 3 c
(int) 1
$ {CLIENT_EXECUTABLE_REL_PATH} zadd large 4 d
(int) 1
$ {CLIENT_EXECUTABLE_REL_PATH} zadd large 5 e
(int) 1
$ {CLIENT_EXECUTABLE_REL_PATH} zquery large 0 "" 0 3
(arr) len=4
(str) a
(dbl) 1
(str) b
(dbl) 2
(arr) end
$ {CLIENT_EXECUTABLE_REL_PATH} zquery large 0 "" 3 10
(arr) len=4
(str) d
(dbl) 4
(str) e
(dbl) 5
(arr) end
$ {CLIENT_EXECUTABLE_REL_PATH} del large
(int) 1

# Test empty string values
$ {CLIENT_EXECUTABLE_REL_PATH} set emptykey ""
(nil)
$ {CLIENT_EXECUTABLE_REL_PATH} get emptykey
(str) 

$ {CLIENT_EXECUTABLE_REL_PATH} del emptykey
(int) 1

# Test overwriting different types
$ {CLIENT_EXECUTABLE_REL_PATH} set mixkey normalvalue
(nil)
$ {CLIENT_EXECUTABLE_REL_PATH} get mixkey
(str) normalvalue
$ {CLIENT_EXECUTABLE_REL_PATH} zadd mixkey 1 member
(err) 3 expect zset
$ {CLIENT_EXECUTABLE_REL_PATH} del mixkey
(int) 1
$ {CLIENT_EXECUTABLE_REL_PATH} zadd mixkey 1 member
(int) 1
$ {CLIENT_EXECUTABLE_REL_PATH} zscore mixkey member
(dbl) 1
$ {CLIENT_EXECUTABLE_REL_PATH} del mixkey
(int) 1

# Test zrem multiple times
$ {CLIENT_EXECUTABLE_REL_PATH} zadd remset 1 a
(int) 1
$ {CLIENT_EXECUTABLE_REL_PATH} zadd remset 2 b
(int) 1
$ {CLIENT_EXECUTABLE_REL_PATH} zadd remset 3 c
(int) 1
$ {CLIENT_EXECUTABLE_REL_PATH} zrem remset a
(int) 1
$ {CLIENT_EXECUTABLE_REL_PATH} zrem remset a
(int) 0
$ {CLIENT_EXECUTABLE_REL_PATH} zrem remset b
(int) 1
$ {CLIENT_EXECUTABLE_REL_PATH} zrem remset c
(int) 1
$ {CLIENT_EXECUTABLE_REL_PATH} del remset
(int) 1

# Test query with offset beyond size
$ {CLIENT_EXECUTABLE_REL_PATH} zadd offtest 1 a
(int) 1
$ {CLIENT_EXECUTABLE_REL_PATH} zadd offtest 2 b
(int) 1
$ {CLIENT_EXECUTABLE_REL_PATH} zquery offtest 0 "" 10 10
(arr) len=0
(arr) end
$ {CLIENT_EXECUTABLE_REL_PATH} del offtest
(int) 1

# Test fractional scores
$ {CLIENT_EXECUTABLE_REL_PATH} zadd fracset 0.1 a
(int) 1
$ {CLIENT_EXECUTABLE_REL_PATH} zadd fracset 0.2 b
(int) 1
$ {CLIENT_EXECUTABLE_REL_PATH} zadd fracset 0.15 c
(int) 1
$ {CLIENT_EXECUTABLE_REL_PATH} zquery fracset 0 "" 0 10
(arr) len=6
(str) a
(dbl) 0.1
(str) c
(dbl) 0.15
(str) b
(dbl) 0.2
(arr) end
$ {CLIENT_EXECUTABLE_REL_PATH} del fracset
(int) 1

# Test special characters in keys and values
$ {CLIENT_EXECUTABLE_REL_PATH} set "key with spaces" "value with spaces"
(nil)
$ {CLIENT_EXECUTABLE_REL_PATH} get "key with spaces"
(str) value with spaces
$ {CLIENT_EXECUTABLE_REL_PATH} del "key with spaces"
(int) 1

# Test pttl on key without expiry
$ {CLIENT_EXECUTABLE_REL_PATH} set noexp value
(nil)
$ {CLIENT_EXECUTABLE_REL_PATH} pttl noexp
(int) -1
$ {CLIENT_EXECUTABLE_REL_PATH} del noexp
(int) 1

# Test pttl and pexpire on key with short expiry
$ {CLIENT_EXECUTABLE_REL_PATH} set shortxp value
(nil)
$ {CLIENT_EXECUTABLE_REL_PATH} get shortxp
(str) value
$ {CLIENT_EXECUTABLE_REL_PATH} pttl shortxp
(int) -1
$ {CLIENT_EXECUTABLE_REL_PATH} pexpire shortxp 100
(int) 1
$ {CLIENT_EXECUTABLE_REL_PATH} get shortxp
(str) value
$ sleep 0.15
$ {CLIENT_EXECUTABLE_REL_PATH} get shortxp
(nil)

# Test sequential operations
$ {CLIENT_EXECUTABLE_REL_PATH} set seq1 val1
(nil)
$ {CLIENT_EXECUTABLE_REL_PATH} set seq2 val2
(nil)
$ {CLIENT_EXECUTABLE_REL_PATH} get seq1
(str) val1
$ {CLIENT_EXECUTABLE_REL_PATH} del seq1
(int) 1
$ {CLIENT_EXECUTABLE_REL_PATH} set seq1 newval1
(nil)
$ {CLIENT_EXECUTABLE_REL_PATH} get seq1
(str) newval1
$ {CLIENT_EXECUTABLE_REL_PATH} get seq2
(str) val2
$ {CLIENT_EXECUTABLE_REL_PATH} del seq1
(int) 1
$ {CLIENT_EXECUTABLE_REL_PATH} del seq2
(int) 1

# Test zset boundaries
$ {CLIENT_EXECUTABLE_REL_PATH} zadd boundset 0 zero
(int) 1
$ {CLIENT_EXECUTABLE_REL_PATH} zadd boundset 100 hundred
(int) 1
$ {CLIENT_EXECUTABLE_REL_PATH} zquery boundset 0 "" 0 1
(arr) len=2
(str) zero
(dbl) 0
(arr) end
$ {CLIENT_EXECUTABLE_REL_PATH} zquery boundset 50 "" 0 10
(arr) len=2
(str) hundred
(dbl) 100
(arr) end
$ {CLIENT_EXECUTABLE_REL_PATH} del boundset
(int) 1

# Test deleting while querying
$ {CLIENT_EXECUTABLE_REL_PATH} zadd delquery 1 a
(int) 1
$ {CLIENT_EXECUTABLE_REL_PATH} zadd delquery 2 b
(int) 1
$ {CLIENT_EXECUTABLE_REL_PATH} zadd delquery 3 c
(int) 1
$ {CLIENT_EXECUTABLE_REL_PATH} zrem delquery b
(int) 1
$ {CLIENT_EXECUTABLE_REL_PATH} zquery delquery 0 "" 0 10
(arr) len=4
(str) a
(dbl) 1
(str) c
(dbl) 3
(arr) end
$ {CLIENT_EXECUTABLE_REL_PATH} del delquery
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
    # Inject the absolute path into the command string, replacing the literal placeholder
    invariant_cmd = cmd.replace(CLIENT_PLACEHOLDER, client_executable_abs_path)
    command_parts = shlex.split(invariant_cmd)

    # *** NEW DEBUGGING OUTPUT ***
    if i == 0:
        print(f"  Debug: Command String (Test 1): {invariant_cmd}") # Show the full string after replacement
        print(f"  Debug: Command Parts (Test 1): {command_parts}")
    # ****************************

    try:
        out = subprocess.check_output(command_parts, stderr=subprocess.STDOUT).decode('utf-8')
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
        print(f"  Test {i+1}/{len(cmds)}: ERROR (Client Exited Non-Zero)")
        print(f"  Command: {invariant_cmd}")
        print(f"  Stderr/Stdout:\n{e.output.decode('utf-8')}")
        print(f"  Return Code: {e.returncode}")
    except FileNotFoundError:
        failed += 1
        print(f"  Test {i+1}/{len(cmds)}: FATAL ERROR (File Not Found by OS)")
        print(f"  Could not find client executable at: {client_executable_abs_path}")
        print(f"  Please ensure the path is correct and dependencies are met.")


print(f"\n{'='*60}")
print(f"Results: {passed} passed, {failed} failed out of {len(cmds)} tests")
print(f"{'='*60}")

if failed > 0:
    sys.exit(1)
