#!/usr/bin/env python3

import shlex
import subprocess
import sys
import re
import os

# --- Dynamic Path Setup ---
CLIENT_BIN_PATH = os.environ.get('CLIENT_BIN_PATH', 'build/bin/release/minis-bench')
CLIENT_PLACEHOLDER = '{client_executable-abs_path}'
script_dir = os.path.dirname(os.path.abspath(__file__))

if os.path.isabs(CLIENT_BIN_PATH):
    client_executable_abs_path = CLIENT_BIN_PATH
else:
    client_executable_abs_path = os.path.normpath(
        os.path.join(script_dir, '..', CLIENT_BIN_PATH)
    )

print(f"--- Client Configuration ---")
print(f"CLIENT_BIN_NAME: {CLIENT_BIN_PATH}")
print(f"Calculated Path: {client_executable_abs_path}")
print(f"----------------------------")

CASES = r'''
# Basic zset operations
$ {client_executable-abs_path} zscore asdf n1
(nil)
$ {client_executable-abs_path} zquery xxx 1 asdf 1 10
(arr) len=0
(arr) end
$ {client_executable-abs_path} zadd zset 1 n1
(int) 1
$ {client_executable-abs_path} zadd zset 2 n2
(int) 1
$ {client_executable-abs_path} zadd zset 1.1 n1
(int) 0
$ {client_executable-abs_path} zscore zset n1
(dbl) 1.1
$ {client_executable-abs_path} zquery zset 1 "" 0 10
(arr) len=4
(str) n1
(dbl) 1.1
(str) n2
(dbl) 2
(arr) end
$ {client_executable-abs_path} zquery zset 1.1 "" 1 10
(arr) len=2
(str) n2
(dbl) 2
(arr) end
$ {client_executable-abs_path} zquery zset 1.1 "" 2 10
(arr) len=0
(arr) end
$ {client_executable-abs_path} zrem zset adsf
(int) 0
$ {client_executable-abs_path} zrem zset n1
(int) 1
$ {client_executable-abs_path} zquery zset 1 "" 0 10
(arr) len=2
(str) n2
(dbl) 2
(arr) end
$ {client_executable-abs_path} del zset
(int) 1

# Test basic key-value operations
$ {client_executable-abs_path} get key1
(nil)
$ {client_executable-abs_path} set key1 value1
(str) OK
$ {client_executable-abs_path} get key1
(str) value1
$ {client_executable-abs_path} set key1 value2
(str) OK
$ {client_executable-abs_path} get key1
(str) value2
$ {client_executable-abs_path} del key1
(int) 1
$ {client_executable-abs_path} get key1
(nil)
$ {client_executable-abs_path} del key1
(int) 0

# Test Hash Operations (Basic)
$ {client_executable-abs_path} hget user:1000 name
(nil)
$ {client_executable-abs_path} hset user:1000 name "Alice"
(int) 1
$ {client_executable-abs_path} hget user:1000 name
(str) Alice
$ {client_executable-abs_path} hset user:1000 name "Bob"
(int) 0
$ {client_executable-abs_path} hget user:1000 name
(str) Bob
$ {client_executable-abs_path} hset user:1000 email "bob@example.com"
(int) 1
$ {client_executable-abs_path} hget user:1000 email
(str) bob@example.com
$ {client_executable-abs_path} hget user:1000 age
(nil)
$ {client_executable-abs_path} del user:1000
(int) 1
$ {client_executable-abs_path} hget user:1000 name
(nil)

# Test Advanced Hash Operations (HGETALL, HDEL)
$ {client_executable-abs_path} hset car:1 model "Tesla"
(int) 1
$ {client_executable-abs_path} hset car:1 year "2024"
(int) 1
$ {client_executable-abs_path} hset car:1 color "Red"
(int) 1
$ {client_executable-abs_path} hgetall car:1
(arr) len=6
(str) color
(str) Red
(str) model
(str) Tesla
(str) year
(str) 2024
(arr) end
$ {client_executable-abs_path} hdel car:1 year
(int) 1
$ {client_executable-abs_path} hget car:1 year
(nil)
$ {client_executable-abs_path} hgetall car:1
(arr) len=4
(str) color
(str) Red
(str) model
(str) Tesla
(arr) end
$ {client_executable-abs_path} hdel car:1 model color
(int) 2
$ {client_executable-abs_path} exists car:1
(int) 0
$ {client_executable-abs_path} hdel car:1 whatever
(int) 0

# Test Hash Type Conflicts
$ {client_executable-abs_path} set str_key "some string"
(str) OK
$ {client_executable-abs_path} hset str_key field value
(err) 3 WRONGTYPE...
$ {client_executable-abs_path} hget str_key field
(err) 3 WRONGTYPE Operation against a key holding the wrong kind of value
$ {client_executable-abs_path} hdel str_key field
(int) 0
$ {client_executable-abs_path} hgetall str_key
(arr) len=0
(arr) end
$ {client_executable-abs_path} del str_key
(int) 1

# Test multiple keys
$ {client_executable-abs_path} set k1 v1
(str) OK
$ {client_executable-abs_path} set k2 v2
(str) OK
$ {client_executable-abs_path} set k3 v3
(str) OK
$ {client_executable-abs_path} get k1
(str) v1
$ {client_executable-abs_path} get k2
(str) v2
$ {client_executable-abs_path} get k3
(str) v3
$ {client_executable-abs_path} del k1
(int) 1
$ {client_executable-abs_path} del k2
(int) 1
$ {client_executable-abs_path} del k3
(int) 1

# Test keys command
$ {client_executable-abs_path} set a 1
(str) OK
$ {client_executable-abs_path} set b 2
(str) OK
$ {client_executable-abs_path} set c 3
(str) OK
$ {client_executable-abs_path} keys \*
(arr) len=3
(str) c
(str) a
(str) b
(arr) end
$ {client_executable-abs_path} keys d
(arr) len=0
(arr) end
$ {client_executable-abs_path} keys c
(arr) len=1
(str) c
(arr) end
$ {client_executable-abs_path} keys c?
(arr) len=0
(arr) end
$ {client_executable-abs_path} set ca 4
(str) OK
$ {client_executable-abs_path} keys c
(arr) len=1
(str) c
(arr) end
$ {client_executable-abs_path} keys c
(arr) len=1
(str) c
(arr) end
$ {client_executable-abs_path} keys c?
(arr) len=1
(str) ca
(arr) end
$ {client_executable-abs_path} keys c\*
(arr) len=2
(str) c
(str) ca
(arr) end
$ {client_executable-abs_path} keys \*
(arr) len=4
(str) c
(str) a
(str) ca
(str) b
(arr) end
$ {client_executable-abs_path} del a
(int) 1
$ {client_executable-abs_path} del b
(int) 1
$ {client_executable-abs_path} del c
(int) 1
$ {client_executable-abs_path} del ca
(int) 1

# Test pexpire and pttl
$ {client_executable-abs_path} set expkey value
(str) OK
$ {client_executable-abs_path} pexpire expkey 10000
(int) 1
$ {client_executable-abs_path} get expkey
(str) value
$ {client_executable-abs_path} del expkey
(int) 1

# Test pexpire on non-existent key
$ {client_executable-abs_path} pexpire nokey 5000
(int) 0
$ {client_executable-abs_path} pttl nokey
(int) -2

# Test zset with negative scores
$ {client_executable-abs_path} zadd negset -5 n1
(int) 1
$ {client_executable-abs_path} zadd negset -2.5 n2
(int) 1
$ {client_executable-abs_path} zadd negset 0 n3
(int) 1
$ {client_executable-abs_path} zquery negset -10 "" 0 10
(arr) len=6
(str) n1
(dbl) -5
(str) n2
(dbl) -2.5
(str) n3
(dbl) 0
(arr) end
$ {client_executable-abs_path} zscore negset n1
(dbl) -5
$ {client_executable-abs_path} zscore negset n2
(dbl) -2.5
$ {client_executable-abs_path} del negset
(int) 1

# Test zset with same scores (lexicographic ordering)
$ {client_executable-abs_path} zadd sameset 1 apple
(int) 1
$ {client_executable-abs_path} zadd sameset 1 banana
(int) 1
$ {client_executable-abs_path} zadd sameset 1 cherry
(int) 1
$ {client_executable-abs_path} zquery sameset 0 "" 0 10
(arr) len=6
(str) apple
(dbl) 1
(str) banana
(dbl) 1
(str) cherry
(dbl) 1
(arr) end
$ {client_executable-abs_path} del sameset
(int) 1

# Test zset update existing member
$ {client_executable-abs_path} zadd updset 1 member
(int) 1
$ {client_executable-abs_path} zscore updset member
(dbl) 1
$ {client_executable-abs_path} zadd updset 5 member
(int) 0
$ {client_executable-abs_path} zscore updset member
(dbl) 5
$ {client_executable-abs_path} del updset
(int) 1

# Test large zset
$ {client_executable-abs_path} zadd large 1 a
(int) 1
$ {client_executable-abs_path} zadd large 2 b
(int) 1
$ {client_executable-abs_path} zadd large 3 c
(int) 1
$ {client_executable-abs_path} zadd large 4 d
(int) 1
$ {client_executable-abs_path} zadd large 5 e
(int) 1
$ {client_executable-abs_path} zquery large 0 "" 0 3
(arr) len=6
(str) a
(dbl) 1
(str) b
(dbl) 2
(str) c
(dbl) 3
(arr) end
$ {client_executable-abs_path} zquery large 0 "" 3 10
(arr) len=4
(str) d
(dbl) 4
(str) e
(dbl) 5
(arr) end
$ {client_executable-abs_path} del large
(int) 1

# Test empty string values
$ {client_executable-abs_path} set emptykey ""
(str) OK
$ {client_executable-abs_path} get emptykey
(str) 

$ {client_executable-abs_path} del emptykey
(int) 1

# Test overwriting different types
$ {client_executable-abs_path} set mixkey normalvalue
(str) OK
$ {client_executable-abs_path} get mixkey
(str) normalvalue
$ {client_executable-abs_path} zadd mixkey 1 member
(err) 3 expect zset
$ {client_executable-abs_path} del mixkey
(int) 1
$ {client_executable-abs_path} zadd mixkey 1 member
(int) 1
$ {client_executable-abs_path} zscore mixkey member
(dbl) 1
$ {client_executable-abs_path} del mixkey
(int) 1

# Test zrem multiple times
$ {client_executable-abs_path} zadd remset 1 a
(int) 1
$ {client_executable-abs_path} zadd remset 2 b
(int) 1
$ {client_executable-abs_path} zadd remset 3 c
(int) 1
$ {client_executable-abs_path} zrem remset a
(int) 1
$ {client_executable-abs_path} zrem remset a
(int) 0
$ {client_executable-abs_path} zrem remset b
(int) 1
$ {client_executable-abs_path} zrem remset c
(int) 1
$ {client_executable-abs_path} del remset
(int) 1

# Test query with offset beyond size
$ {client_executable-abs_path} zadd offtest 1 a
(int) 1
$ {client_executable-abs_path} zadd offtest 2 b
(int) 1
$ {client_executable-abs_path} zquery offtest 0 "" 10 10
(arr) len=0
(arr) end
$ {client_executable-abs_path} del offtest
(int) 1

# Test fractional scores
$ {client_executable-abs_path} zadd fracset 0.1 a
(int) 1
$ {client_executable-abs_path} zadd fracset 0.2 b
(int) 1
$ {client_executable-abs_path} zadd fracset 0.15 c
(int) 1
$ {client_executable-abs_path} zquery fracset 0 "" 0 10
(arr) len=6
(str) a
(dbl) 0.1
(str) c
(dbl) 0.15
(str) b
(dbl) 0.2
(arr) end
$ {client_executable-abs_path} del fracset
(int) 1

# Test special characters in keys and values
$ {client_executable-abs_path} set "key with spaces" "value with spaces"
(str) OK
$ {client_executable-abs_path} get "key with spaces"
(str) value with spaces
$ {client_executable-abs_path} del "key with spaces"
(int) 1

# Test pttl on key without expiry
$ {client_executable-abs_path} set noexp value
(str) OK
$ {client_executable-abs_path} pttl noexp
(int) -1
$ {client_executable-abs_path} del noexp
(int) 1

# Test pttl and pexpire on key with short expiry
$ {client_executable-abs_path} set shortxp value
(str) OK
$ {client_executable-abs_path} get shortxp
(str) value
$ {client_executable-abs_path} pttl shortxp
(int) -1
$ {client_executable-abs_path} pexpire shortxp 100
(int) 1
$ {client_executable-abs_path} get shortxp
(str) value
$ sleep 0.15
$ {client_executable-abs_path} get shortxp
(nil)

# Test sequential operations
$ {client_executable-abs_path} set seq1 val1
(str) OK
$ {client_executable-abs_path} set seq2 val2
(str) OK
$ {client_executable-abs_path} get seq1
(str) val1
$ {client_executable-abs_path} del seq1
(int) 1
$ {client_executable-abs_path} set seq1 newval1
(str) OK
$ {client_executable-abs_path} get seq1
(str) newval1
$ {client_executable-abs_path} get seq2
(str) val2
$ {client_executable-abs_path} del seq1
(int) 1
$ {client_executable-abs_path} del seq2
(int) 1

# Test zset boundaries
$ {client_executable-abs_path} zadd boundset 0 zero
(int) 1
$ {client_executable-abs_path} zadd boundset 100 hundred
(int) 1
$ {client_executable-abs_path} zquery boundset 0 "" 0 1
(arr) len=2
(str) zero
(dbl) 0
(arr) end
$ {client_executable-abs_path} zquery boundset 50 "" 0 10
(arr) len=2
(str) hundred
(dbl) 100
(arr) end
$ {client_executable-abs_path} del boundset
(int) 1

# Test deleting while querying
$ {client_executable-abs_path} zadd delquery 1 a
(int) 1
$ {client_executable-abs_path} zadd delquery 2 b
(int) 1
$ {client_executable-abs_path} zadd delquery 3 c
(int) 1
$ {client_executable-abs_path} zrem delquery b
(int) 1
$ {client_executable-abs_path} zquery delquery 0 "" 0 10
(arr) len=4
(str) a
(dbl) 1
(str) c
(dbl) 3
(arr) end
$ {client_executable-abs_path} del delquery
(int) 1

# Test mset, mget, mdel
$ {client_executable-abs_path} mset a 1 b 2 c 3
(str) OK
$ {client_executable-abs_path} mget a b c
(arr) len=3
(str) 1
(str) 2
(str) 3
(arr) end
$ {client_executable-abs_path} get a
(str) 1
$ {client_executable-abs_path} get b
(str) 2
$ {client_executable-abs_path} get c
(str) 3
$ {client_executable-abs_path} mdel a b c
(int) 3
$ {client_executable-abs_path} del a
(int) 0
$ {client_executable-abs_path} del b
(int) 0
$ {client_executable-abs_path} del c
(int) 0

# Test incr and decr
$ {client_executable-abs_path} set counter 10
(str) OK
$ {client_executable-abs_path} incr counter
(int) 11
$ {client_executable-abs_path} get counter
(str) 11
$ {client_executable-abs_path} decr counter
(int) 10
$ {client_executable-abs_path} get counter
(str) 10

# Test incrby and decrby
$ {client_executable-abs_path} incrby counter 5
(int) 15
$ {client_executable-abs_path} decrby counter 3
(int) 12
$ {client_executable-abs_path} get counter
(str) 12
$ {client_executable-abs_path} del counter
(int) 1

# Test incr on non-existent key (starts at 0)
$ {client_executable-abs_path} incr newcount
(int) 1
$ {client_executable-abs_path} decrby newcount 10
(int) -9
$ {client_executable-abs_path} del newcount
(int) 1

# Test incr type validation
$ {client_executable-abs_path} set mytext "hello"
(str) OK
$ {client_executable-abs_path} incr mytext
(err) 4 value is not an integer or out of range
$ {client_executable-abs_path} del mytext
(int) 1

# Test incr on expired key (should reset)
$ {client_executable-abs_path} set expinc 100
(str) OK
$ {client_executable-abs_path} pexpire expinc 100
(int) 1
$ sleep 0.15
$ {client_executable-abs_path} incr expinc
(int) 1
$ {client_executable-abs_path} del expinc
(int) 1
# Test exists command
$ {client_executable-abs_path} set ex1 val1
(str) OK
$ {client_executable-abs_path} exists ex1
(int) 1
$ {client_executable-abs_path} exists nonex
(int) 0
$ {client_executable-abs_path} set ex2 val2
(str) OK
$ {client_executable-abs_path} exists ex1 ex2 nonex
(int) 2
$ {client_executable-abs_path} del ex1
(int) 1
$ {client_executable-abs_path} exists ex1
(int) 0
# Test exists on expired key
$ {client_executable-abs_path} pexpire ex2 100
(int) 1
$ sleep 0.15
$ {client_executable-abs_path} exists ex2
(int) 0
# Test HEXISTS
$ {client_executable-abs_path} hset user:2000 name "John"
(int) 1
$ {client_executable-abs_path} hexists user:2000 name
(int) 1
$ {client_executable-abs_path} hexists user:2000 age
(int) 0
$ {client_executable-abs_path} hexists user:missing key
(int) 0
$ {client_executable-abs_path} set str_key "hello"
(str) OK
$ {client_executable-abs_path} hexists str_key field
(err) 3 WRONGTYPE Operation against a key holding the wrong kind of value
$ {client_executable-abs_path} mdel user:2000 str_key
(int) 2
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
