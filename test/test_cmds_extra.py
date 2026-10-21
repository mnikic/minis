#!/usr/bin/env python3

import shlex
import subprocess
import sys
import re
import os

# --- Dynamic Path Setup ---
CLIENT_BIN_PATH = os.environ.get('CLIENT_BIN_PATH', 'build/bin/release/minis-bench')
CLIENT_PLACEHOLDER = '{client_executable-abs_path}'
# The server's port, the client's default (1234) when unset
MINIS_PORT = os.environ.get('MINIS_PORT', '')
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
print(f"MINIS_PORT: {MINIS_PORT or 'default'}")
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
(err) 3 WRONGTYPE Operation against a key holding the wrong kind of value
$ {client_executable-abs_path} hget str_key field
(err) 3 WRONGTYPE Operation against a key holding the wrong kind of value
$ {client_executable-abs_path} hdel str_key field
(err) 3 WRONGTYPE Operation against a key holding the wrong kind of value
$ {client_executable-abs_path} hgetall str_key
(err) 3 WRONGTYPE Operation against a key holding the wrong kind of value
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
(str) b
(str) a
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
(str) b
(str) ca
(str) a
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
(err) 3 WRONGTYPE Operation against a key holding the wrong kind of value
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
(int) 0

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
# Test SET removes TTL (Persistence)
$ {client_executable-abs_path} set set_ttl_rem val1
(str) OK
$ {client_executable-abs_path} pexpire set_ttl_rem 10000
(int) 1
# Verify TTL is active (positive integer)
$ {client_executable-abs_path} pttl set_ttl_rem
(int) 10000
# Overwrite with SET
$ {client_executable-abs_path} set set_ttl_rem val2
(str) OK
# Verify TTL is gone (-1 means persistent)
$ {client_executable-abs_path} pttl set_ttl_rem
(int) -1
$ {client_executable-abs_path} del set_ttl_rem
(int) 1

# Test MSET removes TTL (Persistence)
$ {client_executable-abs_path} set mset_ttl_rem val1
(str) OK
$ {client_executable-abs_path} pexpire mset_ttl_rem 10000
(int) 1
# Verify TTL is active
$ {client_executable-abs_path} pttl mset_ttl_rem
(int) 10000
# Overwrite with MSET
$ {client_executable-abs_path} mset mset_ttl_rem val2 other_k other_v
(str) OK
# Verify TTL is gone
$ {client_executable-abs_path} pttl mset_ttl_rem
(int) -1
$ {client_executable-abs_path} mdel mset_ttl_rem other_k
(int) 2
# Test: SET overwrites complex types (ZSET/HASH)
# Redis Rule: SET is destructive. It should replace a ZSET with a String.
$ {client_executable-abs_path} zadd zset_to_kill 1 member
(int) 1
$ {client_executable-abs_path} set zset_to_kill "I am a string now"
(str) OK
$ {client_executable-abs_path} get zset_to_kill
(str) I am a string now
$ {client_executable-abs_path} zscore zset_to_kill member
(err) 3 WRONGTYPE Operation against a key holding the wrong kind of value
$ {client_executable-abs_path} del zset_to_kill
(int) 1

# Test: SET same value clears TTL (Idempotency)
# Even if the value doesn't change, the TTL must be wiped.
$ {client_executable-abs_path} set idem_key "myvalue"
(str) OK
$ {client_executable-abs_path} pexpire idem_key 5000
(int) 1
$ {client_executable-abs_path} pttl idem_key
(int) 5000
# Set to IDENTICAL value
$ {client_executable-abs_path} set idem_key "myvalue"
(str) OK
# TTL should be gone (-1)
$ {client_executable-abs_path} pttl idem_key
(int) -1
$ {client_executable-abs_path} del idem_key
(int) 1

# Test: MSET overwrites mixed types
# Create a String and a ZSet
$ {client_executable-abs_path} set str_key "orig"
(str) OK
$ {client_executable-abs_path} zadd zset_key 10 m1
(int) 1
# Clobber BOTH with MSET
$ {client_executable-abs_path} mset str_key "new_str" zset_key "now_str"
(str) OK
$ {client_executable-abs_path} get str_key
(str) new_str
$ {client_executable-abs_path} get zset_key
(str) now_str
$ {client_executable-abs_path} mdel str_key zset_key
(int) 2

# Test: MSET with duplicate keys in command
# "mset k1 v1 k1 v2" -> k1 should be v2
$ {client_executable-abs_path} mset dupe_key val1 dupe_key val2
(str) OK
$ {client_executable-abs_path} get dupe_key
(str) val2
$ {client_executable-abs_path} del dupe_key
(int) 1

# Test: Set Empty String vs Null
$ {client_executable-abs_path} set empty_str ""
(str) OK
$ {client_executable-abs_path} get empty_str
(str) 
$ {client_executable-abs_path} set null_like_str "NULL"
(str) OK
$ {client_executable-abs_path} get null_like_str
(str) NULL
$ {client_executable-abs_path} mdel empty_str null_like_str
(int) 2
# Test: SET over a key with a TTL, then let the old TTL pass
# The stale expiry record used to make the server spin forever
$ {client_executable-abs_path} set hang_key v1
(str) OK
$ {client_executable-abs_path} pexpire hang_key 100
(int) 1
$ {client_executable-abs_path} set hang_key v2
(str) OK
$ sleep 0.3
$ {client_executable-abs_path} get hang_key
(str) v2
$ {client_executable-abs_path} pexpire hang_key 100
(int) 1
$ sleep 0.3
$ {client_executable-abs_path} get hang_key
(nil)

# Test: DEL with several keys
$ {client_executable-abs_path} mset del_m1 a del_m2 b
(str) OK
$ {client_executable-abs_path} del del_m1 del_missing del_m2
(int) 2
$ {client_executable-abs_path} exists del_m1 del_m2
(int) 0
$ {client_executable-abs_path} set del_dup v
(str) OK
$ {client_executable-abs_path} del del_dup del_dup
(int) 1

# Test: malformed integer arguments
$ {client_executable-abs_path} set int_key 10
(str) OK
$ {client_executable-abs_path} pexpire int_key ""
(err) 4 value is not an integer or out of range
$ {client_executable-abs_path} pexpire int_key " 5"
(err) 4 value is not an integer or out of range
$ {client_executable-abs_path} incrby int_key +5
(err) 4 value is not an integer or out of range
$ {client_executable-abs_path} incrby int_key 5x
(err) 4 value is not an integer or out of range
$ {client_executable-abs_path} incrby int_key 99999999999999999999
(err) 4 value is not an integer or out of range
$ {client_executable-abs_path} decrby int_key -9223372036854775808
(err) 4 decrement would overflow
$ {client_executable-abs_path} get int_key
(str) 10
$ {client_executable-abs_path} set int_key 0
(str) OK
$ {client_executable-abs_path} incrby int_key -9223372036854775808
(int) -9223372036854775808
$ {client_executable-abs_path} del int_key
(int) 1

# Test: PEXPIRE with a non-positive TTL deletes the key
$ {client_executable-abs_path} set pexp_neg v
(str) OK
$ {client_executable-abs_path} pexpire pexp_neg -1
(int) 1
$ {client_executable-abs_path} get pexp_neg
(nil)
$ {client_executable-abs_path} set pexp_zero v
(str) OK
$ {client_executable-abs_path} pexpire pexp_zero 0
(int) 1
$ {client_executable-abs_path} exists pexp_zero
(int) 0
$ {client_executable-abs_path} pexpire pexp_neg -1
(int) 0
$ {client_executable-abs_path} set pexp_huge v
(str) OK
$ {client_executable-abs_path} pexpire pexp_huge 9223372036854775807
(err) 4 invalid expire time in 'pexpire' command
$ {client_executable-abs_path} del pexp_huge
(int) 1

# Test: EXPIRE and TTL (seconds, TTL rounds to nearest)
$ {client_executable-abs_path} set exp_key v
(str) OK
$ {client_executable-abs_path} ttl exp_key
(int) -1
$ {client_executable-abs_path} expire exp_key 100
(int) 1
$ {client_executable-abs_path} ttl exp_key
(int) 100
$ {client_executable-abs_path} pexpire exp_key 1600
(int) 1
$ {client_executable-abs_path} ttl exp_key
(int) 2
$ {client_executable-abs_path} pexpire exp_key 1400
(int) 1
$ {client_executable-abs_path} ttl exp_key
(int) 1
$ {client_executable-abs_path} ttl exp_missing
(int) -2
$ {client_executable-abs_path} expire exp_missing 10
(int) 0
$ {client_executable-abs_path} expire exp_key 9223372036854775
(err) 4 invalid expire time in 'expire' command
$ {client_executable-abs_path} expire exp_key 0
(int) 1
$ {client_executable-abs_path} exists exp_key
(int) 0

# Test: EXPIRE then INCR keeps the TTL
$ {client_executable-abs_path} incr exp_counter
(int) 1
$ {client_executable-abs_path} expire exp_counter 3600
(int) 1
$ {client_executable-abs_path} incr exp_counter
(int) 2
$ {client_executable-abs_path} ttl exp_counter
(int) 3600
$ {client_executable-abs_path} del exp_counter
(int) 1

# Test: SET with EX and PX
$ {client_executable-abs_path} set setex_key v EX 100
(str) OK
$ {client_executable-abs_path} get setex_key
(str) v
$ {client_executable-abs_path} ttl setex_key
(int) 100
$ {client_executable-abs_path} set setex_key v2 px 5000
(str) OK
$ {client_executable-abs_path} get setex_key
(str) v2
$ {client_executable-abs_path} ttl setex_key
(int) 5
$ {client_executable-abs_path} set setex_key v3
(str) OK
$ {client_executable-abs_path} ttl setex_key
(int) -1
$ {client_executable-abs_path} set setex_key bad EX 0
(err) 4 invalid expire time in 'set' command
$ {client_executable-abs_path} set setex_key bad EX -1
(err) 4 invalid expire time in 'set' command
$ {client_executable-abs_path} set setex_key bad EX abc
(err) 4 value is not an integer or out of range
$ {client_executable-abs_path} set setex_key bad EX
(err) 4 syntax error
$ {client_executable-abs_path} set setex_key bad EX 10 PX 100
(err) 4 syntax error
$ {client_executable-abs_path} set setex_key bad NX
(err) 4 syntax error
$ {client_executable-abs_path} get setex_key
(str) v3
$ {client_executable-abs_path} ttl setex_key
(int) -1
$ {client_executable-abs_path} set setex_short v PX 100
(str) OK
$ sleep 0.3
$ {client_executable-abs_path} get setex_short
(nil)
$ {client_executable-abs_path} del setex_key
(int) 1

# Test: SETEX and PSETEX
$ {client_executable-abs_path} setex setex_cmd 1800 v
(str) OK
$ {client_executable-abs_path} get setex_cmd
(str) v
$ {client_executable-abs_path} ttl setex_cmd
(int) 1800
$ {client_executable-abs_path} psetex setex_cmd 5000 w
(str) OK
$ {client_executable-abs_path} get setex_cmd
(str) w
$ {client_executable-abs_path} ttl setex_cmd
(int) 5
$ {client_executable-abs_path} setex setex_cmd 0 x
(err) 4 invalid expire time in 'setex' command
$ {client_executable-abs_path} psetex setex_cmd -5 x
(err) 4 invalid expire time in 'psetex' command
$ {client_executable-abs_path} get setex_cmd
(str) w
$ {client_executable-abs_path} del setex_cmd
(int) 1
# Test: list push and pop
$ {client_executable-abs_path} rpush lst a b
(int) 2
$ {client_executable-abs_path} lpush lst y z
(int) 4
$ {client_executable-abs_path} llen lst
(int) 4
$ {client_executable-abs_path} lpop lst
(str) z
$ {client_executable-abs_path} rpop lst
(str) b
$ {client_executable-abs_path} rpush lst c d
(int) 4
$ {client_executable-abs_path} lpop lst 2
(arr) len=2
(str) y
(str) a
(arr) end
$ {client_executable-abs_path} rpop lst 0
(arr) len=0
(arr) end
$ {client_executable-abs_path} rpop lst 10
(arr) len=2
(str) d
(str) c
(arr) end
$ {client_executable-abs_path} exists lst
(int) 0
$ {client_executable-abs_path} llen lst
(int) 0
$ {client_executable-abs_path} lpop lst
(nil)
$ {client_executable-abs_path} rpop lst 3
(nil)

# Test: list argument and type errors
$ {client_executable-abs_path} rpush lst_err a
(int) 1
$ {client_executable-abs_path} lpop lst_err -1
(err) 4 value is out of range, must be positive
$ {client_executable-abs_path} set lst_str v
(str) OK
$ {client_executable-abs_path} rpush lst_str a
(err) 3 WRONGTYPE Operation against a key holding the wrong kind of value
$ {client_executable-abs_path} lpop lst_str
(err) 3 WRONGTYPE Operation against a key holding the wrong kind of value
$ {client_executable-abs_path} llen lst_str
(err) 3 WRONGTYPE Operation against a key holding the wrong kind of value
$ {client_executable-abs_path} get lst_err
(err) 3 WRONGTYPE Operation against a key holding the wrong kind of value
$ {client_executable-abs_path} set lst_err v
(str) OK
$ {client_executable-abs_path} get lst_err
(str) v
$ {client_executable-abs_path} del lst_err lst_str
(int) 2

# Test: lists expire
$ {client_executable-abs_path} rpush lst_exp a b
(int) 2
$ {client_executable-abs_path} pexpire lst_exp 100
(int) 1
$ sleep 0.3
$ {client_executable-abs_path} llen lst_exp
(int) 0
$ {client_executable-abs_path} lpop lst_exp
(nil)
# Test: LRANGE
$ {client_executable-abs_path} rpush lr c d
(int) 2
$ {client_executable-abs_path} lpush lr b a
(int) 4
$ {client_executable-abs_path} rpush lr e
(int) 5
$ {client_executable-abs_path} lrange lr 0 -1
(arr) len=5
(str) a
(str) b
(str) c
(str) d
(str) e
(arr) end
$ {client_executable-abs_path} lrange lr 1 2
(arr) len=2
(str) b
(str) c
(arr) end
$ {client_executable-abs_path} lrange lr -2 100
(arr) len=2
(str) d
(str) e
(arr) end
$ {client_executable-abs_path} lrange lr -100 0
(arr) len=1
(str) a
(arr) end
$ {client_executable-abs_path} lrange lr 3 1
(arr) len=0
(arr) end
$ {client_executable-abs_path} lrange lr 5 10
(arr) len=0
(arr) end
$ {client_executable-abs_path} lrange lr_missing 0 -1
(arr) len=0
(arr) end
$ {client_executable-abs_path} lrange lr x 1
(err) 4 value is not an integer or out of range
$ {client_executable-abs_path} llen lr
(int) 5
$ {client_executable-abs_path} set lr_str v
(str) OK
$ {client_executable-abs_path} lrange lr_str 0 -1
(err) 3 WRONGTYPE Operation against a key holding the wrong kind of value
$ {client_executable-abs_path} del lr lr_str
(int) 2
# Test: LREM
$ {client_executable-abs_path} rpush lrem a b a c a b a
(int) 7
$ {client_executable-abs_path} lrem lrem 2 a
(int) 2
$ {client_executable-abs_path} lrange lrem 0 -1
(arr) len=5
(str) b
(str) c
(str) a
(str) b
(str) a
(arr) end
$ {client_executable-abs_path} lrem lrem -1 b
(int) 1
$ {client_executable-abs_path} lrange lrem 0 -1
(arr) len=4
(str) b
(str) c
(str) a
(str) a
(arr) end
$ {client_executable-abs_path} lrem lrem 0 a
(int) 2
$ {client_executable-abs_path} lrem lrem 0 z
(int) 0
$ {client_executable-abs_path} lrem lrem x a
(err) 4 value is not an integer or out of range
$ {client_executable-abs_path} lrem lrem 0 b
(int) 1
$ {client_executable-abs_path} lrem lrem 0 c
(int) 1
$ {client_executable-abs_path} exists lrem
(int) 0
$ {client_executable-abs_path} lrem lrem 0 a
(int) 0
# Test: MGET reads other types as missing
$ {client_executable-abs_path} set mg_s v
(str) OK
$ {client_executable-abs_path} hset mg_h f v
(int) 1
$ {client_executable-abs_path} rpush mg_l a
(int) 1
$ {client_executable-abs_path} zadd mg_z 1 m
(int) 1
$ {client_executable-abs_path} mget mg_s mg_h mg_l mg_z mg_none
(arr) len=5
(str) v
(nil)
(nil)
(nil)
(nil)
(arr) end
$ {client_executable-abs_path} del mg_s mg_h mg_l mg_z
(int) 4
# Test: KEYS glob character sets and escapes
$ {client_executable-abs_path} mset glob:hello v glob:hallo v glob:hxllo v glob:a*c v
(str) OK
$ {client_executable-abs_path} keys glob:h[ae]llo
(arr) len=2
(str) glob:hello
(str) glob:hallo
(arr) end
$ {client_executable-abs_path} keys glob:h[^e]llo
(arr) len=2
(str) glob:hxllo
(str) glob:hallo
(arr) end
$ {client_executable-abs_path} keys 'glob:a\*c'
(arr) len=1
(str) glob:a*c
(arr) end
$ {client_executable-abs_path} del glob:hello glob:hallo glob:hxllo glob:a*c
(int) 4
# Test: SCAN (unique prefixes and a big COUNT make it one call)
$ {client_executable-abs_path} mset scanone:a v scanone:b v scanone_x v
(str) OK
$ {client_executable-abs_path} rpush scanone:list a
(int) 1
$ {client_executable-abs_path} scan 0 match scanone:? count 100000
(arr) len=2
(str) 0
(arr) len=2
(str) scanone:b
(str) scanone:a
(arr) end
(arr) end
$ {client_executable-abs_path} scan 0 match scanone* type list count 100000
(arr) len=2
(str) 0
(arr) len=1
(str) scanone:list
(arr) end
(arr) end
$ {client_executable-abs_path} scan 0 match nothing_matches_this* count 100000
(arr) len=2
(str) 0
(arr) len=0
(arr) end
(arr) end
$ {client_executable-abs_path} scan x
(err) 4 invalid cursor
$ {client_executable-abs_path} scan -1
(err) 4 invalid cursor
$ {client_executable-abs_path} scan 0 count 0
(err) 4 syntax error
$ {client_executable-abs_path} scan 0 count x
(err) 4 value is not an integer or out of range
$ {client_executable-abs_path} scan 0 match
(err) 4 syntax error
$ {client_executable-abs_path} scan 0 type set
(err) 4 unknown type name 'set'
$ {client_executable-abs_path} del scanone:a scanone:b scanone_x scanone:list
(int) 4
# Test: BLPOP/BRPOP that do not have to wait, and timeouts
$ {client_executable-abs_path} rpush bp_list a b c
(int) 3
$ {client_executable-abs_path} blpop bp_missing bp_list 1
(arr) len=2
(str) bp_list
(str) a
(arr) end
$ {client_executable-abs_path} brpop bp_list 1
(arr) len=2
(str) bp_list
(str) c
(arr) end
$ {client_executable-abs_path} blpop bp_missing 0.1
(nil)
$ {client_executable-abs_path} blpop bp_missing x
(err) 4 timeout is not a float or out of range
$ {client_executable-abs_path} blpop bp_missing -1
(err) 4 timeout is negative
$ {client_executable-abs_path} blpop bp_missing inf
(err) 4 timeout is out of range
$ {client_executable-abs_path} blpop bp_missing
(err) 4 wrong number of arguments for 'blpop' command
$ {client_executable-abs_path} set bp_str v
(str) OK
$ {client_executable-abs_path} blpop bp_str 1
(err) 3 WRONGTYPE Operation against a key holding the wrong kind of value
$ {client_executable-abs_path} blpop bp_list bp_str 1
(arr) len=2
(str) bp_list
(str) b
(arr) end
$ {client_executable-abs_path} exists bp_list
(int) 0
$ {client_executable-abs_path} del bp_str
(int) 1
# Test: more than 1024 arguments in the binary protocol
$ {client_executable-abs_path} del k0 k1 k2 k3 k4 k5 k6 k7 k8 k9 k10 k11 k12 k13 k14 k15 k16 k17 k18 k19 k20 k21 k22 k23 k24 k25 k26 k27 k28 k29 k30 k31 k32 k33 k34 k35 k36 k37 k38 k39 k40 k41 k42 k43 k44 k45 k46 k47 k48 k49 k50 k51 k52 k53 k54 k55 k56 k57 k58 k59 k60 k61 k62 k63 k64 k65 k66 k67 k68 k69 k70 k71 k72 k73 k74 k75 k76 k77 k78 k79 k80 k81 k82 k83 k84 k85 k86 k87 k88 k89 k90 k91 k92 k93 k94 k95 k96 k97 k98 k99 k100 k101 k102 k103 k104 k105 k106 k107 k108 k109 k110 k111 k112 k113 k114 k115 k116 k117 k118 k119 k120 k121 k122 k123 k124 k125 k126 k127 k128 k129 k130 k131 k132 k133 k134 k135 k136 k137 k138 k139 k140 k141 k142 k143 k144 k145 k146 k147 k148 k149 k150 k151 k152 k153 k154 k155 k156 k157 k158 k159 k160 k161 k162 k163 k164 k165 k166 k167 k168 k169 k170 k171 k172 k173 k174 k175 k176 k177 k178 k179 k180 k181 k182 k183 k184 k185 k186 k187 k188 k189 k190 k191 k192 k193 k194 k195 k196 k197 k198 k199 k200 k201 k202 k203 k204 k205 k206 k207 k208 k209 k210 k211 k212 k213 k214 k215 k216 k217 k218 k219 k220 k221 k222 k223 k224 k225 k226 k227 k228 k229 k230 k231 k232 k233 k234 k235 k236 k237 k238 k239 k240 k241 k242 k243 k244 k245 k246 k247 k248 k249 k250 k251 k252 k253 k254 k255 k256 k257 k258 k259 k260 k261 k262 k263 k264 k265 k266 k267 k268 k269 k270 k271 k272 k273 k274 k275 k276 k277 k278 k279 k280 k281 k282 k283 k284 k285 k286 k287 k288 k289 k290 k291 k292 k293 k294 k295 k296 k297 k298 k299 k300 k301 k302 k303 k304 k305 k306 k307 k308 k309 k310 k311 k312 k313 k314 k315 k316 k317 k318 k319 k320 k321 k322 k323 k324 k325 k326 k327 k328 k329 k330 k331 k332 k333 k334 k335 k336 k337 k338 k339 k340 k341 k342 k343 k344 k345 k346 k347 k348 k349 k350 k351 k352 k353 k354 k355 k356 k357 k358 k359 k360 k361 k362 k363 k364 k365 k366 k367 k368 k369 k370 k371 k372 k373 k374 k375 k376 k377 k378 k379 k380 k381 k382 k383 k384 k385 k386 k387 k388 k389 k390 k391 k392 k393 k394 k395 k396 k397 k398 k399 k400 k401 k402 k403 k404 k405 k406 k407 k408 k409 k410 k411 k412 k413 k414 k415 k416 k417 k418 k419 k420 k421 k422 k423 k424 k425 k426 k427 k428 k429 k430 k431 k432 k433 k434 k435 k436 k437 k438 k439 k440 k441 k442 k443 k444 k445 k446 k447 k448 k449 k450 k451 k452 k453 k454 k455 k456 k457 k458 k459 k460 k461 k462 k463 k464 k465 k466 k467 k468 k469 k470 k471 k472 k473 k474 k475 k476 k477 k478 k479 k480 k481 k482 k483 k484 k485 k486 k487 k488 k489 k490 k491 k492 k493 k494 k495 k496 k497 k498 k499 k500 k501 k502 k503 k504 k505 k506 k507 k508 k509 k510 k511 k512 k513 k514 k515 k516 k517 k518 k519 k520 k521 k522 k523 k524 k525 k526 k527 k528 k529 k530 k531 k532 k533 k534 k535 k536 k537 k538 k539 k540 k541 k542 k543 k544 k545 k546 k547 k548 k549 k550 k551 k552 k553 k554 k555 k556 k557 k558 k559 k560 k561 k562 k563 k564 k565 k566 k567 k568 k569 k570 k571 k572 k573 k574 k575 k576 k577 k578 k579 k580 k581 k582 k583 k584 k585 k586 k587 k588 k589 k590 k591 k592 k593 k594 k595 k596 k597 k598 k599 k600 k601 k602 k603 k604 k605 k606 k607 k608 k609 k610 k611 k612 k613 k614 k615 k616 k617 k618 k619 k620 k621 k622 k623 k624 k625 k626 k627 k628 k629 k630 k631 k632 k633 k634 k635 k636 k637 k638 k639 k640 k641 k642 k643 k644 k645 k646 k647 k648 k649 k650 k651 k652 k653 k654 k655 k656 k657 k658 k659 k660 k661 k662 k663 k664 k665 k666 k667 k668 k669 k670 k671 k672 k673 k674 k675 k676 k677 k678 k679 k680 k681 k682 k683 k684 k685 k686 k687 k688 k689 k690 k691 k692 k693 k694 k695 k696 k697 k698 k699 k700 k701 k702 k703 k704 k705 k706 k707 k708 k709 k710 k711 k712 k713 k714 k715 k716 k717 k718 k719 k720 k721 k722 k723 k724 k725 k726 k727 k728 k729 k730 k731 k732 k733 k734 k735 k736 k737 k738 k739 k740 k741 k742 k743 k744 k745 k746 k747 k748 k749 k750 k751 k752 k753 k754 k755 k756 k757 k758 k759 k760 k761 k762 k763 k764 k765 k766 k767 k768 k769 k770 k771 k772 k773 k774 k775 k776 k777 k778 k779 k780 k781 k782 k783 k784 k785 k786 k787 k788 k789 k790 k791 k792 k793 k794 k795 k796 k797 k798 k799 k800 k801 k802 k803 k804 k805 k806 k807 k808 k809 k810 k811 k812 k813 k814 k815 k816 k817 k818 k819 k820 k821 k822 k823 k824 k825 k826 k827 k828 k829 k830 k831 k832 k833 k834 k835 k836 k837 k838 k839 k840 k841 k842 k843 k844 k845 k846 k847 k848 k849 k850 k851 k852 k853 k854 k855 k856 k857 k858 k859 k860 k861 k862 k863 k864 k865 k866 k867 k868 k869 k870 k871 k872 k873 k874 k875 k876 k877 k878 k879 k880 k881 k882 k883 k884 k885 k886 k887 k888 k889 k890 k891 k892 k893 k894 k895 k896 k897 k898 k899 k900 k901 k902 k903 k904 k905 k906 k907 k908 k909 k910 k911 k912 k913 k914 k915 k916 k917 k918 k919 k920 k921 k922 k923 k924 k925 k926 k927 k928 k929 k930 k931 k932 k933 k934 k935 k936 k937 k938 k939 k940 k941 k942 k943 k944 k945 k946 k947 k948 k949 k950 k951 k952 k953 k954 k955 k956 k957 k958 k959 k960 k961 k962 k963 k964 k965 k966 k967 k968 k969 k970 k971 k972 k973 k974 k975 k976 k977 k978 k979 k980 k981 k982 k983 k984 k985 k986 k987 k988 k989 k990 k991 k992 k993 k994 k995 k996 k997 k998 k999 k1000 k1001 k1002 k1003 k1004 k1005 k1006 k1007 k1008 k1009 k1010 k1011 k1012 k1013 k1014 k1015 k1016 k1017 k1018 k1019 k1020 k1021 k1022 k1023 k1024
(err) 5 Too many arguments.
$ {client_executable-abs_path} ping
(str) PONG
# Test: an empty zset member comes back empty, not nil
$ {client_executable-abs_path} zadd zempty 1 ""
(int) 1
$ {client_executable-abs_path} zquery zempty 0 "" 0 10
(arr) len=2
(str) 
(dbl) 1
(arr) end
$ {client_executable-abs_path} del zempty
(int) 1

# HSET takes several field/value pairs, HMSET is its older form
$ {client_executable-abs_path} hset many a 1 b 2 c 3
(int) 3
$ {client_executable-abs_path} hset many a 1 b 20 d 4
(int) 1
$ {client_executable-abs_path} hget many b
(str) 20
$ {client_executable-abs_path} hset many f 1 f 2
(int) 1
$ {client_executable-abs_path} hget many f
(str) 2
$ {client_executable-abs_path} hset many a
(err) 4 wrong number of arguments for 'hset' command
$ {client_executable-abs_path} hset many a 1 b
(err) 4 wrong number of arguments for 'hset' command
$ {client_executable-abs_path} hset many
(err) 4 wrong number of arguments for 'hset' command
$ {client_executable-abs_path} hmset many e 5 g 6
(str) OK
$ {client_executable-abs_path} hget many g
(str) 6
$ {client_executable-abs_path} hmset many e
(err) 4 wrong number of arguments for 'hmset' command
$ {client_executable-abs_path} del many
(int) 1

# MGET looks keys up 16 at a time: 17 keys, some missing, some twice
$ {client_executable-abs_path} mset g1 v1 g2 v2 g4 v4 g5 v5 g7 v7 g8 v8
(str) OK
$ {client_executable-abs_path} mget g0 g1 g2 g3 g4 g5 g6 g7 g8 g0 g1 g2 g3 g4 g5 g6 g7
(arr) len=17
(nil)
(str) v1
(str) v2
(nil)
(str) v4
(str) v5
(nil)
(str) v7
(str) v8
(nil)
(str) v1
(str) v2
(nil)
(str) v4
(str) v5
(nil)
(str) v7
(arr) end
$ {client_executable-abs_path} del g1 g2 g4 g5 g7 g8
(int) 6

# A small value lives in its entry, a long one apart; overwrites move it
$ {client_executable-abs_path} set sizes v
(str) OK
$ {client_executable-abs_path} set sizes 0123456789012345678901234567890123456789012345678901234567890123456789
(str) OK
$ {client_executable-abs_path} get sizes
(str) 0123456789012345678901234567890123456789012345678901234567890123456789
$ {client_executable-abs_path} set sizes w
(str) OK
$ {client_executable-abs_path} get sizes
(str) w
$ {client_executable-abs_path} del sizes
(int) 1

# HELLO picks RESP2 or RESP3, it means nothing to the binary protocol
$ {client_executable-abs_path} hello 3
(err) 1 Unknown cmd
$ {client_executable-abs_path} hello
(err) 1 Unknown cmd
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
    # Only client commands, not "sleep"
    if MINIS_PORT and command_parts[0] == client_executable_abs_path:
        command_parts[1:1] = ['-p', MINIS_PORT]

    # *** NEW DEBUGGING OUTPUT ***
    if i == 0:
        print(f"  Debug: Command String (Test 1): {invariant_cmd}") # Show the full string after replacement
        print(f"  Debug: Command Parts (Test 1): {command_parts}")
    # ****************************

    try:
        out = subprocess.check_output(command_parts, stderr=subprocess.STDOUT, timeout=10).decode('utf-8')
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
    except subprocess.TimeoutExpired:
        failed += 1
        print(f"  Test {i+1}/{len(cmds)}: ERROR (No reply within 10s, server hung?)")
        print(f"  Command: {invariant_cmd}")
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
