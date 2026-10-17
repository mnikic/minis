#!/usr/bin/env python3
"""
RESP tests driven by redis-py, the client leadgrep uses.

Starts its own server on a free port in a temporary directory.
MINIS_BIN selects the binary (default: build/bin/release/minis).

    python3 test/test_resp.py
"""

import collections
import os
import random
import re
import signal
import socket
import struct
import subprocess
import sys
import tempfile
import threading
import time
import unittest

import redis

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
MINIS_BIN = os.path.abspath(
    os.environ.get(
        "MINIS_BIN", os.path.join(SCRIPT_DIR, "..", "build/bin/release/minis")
    )
)


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class MinisServer:
    def __init__(self, extra_args=()):
        self.workdir = tempfile.TemporaryDirectory(prefix="minis-test-")
        self.port = free_port()
        self.extra_args = list(extra_args)
        self.log = open(os.path.join(self.workdir.name, "server.log"), "w")
        self.start()

    def start(self):
        self.proc = subprocess.Popen(
            [MINIS_BIN, "-p", str(self.port)] + self.extra_args,
            cwd=self.workdir.name,
            stdout=self.log,
            stderr=subprocess.STDOUT,
        )
        deadline = time.time() + 5
        while True:
            try:
                socket.create_connection(("127.0.0.1", self.port), 0.1).close()
                return
            except OSError:
                if self.proc.poll() is not None or time.time() > deadline:
                    self.stop()
                    raise RuntimeError(f"minis did not start ({MINIS_BIN})")
                time.sleep(0.05)

    def shutdown(self):
        # SIGTERM makes the server save a snapshot before it exits
        if self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(5)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait()

    def restart(self):
        """Stops and starts the server in the same directory, so it
        reloads its snapshot."""
        self.shutdown()
        self.start()

    def stop(self):
        self.shutdown()
        self.log.close()
        self.workdir.cleanup()


class MinisTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server = MinisServer()

    @classmethod
    def tearDownClass(cls):
        cls.server.stop()

    def setUp(self):
        # A short timeout turns a hung server into a failure
        self.r = redis.Redis(
            port=self.server.port, decode_responses=True, socket_timeout=2
        )

    def tearDown(self):
        self.r.close()

    def assertAlive(self):
        self.assertTrue(self.r.ping())
        self.assertIsNone(self.server.proc.poll(), "server exited")


class TestSetClearsTtl(MinisTestCase):
    """SET on a key with a TTL drops the TTL, and expiry must not hang."""

    def check_overwrite(self, key, new_value):
        self.assertTrue(self.r.set(key, "value"))
        self.assertEqual(self.r.execute_command("PEXPIRE", key, 100), 1)
        self.assertTrue(self.r.set(key, new_value))
        self.assertEqual(self.r.execute_command("PTTL", key), -1)

        # Let the old expiry time pass so the server's eviction runs
        time.sleep(0.3)

        self.assertAlive()
        self.assertEqual(self.r.get(key), new_value)
        self.assertEqual(self.r.execute_command("PTTL", key), -1)

    def test_new_value(self):
        self.check_overwrite("set_ttl_new", "other")

    def test_same_value(self):
        self.check_overwrite("set_ttl_same", "value")

    def test_ttl_after_overwrite_expires(self):
        key = "set_ttl_again"
        self.check_overwrite(key, "other")

        self.assertEqual(self.r.execute_command("PEXPIRE", key, 100), 1)
        time.sleep(0.3)

        self.assertAlive()
        self.assertIsNone(self.r.get(key))
        self.assertEqual(self.r.execute_command("PTTL", key), -2)


class TestPexpire(MinisTestCase):
    def pexpire(self, key, ms):
        return self.r.execute_command("PEXPIRE", key, ms)

    def test_sets_ttl(self):
        self.r.set("pexp_ttl", "v")
        self.assertEqual(self.pexpire("pexp_ttl", 10000), 1)
        pttl = self.r.execute_command("PTTL", "pexp_ttl")
        self.assertTrue(9000 < pttl <= 10000, pttl)

    def test_missing_key(self):
        self.assertEqual(self.pexpire("pexp_missing", 1000), 0)
        self.assertEqual(self.pexpire("pexp_missing", 0), 0)

    def test_zero_deletes(self):
        self.r.set("pexp_zero", "v")
        self.assertEqual(self.pexpire("pexp_zero", 0), 1)
        self.assertIsNone(self.r.get("pexp_zero"))
        self.assertEqual(self.r.exists("pexp_zero"), 0)

    def test_negative_deletes(self):
        self.r.hset("pexp_neg", "f", "v")
        self.assertEqual(self.pexpire("pexp_neg", -5), 1)
        self.assertEqual(self.r.hgetall("pexp_neg"), {})
        self.assertEqual(self.r.execute_command("PTTL", "pexp_neg"), -2)

    def test_delete_drops_pending_expiry(self):
        self.r.set("pexp_pending", "v")
        self.pexpire("pexp_pending", 100)
        self.assertEqual(self.pexpire("pexp_pending", -1), 1)
        time.sleep(0.3)
        self.assertAlive()

    def test_huge_ttl_rejected(self):
        self.r.set("pexp_huge", "v")
        with self.assertRaisesRegex(redis.ResponseError, "invalid expire"):
            self.pexpire("pexp_huge", 2**63 - 1)
        self.assertEqual(self.r.execute_command("PTTL", "pexp_huge"), -1)


class TestExpireTtl(MinisTestCase):
    def test_expire_sets_ttl_in_seconds(self):
        self.r.set("exp_secs", "v")
        self.assertTrue(self.r.expire("exp_secs", 3600))
        self.assertEqual(self.r.ttl("exp_secs"), 3600)
        pttl = self.r.pttl("exp_secs")
        self.assertTrue(3599000 < pttl <= 3600000, pttl)

    def test_ttl_rounds_to_nearest_second(self):
        self.r.set("exp_round", "v")
        self.r.pexpire("exp_round", 1600)
        self.assertEqual(self.r.ttl("exp_round"), 2)
        self.r.pexpire("exp_round", 1400)
        self.assertEqual(self.r.ttl("exp_round"), 1)

    def test_ttl_no_ttl_and_missing(self):
        self.r.set("exp_none", "v")
        self.assertEqual(self.r.ttl("exp_none"), -1)
        self.assertEqual(self.r.ttl("exp_missing"), -2)

    def test_expire_missing_key(self):
        self.assertFalse(self.r.expire("exp_missing", 10))

    def test_expire_non_positive_deletes(self):
        for ttl in (0, -1, -(2**63)):
            with self.subTest(ttl=ttl):
                self.r.set("exp_del", "v")
                self.assertTrue(self.r.expire("exp_del", ttl))
                self.assertEqual(self.r.exists("exp_del"), 0)

    def test_expire_huge_rejected(self):
        self.r.set("exp_huge", "v")
        with self.assertRaisesRegex(redis.ResponseError, "invalid expire"):
            self.r.expire("exp_huge", 2**63 // 1000)
        self.assertEqual(self.r.ttl("exp_huge"), -1)

    def test_key_expires(self):
        self.r.set("exp_gone", "v")
        self.r.expire("exp_gone", 1)
        time.sleep(1.2)
        self.assertIsNone(self.r.get("exp_gone"))
        self.assertEqual(self.r.ttl("exp_gone"), -2)

    def test_counter_window(self):
        # leadgrep's rate limit: INCR, then EXPIRE on the first hit
        key = "exp_counter"
        self.assertEqual(self.r.incr(key), 1)
        self.assertTrue(self.r.expire(key, 3600))
        self.assertEqual(self.r.incr(key), 2)
        self.assertEqual(self.r.ttl(key), 3600)

    def test_expire_on_hash(self):
        self.r.hset("exp_hash", "f", "v")
        self.assertTrue(self.r.expire("exp_hash", 100))
        self.assertEqual(self.r.ttl("exp_hash"), 100)


class TestSetWithTtl(MinisTestCase):
    def test_set_ex(self):
        # leadgrep's seen:<id> marker
        self.assertTrue(self.r.set("setex_seen", '{"a": 1}', ex=604800))
        self.assertEqual(self.r.get("setex_seen"), '{"a": 1}')
        self.assertEqual(self.r.ttl("setex_seen"), 604800)
        self.assertEqual(self.r.exists("setex_seen"), 1)

    def test_set_px(self):
        self.assertTrue(self.r.set("setex_px", "v", px=5000))
        pttl = self.r.pttl("setex_px")
        self.assertTrue(4000 < pttl <= 5000, pttl)

    def test_option_case_insensitive(self):
        self.assertTrue(self.r.execute_command("SET", "setex_case", "v", "eX", 10))
        self.assertEqual(self.r.ttl("setex_case"), 10)

    def test_set_ex_replaces_ttl(self):
        self.r.set("setex_replace", "old", ex=100)
        self.r.set("setex_replace", "new", ex=200)
        self.assertEqual(self.r.get("setex_replace"), "new")
        self.assertEqual(self.r.ttl("setex_replace"), 200)

    def test_set_ex_same_value_adds_ttl(self):
        self.r.set("setex_same", "v")
        self.r.set("setex_same", "v", ex=50)
        self.assertEqual(self.r.ttl("setex_same"), 50)

    def test_set_ex_over_other_type(self):
        self.r.hset("setex_hash", "f", "v")
        self.r.set("setex_hash", "v", ex=50)
        self.assertEqual(self.r.get("setex_hash"), "v")
        self.assertEqual(self.r.ttl("setex_hash"), 50)

    def test_plain_set_clears_ttl(self):
        self.r.set("setex_clear", "v", ex=50)
        self.r.set("setex_clear", "w")
        self.assertEqual(self.r.ttl("setex_clear"), -1)

    def test_key_expires(self):
        self.r.set("setex_gone", "v", px=100)
        time.sleep(0.3)
        self.assertAlive()
        self.assertIsNone(self.r.get("setex_gone"))

    def test_bad_options(self):
        cases = {
            ("EX", "0"): "invalid expire",
            ("EX", "-1"): "invalid expire",
            ("PX", "0"): "invalid expire",
            ("EX", str(2**63 // 1000)): "invalid expire",
            ("EX", "abc"): "not an integer",
            ("EX",): "syntax error",
            ("EX", "10", "PX", "100"): "syntax error",
            ("EX", "10", "EX", "10"): "syntax error",
            ("BOGUS",): "syntax error",
        }
        self.r.set("setex_bad", "orig")
        for opts, msg in cases.items():
            with self.subTest(opts=opts):
                with self.assertRaisesRegex(redis.ResponseError, msg):
                    self.r.execute_command("SET", "setex_bad", "new", *opts)
                self.assertEqual(self.r.get("setex_bad"), "orig")
                self.assertEqual(self.r.ttl("setex_bad"), -1)

    def test_setex(self):
        # leadgrep's draft keywords
        self.assertTrue(self.r.setex("setex_cmd", 1800, "[1, 2]"))
        self.assertEqual(self.r.get("setex_cmd"), "[1, 2]")
        self.assertEqual(self.r.ttl("setex_cmd"), 1800)

    def test_psetex(self):
        self.assertTrue(self.r.psetex("psetex_cmd", 5000, "v"))
        pttl = self.r.pttl("psetex_cmd")
        self.assertTrue(4000 < pttl <= 5000, pttl)

    def test_setex_bad_ttl(self):
        for ttl, msg in (("0", "invalid expire"), ("-5", "invalid expire"),
                         ("x", "not an integer")):
            with self.subTest(ttl=ttl):
                with self.assertRaisesRegex(redis.ResponseError, msg):
                    self.r.execute_command("SETEX", "setex_badttl", ttl, "v")
                self.assertEqual(self.r.exists("setex_badttl"), 0)


class TestListPushPop(MinisTestCase):
    def test_rpush_lpop_order(self):
        self.assertEqual(self.r.rpush("l_order", "a", "b", "c"), 3)
        self.assertEqual(self.r.rpush("l_order", "d"), 4)
        self.assertEqual(self.r.llen("l_order"), 4)
        self.assertEqual(self.r.lpop("l_order"), "a")
        self.assertEqual(self.r.rpop("l_order"), "d")
        self.assertEqual(self.r.lpop("l_order", 5), ["b", "c"])

    def test_lpush_reverses(self):
        self.assertEqual(self.r.lpush("l_rev", "a", "b", "c"), 3)
        self.assertEqual(self.r.lpop("l_rev", 3), ["c", "b", "a"])

    def test_rpop_count(self):
        self.r.rpush("l_rpopn", "a", "b", "c")
        self.assertEqual(self.r.rpop("l_rpopn", 2), ["c", "b"])
        self.assertEqual(self.r.rpop("l_rpopn", 2), ["a"])

    def test_missing_key(self):
        self.assertIsNone(self.r.lpop("l_missing"))
        self.assertIsNone(self.r.rpop("l_missing"))
        self.assertIsNone(self.r.lpop("l_missing", 3))
        self.assertEqual(self.r.llen("l_missing"), 0)

    def test_missing_key_with_count_is_null_array(self):
        # Redis sends *-1 here, not $-1
        with socket.create_connection(("127.0.0.1", self.server.port)) as sock:
            sock.sendall(b"*3\r\n$4\r\nLPOP\r\n$9\r\nl_missing\r\n$1\r\n2\r\n")
            self.assertEqual(sock.recv(100), b"*-1\r\n")

    def test_count_zero_and_negative(self):
        self.r.rpush("l_zero", "a")
        self.assertEqual(self.r.lpop("l_zero", 0), [])
        with self.assertRaisesRegex(redis.ResponseError, "must be positive"):
            self.r.lpop("l_zero", -1)
        with self.assertRaisesRegex(redis.ResponseError, "must be positive"):
            self.r.execute_command("RPOP", "l_zero", "x")
        self.assertEqual(self.r.llen("l_zero"), 1)

    def test_empty_list_is_deleted(self):
        self.r.rpush("l_empty", "a", "b")
        self.r.lpop("l_empty")
        self.r.rpop("l_empty")
        self.assertEqual(self.r.exists("l_empty"), 0)
        self.r.rpush("l_empty2", "a", "b")
        self.r.lpop("l_empty2", 10)
        self.assertEqual(self.r.exists("l_empty2"), 0)

    def test_wrong_type(self):
        self.r.set("l_str", "v")
        self.r.rpush("l_list", "a")
        for cmd in (lambda: self.r.rpush("l_str", "a"),
                    lambda: self.r.lpush("l_str", "a"),
                    lambda: self.r.lpop("l_str"),
                    lambda: self.r.rpop("l_str", 2),
                    lambda: self.r.llen("l_str"),
                    lambda: self.r.get("l_list"),
                    lambda: self.r.hset("l_list", "f", "v"),
                    lambda: self.r.incr("l_list")):
            with self.assertRaisesRegex(redis.ResponseError, "WRONGTYPE"):
                cmd()
        self.assertEqual(self.r.get("l_str"), "v")
        self.assertEqual(self.r.llen("l_list"), 1)

    def test_set_and_del_replace_list(self):
        self.r.rpush("l_set", "a")
        self.r.set("l_set", "v")
        self.assertEqual(self.r.get("l_set"), "v")
        self.r.rpush("l_del", "a")
        self.assertEqual(self.r.delete("l_del"), 1)
        self.assertEqual(self.r.exists("l_del"), 0)

    def test_list_expires(self):
        self.r.rpush("l_ttl", "a", "b")
        self.assertTrue(self.r.pexpire("l_ttl", 100))
        time.sleep(0.3)
        self.assertAlive()
        self.assertEqual(self.r.llen("l_ttl"), 0)
        self.assertIsNone(self.r.lpop("l_ttl"))

    def test_large_list(self):
        values = [f"v{i}" for i in range(5000)]
        for i in range(0, len(values), 500):
            self.r.rpush("l_large", *values[i:i + 500])
        self.assertEqual(self.r.llen("l_large"), 5000)
        popped = []
        while True:
            batch = self.r.lpop("l_large", 700)
            if batch is None:
                break
            popped += batch
        self.assertEqual(popped, values)

    def test_matches_deque_model(self):
        # Random pushes and pops at both ends wrap the ring buffer and
        # make it grow and shrink
        rng = random.Random(1234)
        model = collections.deque()
        for step in range(3000):
            op = rng.random()
            if op < 0.3:
                vals = [f"s{step}_{i}" for i in range(rng.randint(1, 20))]
                self.assertEqual(self.r.rpush("l_model", *vals),
                                 len(model) + len(vals))
                model.extend(vals)
            elif op < 0.6:
                vals = [f"s{step}_{i}" for i in range(rng.randint(1, 20))]
                self.assertEqual(self.r.lpush("l_model", *vals),
                                 len(model) + len(vals))
                model.extendleft(vals)
            elif op < 0.8:
                n = rng.randint(1, 30)
                got = self.r.lpop("l_model", n)
                want = [model.popleft() for _ in range(min(n, len(model)))]
                self.assertEqual(got, want if want or model else None)
            else:
                n = rng.randint(1, 30)
                got = self.r.rpop("l_model", n)
                want = [model.pop() for _ in range(min(n, len(model)))]
                self.assertEqual(got, want if want or model else None)
            self.assertEqual(self.r.llen("l_model"), len(model))

    def test_pop_too_big_for_reply_keeps_items(self):
        big = "x" * 100_000
        for _ in range(3):
            self.r.rpush("l_big", big)
        # 300KB does not fit in one reply
        with self.assertRaises(redis.RedisError):
            self.r.lpop("l_big", 3)
        self.r.close()
        self.assertEqual(self.r.llen("l_big"), 3)
        self.assertEqual(self.r.lpop("l_big", 2), [big, big])

    def test_leadgrep_queues(self):
        # Keyword suggestions: RPUSH a batch, LPOP what was used
        words = [f"kw{i}" for i in range(50)]
        self.assertEqual(self.r.rpush("keyword_suggestions_queue:1", *words), 50)
        self.r.lpop("keyword_suggestions_queue:1", 5)
        self.assertEqual(self.r.llen("keyword_suggestions_queue:1"), 45)
        # Retries: LPUSH, RPOP the oldest
        self.r.lpush("intentengine:ai_retries", '{"n": 1}')
        self.r.lpush("intentengine:ai_retries", '{"n": 2}')
        self.assertEqual(self.r.rpop("intentengine:ai_retries"), '{"n": 1}')


def ref_lrange(values, start, stop):
    """LRANGE as Redis defines it"""
    n = len(values)
    if start < 0:
        start = max(start + n, 0)
    if stop < 0:
        stop += n
    stop = min(stop, n - 1)
    return values[start:stop + 1] if start <= stop else []


class TestListRange(MinisTestCase):
    VALUES = [f"e{i}" for i in range(10)]

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        r = redis.Redis(port=cls.server.port, decode_responses=True)
        r.rpush("lr", *cls.VALUES)
        r.close()

    def test_all_ranges(self):
        bounds = list(range(-12, 13)) + [-(2**63), 2**63 - 1]
        for start in bounds:
            for stop in bounds:
                with self.subTest(start=start, stop=stop):
                    self.assertEqual(self.r.lrange("lr", start, stop),
                                     ref_lrange(self.VALUES, start, stop))

    def test_whole_list(self):
        self.assertEqual(self.r.lrange("lr", 0, -1), self.VALUES)

    def test_does_not_remove(self):
        # leadgrep peeks at the keyword queue this way
        self.r.lrange("lr", 0, 4)
        self.assertEqual(self.r.llen("lr"), 10)

    def test_missing_key(self):
        self.assertEqual(self.r.lrange("lr_missing", 0, -1), [])

    def test_wrong_type(self):
        self.r.set("lr_str", "v")
        with self.assertRaisesRegex(redis.ResponseError, "WRONGTYPE"):
            self.r.lrange("lr_str", 0, -1)

    def test_bad_index(self):
        for args in (("x", "1"), ("0", "1.5"), ("", "1")):
            with self.subTest(args=args):
                with self.assertRaisesRegex(redis.ResponseError,
                                            "not an integer"):
                    self.r.execute_command("LRANGE", "lr", *args)

    def test_after_pushes_at_both_ends(self):
        # Range over a wrapped ring buffer
        self.r.rpush("lr_wrap", "c", "d")
        self.r.lpush("lr_wrap", "b", "a")
        self.r.lpop("lr_wrap")
        self.r.rpush("lr_wrap", "e")
        self.assertEqual(self.r.lrange("lr_wrap", 0, -1), ["b", "c", "d", "e"])
        self.assertEqual(self.r.lrange("lr_wrap", -2, -1), ["d", "e"])

    def test_range_too_big_for_reply(self):
        big = "x" * 100_000
        for _ in range(3):
            self.r.rpush("lr_big", big)
        with self.assertRaises(redis.RedisError):
            self.r.lrange("lr_big", 0, -1)
        self.r.close()
        self.assertEqual(self.r.lrange("lr_big", 0, 0), [big])


def ref_lrem(values, count, val):
    """LREM as Redis defines it, returns (removed, new list)"""
    idx = [i for i, v in enumerate(values) if v == val]
    if count > 0:
        idx = idx[:count]
    elif count < 0:
        idx = idx[count:] if -count < len(idx) else idx
    gone = set(idx)
    return len(gone), [v for i, v in enumerate(values) if i not in gone]


class TestListRem(MinisTestCase):
    VALUES = ["a", "b", "a", "c", "a", "b", "a"]

    def fresh(self, key):
        self.r.delete(key)
        self.r.rpush(key, *self.VALUES)

    def test_counts(self):
        for count in (0, 1, 2, 3, 4, 10, -1, -2, -3, -4, -10,
                      2**63 - 1, -(2**63 - 1)):
            for val in ("a", "b", "c", "z"):
                with self.subTest(count=count, val=val):
                    self.fresh("lrem")
                    removed, rest = ref_lrem(self.VALUES, count, val)
                    self.assertEqual(self.r.lrem("lrem", count, val), removed)
                    self.assertEqual(self.r.lrange("lrem", 0, -1), rest)

    def test_removing_all_deletes_key(self):
        self.r.rpush("lrem_all", "x", "x")
        self.assertEqual(self.r.lrem("lrem_all", 0, "x"), 2)
        self.assertEqual(self.r.exists("lrem_all"), 0)

    def test_missing_key(self):
        self.assertEqual(self.r.lrem("lrem_missing", 0, "x"), 0)

    def test_wrong_type(self):
        self.r.set("lrem_str", "v")
        with self.assertRaisesRegex(redis.ResponseError, "WRONGTYPE"):
            self.r.lrem("lrem_str", 0, "v")

    def test_bad_count(self):
        self.r.rpush("lrem_bad", "x")
        with self.assertRaisesRegex(redis.ResponseError, "not an integer"):
            self.r.execute_command("LREM", "lrem_bad", "x", "x")
        # valkey refuses a count it cannot negate
        with self.assertRaisesRegex(redis.ResponseError, "out of range"):
            self.r.lrem("lrem_bad", -(2**63), "x")
        self.assertEqual(self.r.llen("lrem_bad"), 1)

    def test_on_wrapped_large_list(self):
        rng = random.Random(99)
        model = collections.deque()
        for i in range(400):
            v = rng.choice("abcd")
            if rng.random() < 0.5:
                self.r.lpush("lrem_big", v)
                model.appendleft(v)
            else:
                self.r.rpush("lrem_big", v)
                model.append(v)
        values = list(model)
        for count, val in ((5, "a"), (-7, "b"), (0, "c"), (3, "d")):
            removed, values = ref_lrem(values, count, val)
            self.assertEqual(self.r.lrem("lrem_big", count, val), removed)
            self.assertEqual(self.r.lrange("lrem_big", 0, -1), values)

    def test_leadgrep_remove_keyword(self):
        self.r.rpush("keyword_suggestions_queue:7", "x", "y", "z")
        self.assertEqual(self.r.lrem("keyword_suggestions_queue:7", 0, "y"), 1)
        self.assertEqual(self.r.lrange("keyword_suggestions_queue:7", 0, -1),
                         ["x", "z"])


class TestListPersistence(MinisTestCase):
    def test_list_survives_restart(self):
        self.r.rpush("lp_list", "a", "b", "c")
        self.r.lpush("lp_list", "z")
        self.r.rpush("lp_ttl", "x")
        self.r.expire("lp_ttl", 3600)
        self.r.close()

        self.server.restart()

        self.assertEqual(self.r.lpop("lp_list", 10), ["z", "a", "b", "c"])
        self.assertEqual(self.r.lpop("lp_ttl"), "x")

    def test_large_list_survives_restart(self):
        values = [f"value-{i}" for i in range(3000)]
        for i in range(0, len(values), 500):
            self.r.rpush("lp_large", *values[i:i + 500])
        self.r.close()

        self.server.restart()

        self.assertEqual(self.r.llen("lp_large"), 3000)
        self.assertEqual(self.r.lpop("lp_large", 3000)[::1000],
                         values[::1000])


def cpu_seconds(pid):
    """User plus system CPU time of a process"""
    with open(f"/proc/{pid}/stat") as f:
        fields = f.read().rsplit(")", 1)[1].split()
    return (int(fields[11]) + int(fields[12])) / os.sysconf("SC_CLK_TCK")


class TestIdle(MinisTestCase):
    # The server checks its snapshot rules every 10s
    SNAPSHOT_INTERVAL = 10

    def test_idle_server_sleeps(self):
        # One change, so there is something to save but no rule is met
        self.r.set("idle_key", "v")
        self.r.close()
        time.sleep(self.SNAPSHOT_INTERVAL + 1.5)

        pid = self.server.proc.pid
        before = cpu_seconds(pid)
        time.sleep(2)
        used = cpu_seconds(pid) - before
        self.assertLess(used, 0.2, f"idle server used {used:.2f}s CPU in 2s")
        self.assertAlive()


class TestShutdown(MinisTestCase):
    def test_sigterm_saves_and_exits_promptly(self):
        # A SIGTERM landing just before the event loop's wait used to be
        # missed until the wait timed out, so the harness killed the
        # server unsaved. It took a few percent of restarts to hit.
        for i in range(50):
            self.r.rpush("shutdown_list", *[f"{i}-{j}" for j in range(100)])
            self.r.close()

            start = time.time()
            self.server.shutdown()
            took = time.time() - start
            self.assertLess(took, 2, f"restart {i}: shutdown took {took:.2f}s")
            self.assertEqual(self.server.proc.returncode, 0)

            self.server.start()
            self.assertEqual(self.r.llen("shutdown_list"), 100 * (i + 1))


class TestMget(MinisTestCase):
    def test_other_types_read_as_missing(self):
        self.r.set("mget_s", "v")
        self.r.hset("mget_h", "f", "v")
        self.r.rpush("mget_l", "a")
        self.r.zadd("mget_z", {"m": 1})
        self.assertEqual(
            self.r.mget("mget_s", "mget_h", "mget_l", "mget_z", "mget_none"),
            ["v", None, None, None, None])
        # The keys themselves are untouched
        self.assertEqual(self.r.hgetall("mget_h"), {"f": "v"})
        self.assertEqual(self.r.lrange("mget_l", 0, -1), ["a"])


class TestLargeEntryPersistence(MinisTestCase):
    """Entries far bigger than one request survive a restart. They used to
    be left out of snapshots."""

    def test_large_entries_survive_restart(self):
        blob = "x" * 10_000
        # Like leadgrep_pending: one hash of many payloads, ~1MB
        for i in range(100):
            self.r.hset("big_hash", f"alert_{i}", f"{i}:{blob}")
        # A backed up queue, ~3MB
        for i in range(0, 300, 10):
            self.r.rpush("big_list", *[f"{j}:{blob}" for j in range(i, i + 10)])
        # ~400KB of zset members, ZADD takes one member at a time
        pipe = self.r.pipeline(transaction=False)
        for j in range(2000):
            pipe.zadd("big_zset", {f"{j:04d}" + "m" * 200: j})
        pipe.execute()
        self.r.set("small", "v")
        self.r.close()

        self.server.restart()

        # A 1MB HGETALL would not fit in one reply, read field by field
        pipe = self.r.pipeline(transaction=False)
        for i in range(100):
            pipe.hget("big_hash", f"alert_{i}")
        self.assertEqual(pipe.execute(), [f"{i}:{blob}" for i in range(100)])
        self.assertEqual(self.r.llen("big_list"), 300)
        self.assertEqual(self.r.lrange("big_list", 299, 299), [f"299:{blob}"])
        self.assertEqual(self.r.zscore("big_zset", "1999" + "m" * 200), 1999)
        self.assertEqual(self.r.zscore("big_zset", "0000" + "m" * 200), 0)
        self.assertEqual(self.r.get("small"), "v")


GLOB_KEYS = ['*a', '-', '?', 'Z', '[', ']', 'a', 'a*c', 'a-c', 'a?c', 'aXc', 'a\\c', 'a]c', 'a^c', 'aa', 'aaa', 'aaab', 'ab', 'abc', 'abd', 'b', 'baaa', 'c', 'hallo', 'heeeello', 'hello', 'hllo', 'hxllo', 'seen:1', 'seen:10', 'seen:123', 'xseen:1', 'z']

# Pattern -> matching GLOB_KEYS, as valkey 9.1 KEYS answers
GLOB_MATCHES = {
    '*': ['*a', '-', '?', 'Z', '[', ']', 'a', 'a*c', 'a-c', 'a?c', 'aXc', 'a\\c', 'a]c', 'a^c', 'aa', 'aaa', 'aaab', 'ab', 'abc', 'abd', 'b', 'baaa', 'c', 'hallo', 'heeeello', 'hello', 'hllo', 'hxllo', 'seen:1', 'seen:10', 'seen:123', 'xseen:1', 'z'],
    '?': ['-', '?', 'Z', '[', ']', 'a', 'b', 'c', 'z'],
    'a': ['a'],
    'a*': ['a', 'a*c', 'a-c', 'a?c', 'aXc', 'a\\c', 'a]c', 'a^c', 'aa', 'aaa', 'aaab', 'ab', 'abc', 'abd'],
    '*c': ['a*c', 'a-c', 'a?c', 'aXc', 'a\\c', 'a]c', 'a^c', 'abc', 'c'],
    'a?c': ['a*c', 'a-c', 'a?c', 'aXc', 'a\\c', 'a]c', 'a^c', 'abc'],
    'a*c': ['a*c', 'a-c', 'a?c', 'aXc', 'a\\c', 'a]c', 'a^c', 'abc'],
    'h?llo': ['hallo', 'hello', 'hxllo'],
    'h*llo': ['hallo', 'heeeello', 'hello', 'hllo', 'hxllo'],
    'h[ae]llo': ['hallo', 'hello'],
    'h[^e]llo': ['hallo', 'hxllo'],
    'h[a-b]llo': ['hallo'],
    'h[b-a]llo': ['hallo'],
    '[a-c]': ['a', 'b', 'c'],
    '[^a-c]': ['-', '?', 'Z', '[', ']', 'z'],
    '[abc]*': ['a', 'a*c', 'a-c', 'a?c', 'aXc', 'a\\c', 'a]c', 'a^c', 'aa', 'aaa', 'aaab', 'ab', 'abc', 'abd', 'b', 'baaa', 'c'],
    'a[]]c': [],
    'a[-]c': ['a-c'],
    'a[\\]]c': ['a]c'],
    'a\\*c': ['a*c'],
    'a\\?c': ['a?c'],
    'a\\\\c': ['a\\c'],
    'a[\\-]c': ['a-c'],
    '[': [],
    '[a': ['a'],
    'a[': [],
    '\\': [],
    'a\\': [],
    'seen:1[0-2]': ['seen:10'],
    'seen:1*': ['seen:1', 'seen:10', 'seen:123'],
    '*seen*': ['seen:1', 'seen:10', 'seen:123', 'xseen:1'],
    '**': ['*a', '-', '?', 'Z', '[', ']', 'a', 'a*c', 'a-c', 'a?c', 'aXc', 'a\\c', 'a]c', 'a^c', 'aa', 'aaa', 'aaab', 'ab', 'abc', 'abd', 'b', 'baaa', 'c', 'hallo', 'heeeello', 'hello', 'hllo', 'hxllo', 'seen:1', 'seen:10', 'seen:123', 'xseen:1', 'z'],
    '*a*': ['*a', 'a', 'a*c', 'a-c', 'a?c', 'aXc', 'a\\c', 'a]c', 'a^c', 'aa', 'aaa', 'aaab', 'ab', 'abc', 'abd', 'baaa', 'hallo'],
    'a*a*b': ['aaab'],
    '*?': ['*a', '-', '?', 'Z', '[', ']', 'a', 'a*c', 'a-c', 'a?c', 'aXc', 'a\\c', 'a]c', 'a^c', 'aa', 'aaa', 'aaab', 'ab', 'abc', 'abd', 'b', 'baaa', 'c', 'hallo', 'heeeello', 'hello', 'hllo', 'hxllo', 'seen:1', 'seen:10', 'seen:123', 'xseen:1', 'z'],
    '?*?': ['*a', 'a*c', 'a-c', 'a?c', 'aXc', 'a\\c', 'a]c', 'a^c', 'aa', 'aaa', 'aaab', 'ab', 'abc', 'abd', 'baaa', 'hallo', 'heeeello', 'hello', 'hllo', 'hxllo', 'seen:1', 'seen:10', 'seen:123', 'xseen:1'],
    '[*]a': ['*a'],
    '[?]': ['?'],
    'a[^]]c': [],
    '[]]': [],
    '[a-]': [']', 'a'],
    '[!a]': ['a'],
    'a**c': ['a*c', 'a-c', 'a?c', 'aXc', 'a\\c', 'a]c', 'a^c', 'abc'],
    '[z-z]': ['z'],
    '[Z]': ['Z'],
    '[^Z]': ['-', '?', '[', ']', 'a', 'b', 'c', 'z'],
}


class TestGlob(MinisTestCase):
    def test_keys_patterns_match_valkey(self):
        pipe = self.r.pipeline(transaction=False)
        for k in GLOB_KEYS:
            pipe.set(k, "v")
        pipe.execute()
        for pattern, want in GLOB_MATCHES.items():
            with self.subTest(pattern=pattern):
                self.assertEqual(sorted(self.r.keys(pattern)), want)


def scan_all(r, **kwargs):
    """Every key a full SCAN returns, with repeats, and the call count"""
    keys, cursor, calls = [], 0, 0
    while True:
        cursor, batch = r.scan(cursor, **kwargs)
        keys += batch
        calls += 1
        if cursor == 0:
            return keys, calls


class TestScan(MinisTestCase):
    def setUp(self):
        super().setUp()
        # Each test owns the whole keyspace
        self.delete_all(self.r.scan_iter(count=100))

    def delete_all(self, keys):
        # A request takes at most 1024 arguments and 200KB
        keys = list(keys)
        for i in range(0, len(keys), 100):
            self.r.delete(*keys[i:i + 100])

    def fill(self, keys, value="v"):
        pipe = self.r.pipeline(transaction=False)
        for k in keys:
            pipe.set(k, value)
        pipe.execute()

    def test_empty(self):
        self.assertEqual(self.r.scan(0), (0, []))

    def test_emptied_keyspace_finishes_at_once(self):
        # Tables keep their size after the keys go
        self.fill(f"gone:{i}" for i in range(5000))
        self.delete_all(f"gone:{i}" for i in range(5000))
        self.assertEqual(self.r.scan(0), (0, []))

    def test_full_iteration(self):
        for n in (1, 10, 1000, 20000):
            for count in (1, 10, 1000):
                if n == 20000 and count == 1:
                    continue  # too slow, covered by count 10
                with self.subTest(n=n, count=count):
                    keys = {f"full:{n}:{i}" for i in range(n)}
                    self.fill(keys)
                    got, _ = scan_all(self.r, count=count)
                    self.assertEqual(set(got), keys)
                    self.delete_all(keys)

    def test_no_repeats_without_changes(self):
        keys = {f"norep:{i}" for i in range(3000)}
        self.fill(keys)
        got, _ = scan_all(self.r, count=7)
        self.assertEqual(len(got), len(keys))

    def test_big_count_finishes_in_one_call(self):
        self.fill(f"one:{i}" for i in range(500))
        got, calls = scan_all(self.r, count=100000)
        self.assertEqual((len(got), calls), (500, 1))

    def test_match(self):
        keys = ([f"seen:{i}" for i in range(300)] +
                [f"shared_lead:{i}" for i in range(50)] + ["seen", "xseen:1"])
        self.fill(keys)
        cases = {
            "seen:*": {k for k in keys if k.startswith("seen:")},
            "seen:?": {f"seen:{i}" for i in range(10)},
            "seen:1[0-2]": {"seen:10", "seen:11", "seen:12"},
            "*lead*": {k for k in keys if "lead" in k},
            "seen": {"seen"},
            "nothing*": set(),
        }
        for pattern, want in cases.items():
            with self.subTest(pattern=pattern):
                got, _ = scan_all(self.r, match=pattern, count=10)
                self.assertEqual(set(got), want)
                self.assertEqual(set(self.r.scan_iter(match=pattern)), want)

    def test_type(self):
        self.r.set("t_str", "v")
        self.r.rpush("t_list", "a")
        self.r.hset("t_hash", "f", "v")
        self.r.zadd("t_zset", {"m": 1})
        for name, key in (("string", "t_str"), ("list", "t_list"),
                          ("hash", "t_hash"), ("zset", "t_zset"),
                          ("LIST", "t_list"), ("ZSet", "t_zset")):
            with self.subTest(name=name):
                got, _ = scan_all(self.r, _type=name)
                self.assertEqual(got, [key])
        got, _ = scan_all(self.r, match="t_*", _type="hash")
        self.assertEqual(got, ["t_hash"])

    def test_expired_keys_are_skipped(self):
        self.fill(f"live:{i}" for i in range(50))
        pipe = self.r.pipeline(transaction=False)
        for i in range(50):
            pipe.set(f"dying:{i}", "v", px=50)
        pipe.execute()
        time.sleep(0.2)
        got, _ = scan_all(self.r, count=5)
        self.assertEqual(set(got), {f"live:{i}" for i in range(50)})

    def test_keys_present_throughout_survive_changes(self):
        # The SCAN guarantee: keys there for the whole scan come back at
        # least once, while others are added (growing and resizing the
        # tables) and deleted (shifting entries back) between calls
        for seed in range(5):
            with self.subTest(seed=seed):
                rng = random.Random(seed)
                stable = {f"stable:{seed}:{i}" for i in range(rng.randint(5, 400))}
                self.fill(stable)
                churn = [f"churn:{seed}:{i}" for i in range(200)]
                self.fill(churn)
                added = 0
                got, cursor = [], 0
                while True:
                    cursor, batch = self.r.scan(cursor, count=rng.randint(1, 20))
                    got += batch
                    if cursor == 0:
                        break
                    # Fewer than the scan takes per call, or it never ends
                    # (true for Redis too), and capped so tables still
                    # resize several times mid scan
                    new = [f"new:{seed}:{added + i}"
                           for i in range(rng.randint(0, 3) if added < 3000 else 0)]
                    added += len(new)
                    self.fill(new)
                    if churn:
                        doomed = rng.sample(churn, min(len(churn), rng.randint(0, 20)))
                        self.delete_all(doomed)
                        churn = [c for c in churn if c not in doomed]
                self.assertEqual(stable - set(got), set())
                # Only keys that ever existed come back
                self.assertTrue(all(k.split(":")[0] in ("stable", "churn", "new")
                                    for k in got))
                self.delete_all(self.r.scan_iter(count=100))

    def test_growth_from_tiny_tables(self):
        # Start small, then grow every shard's table many times over in
        # the first calls of the scan, so it resumes on much bigger
        # tables (and on tables caught mid resize)
        stable = {f"tiny:{i}" for i in range(5)}
        self.fill(stable)
        got, cursor, calls = [], 0, 0
        while True:
            cursor, batch = self.r.scan(cursor, count=1)
            got += batch
            calls += 1
            if cursor == 0:
                break
            if calls <= 20:
                self.fill(f"grow:{calls}:{i}" for i in range(300))
        self.assertEqual(stable - set(got), set())

    def test_any_cursor_terminates(self):
        self.fill(f"cur:{i}" for i in range(100))
        for start in (1, 7, 12345, 2**63, 2**64 - 1):
            with self.subTest(start=start):
                cursor, calls = start, 0
                while True:
                    cursor, _ = self.r.scan(cursor, count=10)
                    calls += 1
                    if cursor == 0:
                        break
                    self.assertLess(calls, 10000)

    def test_errors(self):
        cases = [
            (["x"], "invalid cursor"),
            (["-1"], "invalid cursor"),
            (["18446744073709551616"], "invalid cursor"),
            ([""], "invalid cursor"),
            (["0", "COUNT", "0"], "syntax error"),
            (["0", "COUNT", "-1"], "syntax error"),
            (["0", "COUNT", "x"], "not an integer"),
            (["0", "MATCH"], "syntax error"),
            (["0", "BOGUS", "1"], "syntax error"),
            (["0", "TYPE", "bogus"], "unknown type name 'bogus'"),
        ]
        for args, msg in cases:
            with self.subTest(args=args):
                with self.assertRaisesRegex(redis.ResponseError, msg):
                    self.r.execute_command("SCAN", *args)
        self.assertAlive()

    def test_lenient_cursor_and_repeated_options(self):
        self.fill(["opt_a", "opt_b"])
        for start in ("00", "+0", " 0"):
            with self.subTest(cursor=start):
                cursor, keys = self.r.execute_command("SCAN", start)
                self.assertEqual(cursor, self.r.scan(0)[0])
        # The last MATCH and COUNT win
        cursor, got = 0, []
        while True:
            cursor, keys = self.r.execute_command(
                "SCAN", cursor, "MATCH", "opt_a", "MATCH", "opt_b",
                "COUNT", "1", "COUNT", "1000")
            got += keys
            if int(cursor) == 0:
                break
        self.assertEqual(got, ["opt_b"])

    def test_reply_too_big(self):
        long_keys = [f"long:{i}:" + "k" * 1000 for i in range(300)]
        self.fill(long_keys)
        # 300KB of keys do not fit in one reply
        with self.assertRaises(redis.RedisError):
            self.r.scan(0, count=100000)
        self.r.close()
        got, _ = scan_all(self.r, count=50)
        self.assertEqual(set(got), set(long_keys))
        self.delete_all(long_keys)

    def test_leadgrep_seen_posts(self):
        # yield_all_recent_posts_for_seo
        posts = {f"seen:rss_https://reddit.com/r/x/{i}": f'{{"title": "{i}"}}'
                 for i in range(2000)}
        pipe = self.r.pipeline(transaction=False)
        for k, v in posts.items():
            pipe.set(k, v, ex=604800)
        pipe.execute()
        self.r.hset("leadgrep_pending", "a", "b")
        found = {}
        for key in self.r.scan_iter(match="seen:*", count=1000):
            found[key] = self.r.get(key)
        self.assertEqual(found, posts)


class Waiter(threading.Thread):
    """A client blocked in BLPOP/BRPOP on its own connection"""

    def __init__(self, port, keys, timeout=5, right=False):
        super().__init__(daemon=True)
        self.client = redis.Redis(port=port, decode_responses=True,
                                  socket_timeout=timeout + 5)
        self.keys, self.timeout, self.right = keys, timeout, right
        self.result, self.error, self.took = None, None, None

    def run(self):
        start = time.time()
        try:
            pop = self.client.brpop if self.right else self.client.blpop
            self.result = pop(self.keys, timeout=self.timeout)
        except Exception as e:
            self.error = e
        self.took = time.time() - start
        self.client.close()

    def started(self, port):
        """Starts and returns once the server has parked the client"""
        self.start()
        # The server parks it at once, a short wait is plenty
        time.sleep(0.15)
        return self


def raw_command(*args):
    out = f"*{len(args)}\r\n".encode()
    for a in args:
        a = a.encode() if isinstance(a, str) else a
        out += f"${len(a)}\r\n".encode() + a + b"\r\n"
    return out


class TestBlockingPop(MinisTestCase):
    def waiter(self, keys, **kwargs):
        return Waiter(self.server.port, keys, **kwargs).started(self.server.port)

    def test_pops_at_once_when_data_is_there(self):
        self.r.rpush("bp_now", "a", "b")
        self.assertEqual(self.r.blpop("bp_now", timeout=1), ("bp_now", "a"))
        self.assertEqual(self.r.brpop("bp_now", timeout=1), ("bp_now", "b"))
        self.assertEqual(self.r.exists("bp_now"), 0)

    def test_first_non_empty_key_wins(self):
        self.r.rpush("bp_k2", "two")
        self.r.rpush("bp_k3", "three")
        self.assertEqual(self.r.blpop(["bp_k1", "bp_k2", "bp_k3"], timeout=1),
                         ("bp_k2", "two"))

    def test_timeout(self):
        for timeout in (0.1, 0.3, 1):
            with self.subTest(timeout=timeout):
                start = time.time()
                self.assertIsNone(self.r.blpop("bp_empty", timeout=timeout))
                took = time.time() - start
                self.assertGreaterEqual(took, timeout * 0.9)
                self.assertLess(took, timeout + 0.5)

    def test_timeout_reply_is_null_array(self):
        with socket.create_connection(("127.0.0.1", self.server.port)) as sock:
            sock.settimeout(3)
            sock.sendall(raw_command("BLPOP", "bp_empty", "0.1"))
            self.assertEqual(sock.recv(100), b"*-1\r\n")

    def test_errors(self):
        cases = [
            (["k", "x"], "timeout is not a float or out of range"),
            (["k", ""], "timeout is not a float or out of range"),
            (["k", " 1"], "timeout is not a float or out of range"),
            (["k", "nan"], "timeout is not a float or out of range"),
            (["k", "-1"], "timeout is negative"),
            (["k", "inf"], "timeout is out of range"),
            (["k", "1e400"], "timeout is out of range"),
            (["k", "9223372036854775807"], "timeout is out of range"),
            (["k"], "wrong number of arguments for 'blpop' command"),
        ]
        for args, msg in cases:
            with self.subTest(args=args):
                with self.assertRaisesRegex(redis.ResponseError, msg):
                    self.r.execute_command("BLPOP", *args)
        with self.assertRaisesRegex(redis.ResponseError, "'brpop'"):
            self.r.execute_command("BRPOP", "k")
        self.assertAlive()

    def test_wrong_type(self):
        self.r.set("bp_str", "v")
        self.r.rpush("bp_list", "a")
        with self.assertRaisesRegex(redis.ResponseError, "WRONGTYPE"):
            self.r.blpop(["bp_str", "bp_list"], timeout=1)
        with self.assertRaisesRegex(redis.ResponseError, "WRONGTYPE"):
            self.r.blpop(["bp_missing", "bp_str"], timeout=1)
        # A list ahead of it wins
        self.assertEqual(self.r.blpop(["bp_list", "bp_str"], timeout=1),
                         ("bp_list", "a"))

    def test_push_wakes_waiter(self):
        for push in ("lpush", "rpush"):
            with self.subTest(push=push):
                w = self.waiter("bp_wake")
                start = time.time()
                getattr(self.r, push)("bp_wake", "hello")
                w.join(3)
                self.assertEqual(w.result, ("bp_wake", "hello"))
                self.assertLess(time.time() - start, 0.5)
                self.assertEqual(self.r.exists("bp_wake"), 0)

    def test_brpop_takes_the_tail(self):
        w = self.waiter("bp_tail", right=True)
        self.r.rpush("bp_tail", "a", "b", "c")
        w.join(3)
        self.assertEqual(w.result, ("bp_tail", "c"))
        self.assertEqual(self.r.lrange("bp_tail", 0, -1), ["a", "b"])

    def test_waiters_are_served_oldest_first(self):
        waiters = [self.waiter("bp_fifo") for _ in range(3)]
        self.r.rpush("bp_fifo", "x", "y", "z")
        for w in waiters:
            w.join(3)
        self.assertEqual([w.result for w in waiters],
                         [("bp_fifo", "x"), ("bp_fifo", "y"), ("bp_fifo", "z")])

    def test_one_element_for_two_waiters(self):
        first = self.waiter("bp_one", timeout=1)
        second = self.waiter("bp_one", timeout=1)
        self.r.rpush("bp_one", "only")
        first.join(3)
        second.join(3)
        self.assertEqual(first.result, ("bp_one", "only"))
        self.assertIsNone(second.result)
        self.assertGreaterEqual(second.took, 0.9)

    def test_waiter_on_several_keys(self):
        w = self.waiter(["bp_m1", "bp_m2", "bp_m3"])
        self.r.rpush("bp_m3", "three")
        w.join(3)
        self.assertEqual(w.result, ("bp_m3", "three"))

    def test_waiter_gets_push_before_the_pushers_next_command(self):
        # Like Redis: the pipelined LPOP finds nothing
        w = self.waiter("bp_pipe")
        pipe = self.r.pipeline(transaction=False)
        pipe.rpush("bp_pipe", "v")
        pipe.lpop("bp_pipe")
        pipe.llen("bp_pipe")
        self.assertEqual(pipe.execute(), [1, None, 0])
        w.join(3)
        self.assertEqual(w.result, ("bp_pipe", "v"))

    def test_overwritten_key_keeps_waiting(self):
        w = self.waiter("bp_ow")
        self.r.set("bp_ow", "string now")
        # Another push makes waiters look again, it must not hand out a
        # WRONGTYPE
        self.r.rpush("bp_unrelated", "z")
        time.sleep(0.2)
        self.assertTrue(w.is_alive())
        self.r.delete("bp_ow")
        self.r.rpush("bp_ow", "late")
        w.join(3)
        self.assertIsNone(w.error)
        self.assertEqual(w.result, ("bp_ow", "late"))

    def test_expired_list_keeps_waiting(self):
        w = self.waiter("bp_exp")
        self.r.rpush("bp_exp_other", "a")
        self.r.pexpire("bp_exp_other", 50)
        time.sleep(0.2)
        self.assertTrue(w.is_alive())
        self.r.rpush("bp_exp", "v")
        w.join(3)
        self.assertEqual(w.result, ("bp_exp", "v"))

    def test_forever_waits_until_push(self):
        w = self.waiter("bp_forever", timeout=0)
        time.sleep(1.2)
        self.assertTrue(w.is_alive())
        self.r.rpush("bp_forever", "finally")
        w.join(3)
        self.assertEqual(w.result, ("bp_forever", "finally"))

    def test_disconnected_waiter_loses_nothing(self):
        sock = socket.create_connection(("127.0.0.1", self.server.port))
        sock.sendall(raw_command("BLPOP", "bp_gone", "0"))
        time.sleep(0.1)
        sock.close()
        time.sleep(0.1)
        self.r.rpush("bp_gone", "kept")
        self.assertEqual(self.r.lrange("bp_gone", 0, -1), ["kept"])
        self.assertAlive()

    def test_pipelined_commands_wait_behind_it(self):
        with socket.create_connection(("127.0.0.1", self.server.port)) as sock:
            sock.settimeout(0.3)
            sock.sendall(raw_command("BLPOP", "bp_behind", "5") +
                         raw_command("PING"))
            with self.assertRaises(socket.timeout):
                sock.recv(100)
            self.r.rpush("bp_behind", "v")
            sock.settimeout(3)
            want = b"*2\r\n$9\r\nbp_behind\r\n$1\r\nv\r\n+PONG\r\n"
            got = b""
            while len(got) < len(want):
                got += sock.recv(100)
            self.assertEqual(got, want)

    def test_more_data_while_waiting(self):
        # Requests sent after the client blocked wait too
        with socket.create_connection(("127.0.0.1", self.server.port)) as sock:
            sock.settimeout(3)
            sock.sendall(raw_command("BLPOP", "bp_more", "5"))
            time.sleep(0.1)
            sock.sendall(raw_command("PING") + raw_command("ECHO", "x"))
            time.sleep(0.1)
            self.r.rpush("bp_more", "v")
            want = b"*2\r\n$7\r\nbp_more\r\n$1\r\nv\r\n+PONG\r\n"
            got = b""
            while len(got) < len(want):
                got += sock.recv(100)
            self.assertTrue(got.startswith(want), got)

    def test_others_are_served_while_clients_wait(self):
        waiters = [self.waiter("bp_busy", timeout=10) for _ in range(20)]
        start = time.time()
        for i in range(100):
            self.r.set(f"bp_busy_key{i}", i)
        self.assertLess(time.time() - start, 1)
        self.r.rpush("bp_busy", *[str(i) for i in range(20)])
        for w in waiters:
            w.join(3)
        self.assertEqual(sorted(int(w.result[1]) for w in waiters),
                         list(range(20)))

    def test_many_waiters_each_get_one(self):
        waiters = [Waiter(self.server.port, "bp_many", timeout=10)
                   for _ in range(200)]
        for w in waiters:
            w.start()
        time.sleep(0.5)
        values = [str(i) for i in range(200)]
        for i in range(0, 200, 50):
            self.r.rpush("bp_many", *values[i:i + 50])
        for w in waiters:
            w.join(10)
        got = [w.result[1] for w in waiters if w.result]
        self.assertEqual(sorted(got, key=int), values)
        self.assertEqual(self.r.exists("bp_many"), 0)

    def test_producers_and_consumers(self):
        # Every pushed item is popped exactly once
        items = [f"item-{i}" for i in range(2000)]
        consumed, lock = [], threading.Lock()

        def consume():
            c = redis.Redis(port=self.server.port, decode_responses=True,
                            socket_timeout=10)
            while True:
                got = c.blpop(["bp_q1", "bp_q2"], timeout=1)
                if got is None:
                    break
                with lock:
                    consumed.append(got[1])
            c.close()

        def produce(chunk, key):
            c = redis.Redis(port=self.server.port, decode_responses=True)
            for i in range(0, len(chunk), 7):
                c.rpush(key, *chunk[i:i + 7])
            c.close()

        consumers = [threading.Thread(target=consume) for _ in range(4)]
        producers = [threading.Thread(target=produce, args=(items[:1000], "bp_q1")),
                     threading.Thread(target=produce, args=(items[1000:], "bp_q2"))]
        for t in consumers + producers:
            t.start()
        for t in producers + consumers:
            t.join(30)
        self.assertEqual(sorted(consumed), sorted(items))

    def test_leadgrep_ollama_worker(self):
        # push_to_seo_ollama_queue / pop_from_seo_ollama_queue
        got = []

        def worker():
            c = redis.Redis(port=self.server.port, decode_responses=True,
                            socket_timeout=10)
            while len(got) < 50:
                result = c.blpop("seo_ollama_queue", timeout=5)
                if result:
                    got.append(result[1])
            c.close()

        t = threading.Thread(target=worker)
        t.start()
        for i in range(50):
            self.r.lpush("seo_ollama_queue", f'{{"post": {i}}}')
            if i % 10 == 0:
                time.sleep(0.05)
        t.join(10)
        self.assertEqual(sorted(got), sorted(f'{{"post": {i}}}' for i in range(50)))


class TestBlockingShutdown(MinisTestCase):
    def test_sigterm_with_clients_waiting(self):
        # Raw sockets: redis-py would reconnect and wait again
        socks = []
        for _ in range(5):
            sock = socket.create_connection(("127.0.0.1", self.server.port))
            sock.settimeout(3)
            sock.sendall(raw_command("BLPOP", "bps_forever", "0"))
            socks.append(sock)
        self.r.rpush("bps_kept", "a")
        self.r.close()
        time.sleep(0.2)

        start = time.time()
        self.server.shutdown()
        self.assertLess(time.time() - start, 2)
        self.assertEqual(self.server.proc.returncode, 0)
        # Their connections were closed without a reply
        for sock in socks:
            self.assertEqual(sock.recv(100), b"")
            sock.close()

        self.server.start()
        self.assertEqual(self.r.lrange("bps_kept", 0, -1), ["a"])


def binary_command(*args):
    """A request in minis's binary protocol"""
    args = [a.encode() if isinstance(a, str) else a for a in args]
    body = struct.pack(">I", len(args))
    body += b"".join(struct.pack(">I", len(a)) + a for a in args)
    return struct.pack(">I", len(body)) + body


class TestErrorClose(MinisTestCase):
    """After an error reply the server closes, but first lets the client
    read the error even if it is still sending"""

    def test_error_survives_unread_input(self):
        with socket.create_connection(("127.0.0.1", self.server.port)) as sock:
            sock.settimeout(5)
            # An error, then more input than the server reads before it
            # handles the error, sent from a thread as it can block
            data = binary_command("del", *["k"] * 1025) + b"x" * 2_000_000
            sender = threading.Thread(
                target=lambda: sock.sendall(data), daemon=True)
            sender.start()
            # Closing with input unread would reset the connection and
            # throw away the error reply
            time.sleep(0.3)
            got = b""
            while True:
                chunk = sock.recv(4096)
                if not chunk:
                    break
                got += chunk
            self.assertIn(b"Too many arguments", got)
            sender.join(5)
        self.assertAlive()

    def test_client_that_never_closes_is_dropped(self):
        # Draining has a time limit, so such clients do not pile up
        with socket.create_connection(("127.0.0.1", self.server.port)) as sock:
            sock.settimeout(3)
            sock.sendall(binary_command("del", *["k"] * 1025))
            got = b""
            while True:
                chunk = sock.recv(4096)
                if not chunk:
                    break
                got += chunk
            self.assertIn(b"Too many arguments", got)
            # Input is still taken and dropped for a while...
            sock.sendall(b"y" * 1000)
            # ...then the server closes for good
            time.sleep(1.5)
            with self.assertRaises(OSError):
                for _ in range(20):
                    sock.sendall(b"z" * 65536)
                    time.sleep(0.05)
        self.assertAlive()


class TestProtocolErrors(MinisTestCase):
    """Requests that can never be parsed get an error and a close, like in
    Redis, instead of a connection waiting forever"""

    def raw(self, data):
        """Sends data on a fresh connection and returns everything the
        server sends back until it closes"""
        with socket.create_connection(("127.0.0.1", self.server.port)) as sock:
            sock.settimeout(3)
            sock.sendall(data)
            got = b""
            while True:
                chunk = sock.recv(4096)
                if not chunk:
                    return got
                got += chunk

    def test_too_many_arguments(self):
        want = b"-ERR Protocol error: too many arguments (at most 1024)\r\n"
        self.assertEqual(self.raw(b"*1025\r\n"), want)
        self.assertEqual(self.raw(raw_command("DEL", *["k"] * 2000)), want)
        self.assertAlive()

    def test_too_many_arguments_with_redis_py(self):
        for _ in range(20):
            start = time.time()
            with self.assertRaisesRegex(redis.ResponseError,
                                        "too many arguments"):
                self.r.delete(*[f"many:{i}" for i in range(1100)])
            self.assertLess(time.time() - start, 1)
            # The client reconnects
            self.assertTrue(self.r.ping())

    def test_malformed_requests(self):
        cases = [
            (b"*x\r\n", b"invalid multibulk length"),
            (b"*99999999999999999999\r\n", b"invalid multibulk length"),
            (b"*1\r\n:3\r\n", b"expected '$', got ':'"),
            (b"*1\r\n\r\n", b"expected '$', got ' '"),
            (b"*1\r\n$x\r\n", b"invalid bulk length"),
            (b"*1\r\n$-5\r\n", b"invalid bulk length"),
            (b"*1\r\n$4\r\nPINGxx", b"invalid CRLF in request"),
        ]
        for data, msg in cases:
            with self.subTest(data=data):
                self.assertEqual(self.raw(data),
                                 b"-ERR Protocol error: " + msg + b"\r\n")
        self.assertAlive()

    def test_garbage_is_caught_without_waiting(self):
        # Valkey waits for a CR (or 64KB) here, minis knows sooner
        self.assertEqual(self.raw(b"*1garbage"),
                         b"-ERR Protocol error: invalid multibulk length\r\n")

    def test_empty_requests_get_no_reply(self):
        with socket.create_connection(("127.0.0.1", self.server.port)) as sock:
            sock.settimeout(3)
            sock.sendall(b"*0\r\n*-1\r\n" + raw_command("PING"))
            self.assertEqual(sock.recv(100), b"+PONG\r\n")

    def test_commands_before_garbage_are_answered(self):
        self.assertEqual(self.raw(raw_command("PING") + b"*1\r\n$x\r\n"),
                         b"+PONG\r\n-ERR Protocol error: invalid bulk length\r\n")

    def test_request_too_large(self):
        # Known from the length header, without waiting for the data
        self.assertEqual(
            self.raw(b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$300000\r\n"),
            b"-ERR request too large\r\n")
        for size in (300_000, 5_000_000):
            with self.subTest(size=size):
                start = time.time()
                with self.assertRaisesRegex(redis.ResponseError,
                                            "request too large"):
                    self.r.set("too_large", "x" * size)
                self.assertLess(time.time() - start, 1)
        self.assertTrue(self.r.set("fits", "x" * 200_000))
        self.assertEqual(len(self.r.get("fits")), 200_000)

    def test_1024_arguments_work(self):
        keys = [f"limit:{i}" for i in range(1023)]
        self.r.mset({k: "v" for k in keys[:511]})
        self.assertEqual(self.r.delete(*keys), 511)


class TestTtlPersistence(MinisTestCase):
    """Snapshots keep each key's time left. (A reboot, which restarts the
    monotonic clock, is covered by persistence_test.c.)"""

    def test_ttl_survives_restart(self):
        self.r.set("tp_str", "v", ex=100)
        self.r.rpush("tp_list", "a")
        self.r.expire("tp_list", 200)
        self.r.hset("tp_hash", "f", "v")
        self.r.pexpire("tp_hash", 300_000)
        self.r.set("tp_forever", "v")
        self.r.close()

        self.server.restart()

        self.assertIn(self.r.ttl("tp_str"), (99, 100))
        self.assertIn(self.r.ttl("tp_list"), (199, 200))
        self.assertTrue(299_000 < self.r.pttl("tp_hash") <= 300_000)
        self.assertEqual(self.r.ttl("tp_forever"), -1)
        self.assertEqual(self.r.get("tp_str"), "v")

    def test_key_expiring_while_down_is_gone(self):
        self.r.set("tp_short", "v", px=500)
        self.r.set("tp_long", "v", ex=100)
        self.r.close()

        self.server.shutdown()
        time.sleep(0.8)
        self.server.start()

        self.assertIsNone(self.r.get("tp_short"))
        self.assertEqual(self.r.get("tp_long"), "v")

    def test_restored_ttl_still_expires(self):
        self.r.set("tp_soon", "v", px=1500)
        self.r.close()

        self.server.restart()

        self.assertEqual(self.r.get("tp_soon"), "v")
        time.sleep(1.7)
        self.assertIsNone(self.r.get("tp_soon"))


def lan_address():
    """This machine's address on its network, None if it has none. (No
    packet is sent: a UDP connect only picks the route.)"""
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        try:
            sock.connect(("192.0.2.1", 9))
            address = sock.getsockname()[0]
        except OSError:
            return None
    return None if address.startswith("127.") else address


def reachable(address, port):
    try:
        socket.create_connection((address, port), 1).close()
        return True
    except OSError:
        return False


class TestBind(unittest.TestCase):
    def test_bind_to_loopback(self):
        lan = lan_address()
        if lan is None:
            self.skipTest("no network address to check against")
        server = MinisServer(["-b", "127.0.0.1"])
        try:
            self.assertTrue(reachable("127.0.0.1", server.port))
            self.assertFalse(reachable(lan, server.port))
        finally:
            server.stop()

    def test_default_is_all_addresses(self):
        lan = lan_address()
        if lan is None:
            self.skipTest("no network address to check against")
        server = MinisServer()
        try:
            self.assertTrue(reachable("127.0.0.1", server.port))
            self.assertTrue(reachable(lan, server.port))
        finally:
            server.stop()

    def test_invalid_address_stops_startup(self):
        proc = subprocess.run([MINIS_BIN, "-p", str(free_port()), "-b", "nope"],
                              capture_output=True, text=True, timeout=10)
        self.assertNotEqual(proc.returncode, 0)
        self.assertIn("Invalid bind address: nope", proc.stdout + proc.stderr)


class Daemon:
    """minis -d, which detaches: its PID comes from the startup output"""

    def __init__(self, workdir, port):
        self.workdir, self.port = workdir, port

    def start(self):
        out = subprocess.run([MINIS_BIN, "-p", str(self.port), "-d"],
                             cwd=self.workdir, capture_output=True,
                             text=True, timeout=10)
        self.output = out.stdout + out.stderr
        self.returncode = out.returncode
        match = re.search(r"PID:\s+(\d+)", self.output)
        self.pid = int(match.group(1)) if match else None
        return self

    def stop(self):
        if self.pid is None:
            return
        try:
            os.kill(self.pid, signal.SIGTERM)
        except ProcessLookupError:
            return
        deadline = time.time() + 5
        while time.time() < deadline:
            try:
                os.kill(self.pid, 0)
            except ProcessLookupError:
                return
            time.sleep(0.05)
        os.kill(self.pid, signal.SIGKILL)


def ignores_sigchld(pid):
    with open(f"/proc/{pid}/status") as f:
        mask = next(line for line in f if line.startswith("SigIgn:"))
    return bool(int(mask.split()[1], 16) & (1 << (signal.SIGCHLD - 1)))


class TestSigchld(unittest.TestCase):
    """An ignored SIGCHLD reaps background snapshot children before
    minis waits for them, so it must not stay ignored"""

    def test_inherited_ignore_is_reset(self):
        port = free_port()
        with tempfile.TemporaryDirectory() as workdir:
            proc = subprocess.Popen(
                [MINIS_BIN, "-p", str(port)], cwd=workdir,
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                preexec_fn=lambda: signal.signal(signal.SIGCHLD, signal.SIG_IGN))
            try:
                deadline = time.time() + 5
                while not reachable("127.0.0.1", port):
                    self.assertLess(time.time(), deadline)
                    time.sleep(0.05)
                self.assertFalse(ignores_sigchld(proc.pid))
            finally:
                proc.terminate()
                proc.wait(5)

    def test_daemon_does_not_ignore_it(self):
        with tempfile.TemporaryDirectory() as workdir:
            daemon = Daemon(workdir, free_port()).start()
            try:
                self.assertFalse(ignores_sigchld(daemon.pid))
            finally:
                daemon.stop()


class TestDaemon(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="minis-daemon-")
        self.port = free_port()

    def tearDown(self):
        self.tmp.cleanup()

    def test_serving_when_start_returns(self):
        # The launcher waits until the daemon listens
        daemon = Daemon(self.tmp.name, self.port).start()
        try:
            self.assertEqual(daemon.returncode, 0, daemon.output)
            self.assertIn("started in the background", daemon.output)
            self.assertTrue(redis.Redis(port=self.port).ping())
        finally:
            daemon.stop()

    def test_failed_start_is_reported(self):
        with socket.socket() as taken:
            taken.bind(("0.0.0.0", self.port))
            taken.listen()
            daemon = Daemon(self.tmp.name, self.port).start()
            try:
                self.assertEqual(daemon.returncode, 1, daemon.output)
                self.assertIn("failed to start", daemon.output)
            finally:
                daemon.stop()

    def test_snapshots_survive_a_daemon_restart(self):
        # The daemon changes to /, snapshots must still go to ./shards
        daemon = Daemon(self.tmp.name, self.port).start()
        try:
            self.assertEqual(daemon.returncode, 0, daemon.output)
            r = redis.Redis(port=self.port, decode_responses=True)
            r.set("dk", "v")
            r.set("dk_ttl", "x", ex=100)
            r.rpush("dk_list", "a", "b")
            r.close()
        finally:
            daemon.stop()
        self.assertTrue(os.listdir(os.path.join(self.tmp.name, "shards")))

        daemon = Daemon(self.tmp.name, self.port).start()
        try:
            r = redis.Redis(port=self.port, decode_responses=True)
            self.assertEqual(r.get("dk"), "v")
            self.assertIn(r.ttl("dk_ttl"), (99, 100))
            self.assertEqual(r.lrange("dk_list", 0, -1), ["a", "b"])
            r.close()
        finally:
            daemon.stop()


class TestEmptyStrings(MinisTestCase):
    def test_empty_values_are_not_nil(self):
        self.r.set("es_str", "")
        self.assertEqual(self.r.get("es_str"), "")
        self.r.zadd("es_zset", {"": 1})
        self.assertEqual(self.r.execute_command("ZQUERY", "es_zset", 0, "", 0, 10),
                         ["", "1"])


class TestBinarySafe(MinisTestCase):
    """Values may hold any bytes, NUL included"""

    def setUp(self):
        super().setUp()
        self.b = redis.Redis(port=self.server.port, socket_timeout=2)

    def tearDown(self):
        self.b.close()
        super().tearDown()

    VALUES = [b"a\x00b", b"\x00", b"\x00\x00\x00", b"tail\x00",
              bytes(range(256)), b"\xff\xfe\x00\x01", b""]

    def test_string_values(self):
        for i, value in enumerate(self.VALUES):
            with self.subTest(value=value):
                self.assertTrue(self.b.set(f"bin_str{i}", value))
                self.assertEqual(self.b.get(f"bin_str{i}"), value)
                self.b.setex(f"bin_setex{i}", 100, value)
                self.assertEqual(self.b.get(f"bin_setex{i}"), value)
                self.b.set(f"bin_ex{i}", value, ex=100)
                self.assertEqual(self.b.get(f"bin_ex{i}"), value)

    def test_mset_mget(self):
        pairs = {f"bin_m{i}": v for i, v in enumerate(self.VALUES)}
        self.assertTrue(self.b.mset(pairs))
        self.assertEqual(self.b.mget(list(pairs)), list(pairs.values()))

    def test_differs_only_after_nul(self):
        # A C string compare took these for equal and kept the old value
        self.b.set("bin_same", b"a\x00b")
        self.b.set("bin_same", b"a\x00c")
        self.assertEqual(self.b.get("bin_same"), b"a\x00c")
        self.b.set("bin_same", b"a")
        self.assertEqual(self.b.get("bin_same"), b"a")

    def test_incr_needs_the_whole_value(self):
        self.b.set("bin_num", b"1\x002")
        with self.assertRaisesRegex(redis.ResponseError, "not an integer"):
            self.b.incr("bin_num")
        self.assertEqual(self.b.get("bin_num"), b"1\x002")
        self.b.set("bin_num", b"12")
        self.assertEqual(self.b.incr("bin_num"), 13)

    def test_list_elements(self):
        values = [v for v in self.VALUES]
        self.b.rpush("bin_list", *values)
        self.assertEqual(self.b.lrange("bin_list", 0, -1), values)
        self.b.lpush("bin_list", b"\x00head")
        self.assertEqual(self.b.lpop("bin_list"), b"\x00head")
        self.assertEqual(self.b.rpop("bin_list"), values[-1])
        self.assertEqual(self.b.lpop("bin_list", 2), values[:2])
        self.assertEqual(self.b.blpop("bin_list", timeout=1),
                         (b"bin_list", values[2]))
        self.assertEqual(self.b.brpop("bin_list", timeout=1),
                         (b"bin_list", values[-2]))

    def test_lrem_compares_every_byte(self):
        self.b.rpush("bin_lrem", b"a\x00b", b"a\x00c", b"a", b"a\x00b")
        self.assertEqual(self.b.lrem("bin_lrem", 0, b"a\x00b"), 2)
        self.assertEqual(self.b.lrange("bin_lrem", 0, -1), [b"a\x00c", b"a"])
        self.assertEqual(self.b.lrem("bin_lrem", 0, b"a"), 1)
        self.assertEqual(self.b.lrange("bin_lrem", 0, -1), [b"a\x00c"])

    def test_hash_fields_and_values(self):
        fields = {b"f\x00a": b"\x00one", b"f\x00b": b"two\x00", b"f": b"",
                  b"": b"empty field", b"\xff": bytes(range(256))}
        for field, value in fields.items():
            self.assertEqual(self.b.hset("bin_hash", field, value), 1)
        # Fields differing only after a NUL are different fields
        self.assertEqual(self.b.hgetall("bin_hash"), fields)
        for field, value in fields.items():
            self.assertEqual(self.b.hget("bin_hash", field), value)
            self.assertTrue(self.b.hexists("bin_hash", field))
        self.assertFalse(self.b.hexists("bin_hash", b"f\x00c"))
        self.assertEqual(self.b.hset("bin_hash", b"f\x00a", b"\x00one\x00"), 0)
        self.assertEqual(self.b.hget("bin_hash", b"f\x00a"), b"\x00one\x00")
        self.assertEqual(self.b.hdel("bin_hash", b"f\x00b", b"f\x00zz"), 1)
        self.assertIsNone(self.b.hget("bin_hash", b"f\x00b"))
        self.assertEqual(self.b.hget("bin_hash", b"f"), b"")

    def test_survives_restart(self):
        self.b.hset("bin_keep_hash", b"k\x00", b"\x00v")
        self.b.rpush("bin_keep_list", b"\x00", b"x\x00y")
        self.b.set("bin_keep", bytes(range(256)))
        self.b.set("bin_keep_ttl", b"\x00x\x00", ex=100)
        self.b.close()
        self.server.restart()
        self.assertEqual(self.b.get("bin_keep"), bytes(range(256)))
        self.assertEqual(self.b.get("bin_keep_ttl"), b"\x00x\x00")
        self.assertEqual(self.b.lrange("bin_keep_list", 0, -1),
                         [b"\x00", b"x\x00y"])
        self.assertEqual(self.b.hgetall("bin_keep_hash"), {b"k\x00": b"\x00v"})


class TestDel(MinisTestCase):
    def test_single_key(self):
        self.r.set("del_one", "v")
        self.assertEqual(self.r.delete("del_one"), 1)
        self.assertIsNone(self.r.get("del_one"))
        self.assertEqual(self.r.delete("del_one"), 0)

    def test_many_keys(self):
        self.r.set("del_a", "1")
        self.r.set("del_b", "2")
        self.r.hset("del_h", "f", "v")
        self.assertEqual(self.r.delete("del_a", "del_missing", "del_b", "del_h"), 3)
        self.assertEqual(self.r.exists("del_a", "del_b", "del_h"), 0)

    def test_duplicate_keys_count_once(self):
        self.r.set("del_dup", "v")
        self.assertEqual(self.r.delete("del_dup", "del_dup"), 1)

    def test_expired_key_not_counted(self):
        self.r.set("del_expired", "v")
        self.r.execute_command("PEXPIRE", "del_expired", 50)
        time.sleep(0.1)
        self.assertEqual(self.r.delete("del_expired"), 0)

    def test_no_keys_is_error(self):
        with self.assertRaises(redis.ResponseError):
            self.r.execute_command("DEL")
        self.assertAlive()

    def test_mdel_alias(self):
        self.r.set("mdel_a", "1")
        self.r.set("mdel_b", "2")
        self.assertEqual(self.r.execute_command("MDEL", "mdel_a", "mdel_b"), 2)


class TestIntegerArgs(MinisTestCase):
    BAD_INTS = ["", " 5", "+5", "5 ", "5x", "1.5", "99999999999999999999"]

    def assertNotInt(self, *cmd):
        with self.assertRaisesRegex(redis.ResponseError, "not an integer"):
            self.r.execute_command(*cmd)

    def test_pexpire_rejects_bad_ints(self):
        self.r.set("int_pexpire", "v")
        for bad in self.BAD_INTS:
            with self.subTest(bad=bad):
                self.assertNotInt("PEXPIRE", "int_pexpire", bad)
        # None of them may have set a TTL
        self.assertEqual(self.r.execute_command("PTTL", "int_pexpire"), -1)

    def test_incrby_rejects_bad_ints(self):
        self.r.set("int_incrby", "10")
        for bad in self.BAD_INTS:
            with self.subTest(bad=bad):
                self.assertNotInt("INCRBY", "int_incrby", bad)
                self.assertNotInt("DECRBY", "int_incrby", bad)
        self.assertEqual(self.r.get("int_incrby"), "10")

    def test_int64_limits_in_replies(self):
        self.r.set("int_limits", "0")
        self.assertEqual(self.r.incrby("int_limits", -(2**63)), -(2**63))
        self.r.set("int_limits", "0")
        self.assertEqual(self.r.incrby("int_limits", 2**63 - 1), 2**63 - 1)
        self.assertEqual(self.r.decr("int_limits"), 2**63 - 2)

    def test_decrby_int64_min(self):
        self.r.set("int_decrby_min", "0")
        with self.assertRaisesRegex(redis.ResponseError, "overflow"):
            self.r.decrby("int_decrby_min", -(2**63))
        self.assertEqual(self.r.get("int_decrby_min"), "0")
        self.assertAlive()


if __name__ == "__main__":
    unittest.main(verbosity=2)
