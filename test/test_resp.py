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
import socket
import subprocess
import sys
import tempfile
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
    def __init__(self):
        self.workdir = tempfile.TemporaryDirectory(prefix="minis-test-")
        self.port = free_port()
        self.log = open(os.path.join(self.workdir.name, "server.log"), "w")
        self.start()

    def start(self):
        self.proc = subprocess.Popen(
            [MINIS_BIN, "-p", str(self.port)],
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
