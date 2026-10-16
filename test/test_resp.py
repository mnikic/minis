#!/usr/bin/env python3
"""
RESP tests driven by redis-py, the client leadgrep uses.

Starts its own server on a free port in a temporary directory.
MINIS_BIN selects the binary (default: build/bin/release/minis).

    python3 test/test_resp.py
"""

import os
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

    def stop(self):
        if self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(5)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait()
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
