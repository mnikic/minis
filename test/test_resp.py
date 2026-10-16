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
