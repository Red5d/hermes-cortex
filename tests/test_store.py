"""Tests for CortexStore (SQLite WAL cross-profile persistence)."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from cortex.store import CortexStore


class TestCortexStore(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tmp.name) / "cortex.db"
        self.store_herald = CortexStore(self.db_path)
        self.store_analyst = CortexStore(self.db_path)

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def test_cross_profile_visibility_and_watermark(self) -> None:
        w0 = self.store_herald.get_watchers_watermark()
        rec = self.store_analyst.upsert_watcher(
            watcher_id="w_caddy",
            watcher_type="log_tail",
            config={"path": "/var/log/caddy.log", "source": "caddy"},
        )
        self.assertEqual(rec.id, "w_caddy")
        self.assertGreater(self.store_herald.get_watchers_watermark(), w0)
        watchers = self.store_herald.list_watchers()
        self.assertEqual([w.id for w in watchers], ["w_caddy"])

        toggled = self.store_analyst.set_watcher_enabled("w_caddy")
        self.assertIsNotNone(toggled)
        self.assertFalse(toggled.enabled)
        self.assertEqual(self.store_herald.list_watchers(enabled_only=True), [])

        self.assertTrue(self.store_analyst.delete_watcher("w_caddy"))
        self.assertEqual(self.store_herald.list_watchers(), [])

    def test_rules_cooldown_and_event_log_trim(self) -> None:
        r = self.store_analyst.upsert_rule(
            rule_id="r_502",
            name="Caddy 502 spike",
            source="caddy",
            condition="Spike in HTTP 502 errors",
            severity="high",
            cooldown_seconds=60,
        )
        self.assertEqual(r.id, "r_502")
        self.assertEqual(len(self.store_herald.list_rules(source="caddy", enabled_only=True)), 1)

        self.assertTrue(self.store_herald.try_acquire_cooldown("r_502", "caddy", 60, now=1000.0))
        self.assertFalse(self.store_herald.try_acquire_cooldown("r_502", "caddy", 60, now=1030.0))
        self.assertTrue(self.store_herald.try_acquire_cooldown("r_502", "caddy", 60, now=1061.0))

        for i in range(5):
            self.store_herald.record_event(
                source="caddy", rule_id="r_502", decision="log", excerpt=f"line {i}", max_rows=3
            )
        logs = self.store_analyst.query_log(limit=10)
        self.assertEqual(len(logs), 3)
        self.assertEqual(logs[0]["excerpt"], "line 4")


if __name__ == "__main__":
    unittest.main()
