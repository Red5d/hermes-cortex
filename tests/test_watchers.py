"""Tests for WatcherManager, safety guards, log_tail micro-batching, and command_poll."""

from __future__ import annotations

import asyncio
import tempfile
import unittest
from pathlib import Path

from cortex.rules import CortexEvent
from cortex.store import CortexStore
from cortex.watchers import WatcherManager, validate_watcher_config


class TestWatchers(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.probes_dir = self.root / "probes"
        self.probes_dir.mkdir()
        self.logs_dir = self.root / "logs"
        self.logs_dir.mkdir()
        self.store = CortexStore(self.root / "cortex.db")

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def test_safety_guards_reject_traversal_and_unsafe_paths(self) -> None:
        with self.assertRaises(ValueError):
            validate_watcher_config(
                "command_poll",
                {"script_path": "../../bin/sh"},
                probes_dir=self.probes_dir,
                allowed_log_roots=[self.logs_dir],
            )

        with self.assertRaises(ValueError):
            validate_watcher_config(
                "command_poll",
                {"script_path": "/etc/passwd"},
                probes_dir=self.probes_dir,
                allowed_log_roots=[self.logs_dir],
            )

        with self.assertRaises(ValueError):
            validate_watcher_config(
                "log_tail",
                {"path": "/etc/shadow"},
                probes_dir=self.probes_dir,
                allowed_log_roots=[self.logs_dir],
            )

        cfg = validate_watcher_config(
            "command_poll",
            {"script_path": "check_disk.py", "interval_seconds": 1},
            probes_dir=self.probes_dir,
            allowed_log_roots=[self.logs_dir],
        )
        self.assertEqual(cfg["interval_seconds"], 5.0)
        self.assertTrue(Path(cfg["script_path"]).is_relative_to(self.probes_dir))

    def test_log_tail_micro_batching_and_live_reconcile(self) -> None:
        log_file = self.logs_dir / "app.log"
        log_file.write_text("", encoding="utf-8")

        events: list[CortexEvent] = []

        async def on_event(ev: CortexEvent) -> None:
            events.append(ev)

        async def run() -> None:
            mgr = WatcherManager(
                store=self.store,
                on_event=on_event,
                probes_dir=self.probes_dir,
                reconcile_interval_s=0.25,
            )
            await mgr.start()
            try:
                cfg = validate_watcher_config(
                    "log_tail",
                    {
                        "path": str(log_file),
                        "source": "app",
                        "regex_prefilter": "ERROR",
                        "from_end": False,
                        "batch_window_ms": 200,
                        "max_batch_lines": 20,
                    },
                    probes_dir=self.probes_dir,
                    allowed_log_roots=[self.logs_dir],
                )
                self.store.upsert_watcher(watcher_id="w_app", watcher_type="log_tail", config=cfg)
                mgr.trigger_reconcile()
                await asyncio.sleep(0.15)

                # Write 50 ERROR lines + 10 INFO lines in one burst
                lines = [f"INFO ok {i}" for i in range(10)] + [f"ERROR boom {i}" for i in range(50)]
                log_file.write_text("\n".join(lines) + "\n", encoding="utf-8")

                await asyncio.sleep(0.7)
                # 50 ERROR lines with max_batch_lines=20 -> 3 batched events (20 + 20 + 10), INFO filtered
                self.assertEqual(len(events), 3)
                total_lines = sum(e.metadata["line_count"] for e in events)
                self.assertEqual(total_lines, 50)
            finally:
                await mgr.stop()

        asyncio.run(run())

    def test_command_poll_executes_probe_script(self) -> None:
        probe = self.probes_dir / "disk_probe.py"
        probe.write_text('import json\nprint(json.dumps({"disk_pct": 91}))\n', encoding="utf-8")

        events: list[CortexEvent] = []

        async def on_event(ev: CortexEvent) -> None:
            events.append(ev)

        async def run() -> None:
            mgr = WatcherManager(
                store=self.store,
                on_event=on_event,
                probes_dir=self.probes_dir,
                reconcile_interval_s=0.2,
            )
            cfg = validate_watcher_config(
                "command_poll",
                {"script_path": "disk_probe.py", "source": "probe:disk", "interval_seconds": 5},
                probes_dir=self.probes_dir,
                allowed_log_roots=[self.logs_dir],
                require_probe_exists=True,
            )
            self.store.upsert_watcher(watcher_id="w_disk", watcher_type="command_poll", config=cfg)
            await mgr.start()
            try:
                await asyncio.sleep(0.4)
                self.assertGreaterEqual(len(events), 1)
                self.assertEqual(events[0].source, "probe:disk")
                self.assertEqual(events[0].payload["returncode"], 0)
                self.assertEqual(events[0].payload["output"], {"disk_pct": 91})
            finally:
                await mgr.stop()

        asyncio.run(run())


if __name__ == "__main__":
    unittest.main()
