"""Tests for plugin registration, tool calling convention, daemon gating, and escalation delivery."""

from __future__ import annotations

import asyncio
import json
import os
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import cortex
from cortex.daemon import CortexRuntime
from cortex.decision import DecisionClient
from cortex.rules import CortexEvent
from cortex.store import CortexStore


class _FakeCtx:
    def __init__(self, settings: dict, profile_name: str = "default"):
        self._settings = settings
        self.profile_name = profile_name
        self.tools: dict[str, dict] = {}
        self.hooks: dict[str, list] = {}
        self.unload_callbacks: list = []
        self.injected: list[dict] = []
        self.inject_returns = True

    def get_config(self, key: str, default=None):
        return self._settings.get(key, default)

    def register_tool(self, *, name, toolset, schema, handler, description="", **_kw):
        self.tools[name] = {"toolset": toolset, "schema": schema, "handler": handler}

    def register_hook(self, hook_name: str, callback):
        self.hooks.setdefault(hook_name, []).append(callback)

    def on_unload(self, callback):
        self.unload_callbacks.append(callback)

    def inject_message(self, content: str, role: str = "user", *, session_key=None, origin=None) -> bool:
        self.injected.append({"content": content, "role": role, "origin": origin})
        return self.inject_returns


class TestToolsAndPluginIntegration(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def test_register_daemon_gating_and_tool_handlers(self) -> None:
        # Worker profile ("analyst") should register tools and hooks, NOT start daemon
        analyst_ctx = _FakeCtx(
            {
                "shared_state_dir": str(self.root / "shared"),
                "daemon_profile": "herald",
                "decision_backend": "local_head",
            },
            profile_name="analyst",
        )
        cortex.register(analyst_ctx)
        self.assertEqual(
            set(analyst_ctx.tools.keys()),
            {"cortex_manage_watchers", "cortex_manage_rules", "cortex_query_log", "cortex_test_event"},
        )
        self.assertEqual(analyst_ctx.unload_callbacks, [])

        # Tool handler signature test (args: dict, **ctx_kwargs) -> JSON str
        res_rule = json.loads(
            analyst_ctx.tools["cortex_manage_rules"]["handler"](
                {
                    "action": "add",
                    "rule": {
                        "id": "r_test",
                        "name": "Test High",
                        "source": "probe:disk",
                        "condition": "disk > 95%",
                        "severity": "high",
                    },
                },
                task_id="ignored_kwarg",
            )
        )
        self.assertTrue(res_rule["ok"])

        # Dry run test_event (no local_handler -> fail-safe high -> escalate)
        dry = json.loads(
            analyst_ctx.tools["cortex_test_event"]["handler"](
                {"source": "probe:disk", "payload": {"usage": 98}}
            )
        )
        self.assertTrue(dry["ok"])
        self.assertTrue(dry["dry_run"])
        self.assertEqual(dry["outcome"]["action"], "escalate")

        # pre_gateway_dispatch captures origin cleanly
        fake_event = SimpleNamespace(
            source=SimpleNamespace(to_dict=lambda: {"platform": "discord", "chat_id": "12345"})
        )
        for cb in analyst_ctx.hooks["pre_gateway_dispatch"]:
            self.assertIsNone(cb(event=fake_event))
        store = CortexStore(self.root / "shared" / "cortex.db")
        self.assertEqual(store.get_meta("last_active_origin"), {"platform": "discord", "chat_id": "12345"})

    def test_herald_daemon_gating_and_escalation_delivery(self) -> None:
        ctx = _FakeCtx(
            {
                "shared_state_dir": str(self.root / "shared"),
                "daemon_profile": "herald",
                "listen_port": 0,
            },
            profile_name="herald",
        )
        store = CortexStore(self.root / "shared" / "cortex.db")
        store.set_meta("last_active_origin", {"platform": "discord", "chat_id": "999"})
        store.upsert_rule(
            rule_id="r_esc",
            name="Service down",
            source="uptime",
            condition="Service is down",
            severity="high",
            cooldown_seconds=0,
        )
        client = DecisionClient(backend="local_head", local_handler=lambda _p, _c: "escalate")
        runtime = CortexRuntime(
            ctx=ctx,
            store=store,
            decision_client=client,
            probes_dir=self.root / "shared" / "probes",
            lock_path=self.root / "shared" / "daemon.lock",
            listen_port=0,
        )
        self.assertTrue(runtime.should_start_daemon(daemon_profile="herald"))

        with patch.dict(os.environ, {"HERMES_KANBAN_TASK": "t_123"}):
            self.assertFalse(runtime.should_start_daemon(daemon_profile="herald"))

        asyncio.run(runtime.handle_event(CortexEvent(source="uptime", payload="502 down")))
        self.assertEqual(len(ctx.injected), 1)
        self.assertEqual(ctx.injected[0]["origin"], {"platform": "discord", "chat_id": "999"})
        self.assertIn("[CORTEX ESCALATION]", ctx.injected[0]["content"])
        logs = store.query_log(decision="escalate")
        self.assertEqual(len(logs), 1)

    def test_runtime_start_stop_and_failure_releases_lock(self) -> None:
        ctx = _FakeCtx({"shared_state_dir": str(self.root / "shared")}, profile_name="herald")
        store = CortexStore(self.root / "shared" / "cortex.db")
        client = DecisionClient(backend="local_head", local_handler=lambda _p, _c: "discard")
        runtime = CortexRuntime(
            ctx=ctx,
            store=store,
            decision_client=client,
            probes_dir=self.root / "shared" / "probes",
            lock_path=self.root / "shared" / "daemon.lock",
            listen_port=0,
        )
        self.assertTrue(runtime.start())
        self.assertIsNotNone(runtime._thread)
        self.assertTrue(runtime._thread.is_alive())
        runtime.stop()
        self.assertIsNone(runtime._thread)

        # Simulate _async_start crash -> start() returns False and releases lock
        async def boom():
            raise OSError("port in use")

        runtime._async_start = boom  # type: ignore[method-assign]
        self.assertFalse(runtime.start())
        self.assertIsNone(runtime._thread)
        self.assertIsNone(runtime._lock_fh)

    def test_kanban_fallback_positional_title_arg(self) -> None:
        ctx = _FakeCtx({}, profile_name="herald")
        ctx.inject_returns = False
        store = CortexStore(self.root / "shared" / "cortex.db")
        store.upsert_rule(
            rule_id="r_esc",
            name="Service down",
            source="uptime",
            condition="down",
            severity="high",
            cooldown_seconds=0,
        )
        client = DecisionClient(backend="local_head", local_handler=lambda _p, _c: "escalate")
        runtime = CortexRuntime(
            ctx=ctx,
            store=store,
            decision_client=client,
            probes_dir=self.root / "shared" / "probes",
            lock_path=self.root / "shared" / "daemon.lock",
            listen_port=0,
        )
        captured_argv: list[tuple] = []

        class _FakeProc:
            returncode = 0

            async def communicate(self):
                return b"created\n", b""

        async def fake_exec(*args, **_kw):
            captured_argv.append(args)
            return _FakeProc()

        with patch("shutil.which", return_value="/usr/local/bin/hermes"), patch(
            "asyncio.create_subprocess_exec", side_effect=fake_exec
        ):
            asyncio.run(runtime.handle_event(CortexEvent(source="uptime", payload="502")))

        self.assertEqual(len(captured_argv), 1)
        argv = captured_argv[0]
        self.assertEqual(argv[:3], ("/usr/local/bin/hermes", "kanban", "create"))
        self.assertNotIn("--title", argv)
        self.assertTrue(argv[3].startswith("[Cortex] HIGH:"))

    def test_allowed_log_roots_includes_base_hermes_home_under_profile_override(self) -> None:
        base_home = self.root / "base_hermes"
        profile_home = base_home / "profiles" / "analyst"
        (base_home / "logs").mkdir(parents=True)
        (profile_home / "logs").mkdir(parents=True)
        base_log = base_home / "logs" / "errors.log"
        base_log.write_text("err\n", encoding="utf-8")

        fake_hc = SimpleNamespace(
            get_process_hermes_home=lambda: base_home,
            get_hermes_home=lambda: profile_home,
        )
        ctx = _FakeCtx({"daemon_profile": "herald"}, profile_name="analyst")
        with patch.dict("sys.modules", {"hermes_constants": fake_hc}):
            cortex.register(ctx)
        res = json.loads(
            ctx.tools["cortex_manage_watchers"]["handler"](
                {
                    "action": "add",
                    "watcher": {
                        "id": "w_base_err",
                        "type": "log_tail",
                        "config": {"path": str(base_log), "source": "hermes_errors"},
                    },
                }
            )
        )
        self.assertTrue(res["ok"], res)


if __name__ == "__main__":
    unittest.main()
