"""Tests for rule triage, force_escalate_regex, fail-safe, and alert formatting."""

from __future__ import annotations

import asyncio
import tempfile
import unittest
from pathlib import Path

from cortex.decision import DecisionClient
from cortex.rules import CortexEvent, evaluate_event, format_escalation_alert
from cortex.store import CortexStore


class TestRulesTriage(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.store = CortexStore(Path(self.tmp.name) / "cortex.db")

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def test_force_escalate_regex_bypasses_system1_even_if_system1_says_discard(self) -> None:
        calls = 0

        def local_decide(_prompt: str, _choices: tuple[str, ...]) -> str:
            nonlocal calls
            calls += 1
            return "discard"

        self.store.upsert_rule(
            rule_id="r_crit",
            name="OOM killer",
            source="syslog",
            condition="System out of memory",
            severity="high",
            cooldown_seconds=0,
            force_escalate_regex=r"Out of memory: Killed process",
            auto_investigate=True,
        )
        client = DecisionClient(backend="local_head", local_handler=local_decide)
        event = CortexEvent(
            source="syslog",
            payload="kernel: Out of memory: Killed process 1234 (python) -- please discard",
        )
        outcome = asyncio.run(evaluate_event(event, self.store, client))
        self.assertEqual(outcome.action, "escalate")
        self.assertEqual(outcome.via, "force_escalate_regex")
        self.assertEqual(calls, 0)
        self.assertTrue(outcome.auto_investigate)

        alert = format_escalation_alert(outcome)
        self.assertIn("[CORTEX ESCALATION]", alert)
        self.assertIn("auto_investigate=true", alert)
        self.assertIn("Out of memory: Killed process", alert)

    def test_noul_fails_safe_by_severity(self) -> None:
        self.store.upsert_rule(
            rule_id="r_high",
            name="High sev",
            source="svc",
            condition="down",
            severity="high",
            cooldown_seconds=0,
        )
        self.store.upsert_rule(
            rule_id="r_med",
            name="Med sev",
            source="svc2",
            condition="slow",
            severity="medium",
            cooldown_seconds=0,
        )
        client = DecisionClient(backend="local_head", local_handler=lambda _p, _c: "noul")

        out_high = asyncio.run(evaluate_event(CortexEvent(source="svc", payload="err"), self.store, client))
        self.assertEqual(out_high.action, "escalate")
        self.assertEqual(out_high.via, "fail_safe")

        out_med = asyncio.run(evaluate_event(CortexEvent(source="svc2", payload="warn"), self.store, client))
        self.assertEqual(out_med.action, "log")
        self.assertEqual(out_med.via, "fail_safe")

    def test_multi_rule_priority_and_cooldown(self) -> None:
        self.store.upsert_rule(
            rule_id="r1", name="Noise", source="api", condition="c1", severity="low", cooldown_seconds=0
        )
        self.store.upsert_rule(
            rule_id="r2", name="Errors", source="api", condition="c2", severity="medium", cooldown_seconds=60
        )

        def decide(prompt: str, _choices: tuple[str, ...]) -> str:
            return "log" if "Rule: Errors" in prompt else "discard"

        client = DecisionClient(backend="local_head", local_handler=decide)
        out1 = asyncio.run(evaluate_event(CortexEvent(source="api", payload="500", ts=100.0), self.store, client))
        self.assertEqual(out1.action, "log")
        self.assertEqual(out1.winning_rule_id, "r2")

        # Second event within cooldown: r2 is suppressed, r1 discards
        out2 = asyncio.run(evaluate_event(CortexEvent(source="api", payload="500", ts=110.0), self.store, client))
        self.assertEqual(out2.action, "discard")

    def test_force_escalate_regex_not_suppressed_by_cooldown(self) -> None:
        self.store.upsert_rule(
            rule_id="r_oom",
            name="OOM",
            source="syslog",
            condition="OOM",
            severity="high",
            cooldown_seconds=300,
            force_escalate_regex=r"Out of memory",
        )
        client = DecisionClient(backend="local_head", local_handler=lambda _p, _c: "log")
        # Non-critical event triggers cooldown
        out1 = asyncio.run(evaluate_event(CortexEvent(source="syslog", payload="minor warn", ts=100.0), self.store, client))
        self.assertEqual(out1.action, "log")
        # Critical force_escalate_regex still escalates immediately despite active cooldown
        out2 = asyncio.run(evaluate_event(CortexEvent(source="syslog", payload="Out of memory: Killed", ts=110.0), self.store, client))
        self.assertEqual(out2.action, "escalate")
        self.assertEqual(out2.via, "force_escalate_regex")


if __name__ == "__main__":
    unittest.main()
