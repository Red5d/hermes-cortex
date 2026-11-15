"""Rule evaluation, force_escalate_regex, fail-safe handling, and alert formatting."""

from __future__ import annotations

import json
import re
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Optional

from .decision import DecisionClient, DecisionResult, build_decision_prompt
from .store import CortexStore, RuleRecord

ACTION_PRIORITY = {"discard": 0, "log": 1, "escalate": 2}
TRIAGE_CHOICES = ("discard", "log", "escalate")
VALID_SEVERITIES = ("low", "medium", "high")


@dataclass(frozen=True)
class CortexEvent:
    source: str
    payload: Any
    ts: float = field(default_factory=time.time)
    metadata: dict[str, Any] = field(default_factory=dict)

    def as_text(self) -> str:
        if isinstance(self.payload, str):
            return self.payload
        try:
            return json.dumps(self.payload, ensure_ascii=False, sort_keys=True)
        except (TypeError, ValueError):
            return str(self.payload)


@dataclass(frozen=True)
class RuleEvaluation:
    rule_id: str
    rule_name: str
    severity: str
    action: str
    via: str  # "force_escalate_regex" | "system1" | "fail_safe" | "cooldown_suppressed"
    latency_ms: float = 0.0
    error: Optional[str] = None
    auto_investigate: bool = False


@dataclass(frozen=True)
class TriageOutcome:
    action: str
    source: str
    ts: float
    excerpt: str
    winning_rule_id: Optional[str]
    winning_rule_name: str
    severity: str
    via: str
    auto_investigate: bool
    evaluations: list[RuleEvaluation]

    def to_dict(self) -> dict[str, Any]:
        return {
            "action": self.action,
            "source": self.source,
            "ts": self.ts,
            "excerpt": self.excerpt,
            "winning_rule_id": self.winning_rule_id,
            "winning_rule_name": self.winning_rule_name,
            "severity": self.severity,
            "via": self.via,
            "auto_investigate": self.auto_investigate,
            "evaluations": [
                {
                    "rule_id": e.rule_id,
                    "rule_name": e.rule_name,
                    "severity": e.severity,
                    "action": e.action,
                    "via": e.via,
                    "latency_ms": round(e.latency_ms, 2),
                    "error": e.error,
                }
                for e in self.evaluations
            ],
        }


def validate_rule_payload(rule: dict[str, Any]) -> dict[str, Any]:
    """Validate and normalize a rule dict from tool arguments."""
    if not isinstance(rule, dict):
        raise ValueError("rule must be an object")
    name = str(rule.get("name") or "").strip()
    source = str(rule.get("source") or "").strip()
    condition = str(rule.get("condition") or "").strip()
    if not name:
        raise ValueError("rule.name is required")
    if not source:
        raise ValueError("rule.source is required")
    if not condition:
        raise ValueError("rule.condition is required")
    severity = str(rule.get("severity") or "medium").strip().lower()
    if severity not in VALID_SEVERITIES:
        raise ValueError(f"rule.severity must be one of {VALID_SEVERITIES}")
    cooldown_seconds = max(0, int(rule.get("cooldown_seconds", 300)))
    force_regex = rule.get("force_escalate_regex")
    if force_regex is not None:
        force_regex = str(force_regex)
        if force_regex:
            try:
                re.compile(force_regex)
            except re.error as exc:
                raise ValueError(f"invalid force_escalate_regex: {exc}") from exc
        else:
            force_regex = None
    return {
        "rule_id": rule.get("id") or rule.get("rule_id"),
        "name": name,
        "source": source,
        "condition": condition,
        "severity": severity,
        "cooldown_seconds": cooldown_seconds,
        "force_escalate_regex": force_regex,
        "auto_investigate": bool(rule.get("auto_investigate", False)),
        "enabled": bool(rule.get("enabled", True)),
    }


def _fail_safe_default(rule: RuleRecord) -> str:
    return "escalate" if rule.severity == "high" else "log"


def _matches_force_escalate(rule: RuleRecord, text: str) -> bool:
    if not rule.force_escalate_regex:
        return False
    try:
        return bool(re.search(rule.force_escalate_regex, text))
    except re.error:
        return False


def _combine_evaluations(event: CortexEvent, evals: list[RuleEvaluation]) -> TriageOutcome:
    excerpt = event.as_text()[:1500]
    if not evals:
        return TriageOutcome(
            action="discard",
            source=event.source,
            ts=event.ts,
            excerpt=excerpt,
            winning_rule_id=None,
            winning_rule_name="none",
            severity="low",
            via="no_matching_rule",
            auto_investigate=False,
            evaluations=[],
        )
    winner = max(evals, key=lambda e: ACTION_PRIORITY.get(e.action, 0))
    return TriageOutcome(
        action=winner.action,
        source=event.source,
        ts=event.ts,
        excerpt=excerpt,
        winning_rule_id=winner.rule_id,
        winning_rule_name=winner.rule_name,
        severity=winner.severity,
        via=winner.via,
        auto_investigate=any(e.auto_investigate for e in evals if e.action == "escalate"),
        evaluations=evals,
    )


async def evaluate_event(
    event: CortexEvent,
    store: CortexStore,
    decision_client: DecisionClient,
    *,
    dry_run: bool = False,
) -> TriageOutcome:
    rules = store.list_rules(source=event.source, enabled_only=True)
    if not rules:
        return _combine_evaluations(event, [])

    text = event.as_text()
    evals: list[RuleEvaluation] = []
    for rule in rules:
        if _matches_force_escalate(rule, text):
            evals.append(
                RuleEvaluation(
                    rule_id=rule.id,
                    rule_name=rule.name,
                    severity=rule.severity,
                    action="escalate",
                    via="force_escalate_regex",
                    auto_investigate=rule.auto_investigate,
                )
            )
            continue

        if not dry_run and not store.try_acquire_cooldown(
            rule.id, event.source, rule.cooldown_seconds, now=event.ts
        ):
            evals.append(
                RuleEvaluation(
                    rule_id=rule.id,
                    rule_name=rule.name,
                    severity=rule.severity,
                    action="discard",
                    via="cooldown_suppressed",
                )
            )
            continue

        default_action = _fail_safe_default(rule)
        prompt = build_decision_prompt(
            rule_name=rule.name,
            condition=rule.condition,
            source=event.source,
            payload_text=text,
            choices=TRIAGE_CHOICES,
        )
        res: DecisionResult = await decision_client.decide(
            prompt,
            TRIAGE_CHOICES,
            default=default_action,
            state=f"Source: {event.source}\n<event_data>\n{text[:4000]}\n</event_data>",
            instructions=f"Rule '{rule.name}': {rule.condition}",
        )
        evals.append(
            RuleEvaluation(
                rule_id=rule.id,
                rule_name=rule.name,
                severity=rule.severity,
                action=res.choice,
                via="fail_safe" if res.fallback_used else "system1",
                latency_ms=res.latency_ms,
                error=res.error,
                auto_investigate=rule.auto_investigate,
            )
        )

    return _combine_evaluations(event, evals)


def evaluate_event_sync(
    event: CortexEvent,
    store: CortexStore,
    decision_client: DecisionClient,
    *,
    dry_run: bool = True,
) -> TriageOutcome:
    """Synchronous evaluation path used by cortex_test_event."""
    rules = store.list_rules(source=event.source, enabled_only=True)
    if not rules:
        return _combine_evaluations(event, [])

    text = event.as_text()
    evals: list[RuleEvaluation] = []
    for rule in rules:
        if _matches_force_escalate(rule, text):
            evals.append(
                RuleEvaluation(
                    rule_id=rule.id,
                    rule_name=rule.name,
                    severity=rule.severity,
                    action="escalate",
                    via="force_escalate_regex",
                    auto_investigate=rule.auto_investigate,
                )
            )
            continue

        if not dry_run and not store.try_acquire_cooldown(
            rule.id, event.source, rule.cooldown_seconds, now=event.ts
        ):
            evals.append(
                RuleEvaluation(
                    rule_id=rule.id,
                    rule_name=rule.name,
                    severity=rule.severity,
                    action="discard",
                    via="cooldown_suppressed",
                )
            )
            continue

        default_action = _fail_safe_default(rule)
        prompt = build_decision_prompt(
            rule_name=rule.name,
            condition=rule.condition,
            source=event.source,
            payload_text=text,
            choices=TRIAGE_CHOICES,
        )
        res: DecisionResult = decision_client.decide_sync(
            prompt,
            TRIAGE_CHOICES,
            default=default_action,
            state=f"Source: {event.source}\n<event_data>\n{text[:4000]}\n</event_data>",
            instructions=f"Rule '{rule.name}': {rule.condition}",
        )
        evals.append(
            RuleEvaluation(
                rule_id=rule.id,
                rule_name=rule.name,
                severity=rule.severity,
                action=res.choice,
                via="fail_safe" if res.fallback_used else "system1",
                latency_ms=res.latency_ms,
                error=res.error,
                auto_investigate=rule.auto_investigate,
            )
        )

    return _combine_evaluations(event, evals)


def format_escalation_alert(outcome: TriageOutcome) -> str:
    """Build deterministic escalation payload for herald (zero System 2 tokens)."""
    iso_ts = datetime.fromtimestamp(outcome.ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    investigate_hint = " (auto_investigate=true: consider delegating to analyst)" if outcome.auto_investigate else ""
    return (
        f"[CORTEX ESCALATION]{investigate_hint}\n"
        f"rule: {outcome.winning_rule_name} ({outcome.winning_rule_id})\n"
        f"severity: {outcome.severity}\n"
        f"source: {outcome.source}\n"
        f"timestamp: {iso_ts}\n"
        f"decision_via: {outcome.via}\n"
        f"<event_excerpt>\n{outcome.excerpt}\n</event_excerpt>"
    )
