"""Agent-facing Cortex tools (handler(args: dict, **ctx_kwargs) -> str)."""

from __future__ import annotations

import json
from dataclasses import asdict
from pathlib import Path
from typing import Any, Callable, Iterable

from .decision import DecisionClient
from .rules import CortexEvent, evaluate_event_sync, validate_rule_payload
from .store import CortexStore
from .watchers import validate_watcher_config


def _json_ok(data: dict[str, Any]) -> str:
    return json.dumps({"ok": True, **data}, ensure_ascii=False)


def _json_err(message: str) -> str:
    return json.dumps({"ok": False, "error": message}, ensure_ascii=False)


def register_cortex_tools(
    ctx: Any,
    *,
    store: CortexStore,
    decision_client: DecisionClient,
    probes_dir: Path,
    allowed_log_roots: Iterable[Path],
    allow_arbitrary_log_paths: bool = False,
    on_watchers_changed: Optional[Callable[[], None]] = None,
) -> None:
    """Register the four fixed-schema Cortex tools on PluginContext."""

    def handle_manage_watchers(args: dict[str, Any], **_kw: Any) -> str:
        action = str((args or {}).get("action") or "").strip().lower()
        watcher_id = (args or {}).get("watcher_id")
        watcher = (args or {}).get("watcher")

        try:
            if action == "list":
                rows = [asdict(w) for w in store.list_watchers()]
                return _json_ok({"watchers": rows, "count": len(rows)})

            if action in ("add", "update"):
                if not isinstance(watcher, dict):
                    return _json_err(f"'watcher' object is required for action={action}")
                wid = watcher_id or watcher.get("id") or watcher.get("watcher_id")
                if action == "update":
                    if not wid:
                        return _json_err("'watcher_id' is required for action=update")
                    existing = store.get_watcher(str(wid))
                    if existing is None:
                        return _json_err(f"watcher not found: {wid}")
                    wtype = str(watcher.get("type") or existing.type)
                    merged_cfg = {**existing.config, **(watcher.get("config") or {})}
                    enabled = bool(watcher.get("enabled", existing.enabled))
                else:
                    wtype = str(watcher.get("type") or "")
                    merged_cfg = dict(watcher.get("config") or {})
                    enabled = bool(watcher.get("enabled", True))

                normalized_cfg = validate_watcher_config(
                    wtype,
                    merged_cfg,
                    probes_dir=probes_dir,
                    allowed_log_roots=allowed_log_roots,
                    allow_arbitrary_log_paths=allow_arbitrary_log_paths,
                    require_probe_exists=False,
                )
                rec = store.upsert_watcher(
                    watcher_id=str(wid) if wid else None,
                    watcher_type=wtype,
                    config=normalized_cfg,
                    enabled=enabled,
                )
                if on_watchers_changed is not None:
                    on_watchers_changed()
                return _json_ok({"watcher": asdict(rec)})

            if action == "remove":
                if not watcher_id:
                    return _json_err("'watcher_id' is required for action=remove")
                deleted = store.delete_watcher(str(watcher_id))
                if not deleted:
                    return _json_err(f"watcher not found: {watcher_id}")
                if on_watchers_changed is not None:
                    on_watchers_changed()
                return _json_ok({"removed": str(watcher_id)})

            if action == "toggle":
                if not watcher_id:
                    return _json_err("'watcher_id' is required for action=toggle")
                rec = store.set_watcher_enabled(str(watcher_id))
                if rec is None:
                    return _json_err(f"watcher not found: {watcher_id}")
                if on_watchers_changed is not None:
                    on_watchers_changed()
                return _json_ok({"watcher": asdict(rec)})

            return _json_err(f"unknown action: {action!r}")
        except ValueError as exc:
            return _json_err(str(exc))

    def handle_manage_rules(args: dict[str, Any], **_kw: Any) -> str:
        action = str((args or {}).get("action") or "").strip().lower()
        rule_id = (args or {}).get("rule_id")
        rule = (args or {}).get("rule")

        try:
            if action == "list":
                rows = [asdict(r) for r in store.list_rules()]
                return _json_ok({"rules": rows, "count": len(rows)})

            if action in ("add", "update"):
                if not isinstance(rule, dict):
                    return _json_err(f"'rule' object is required for action={action}")
                if action == "update":
                    rid = rule_id or rule.get("id") or rule.get("rule_id")
                    if not rid:
                        return _json_err("'rule_id' is required for action=update")
                    existing = store.get_rule(str(rid))
                    if existing is None:
                        return _json_err(f"rule not found: {rid}")
                    merged = {**asdict(existing), **rule, "id": existing.id}
                else:
                    merged = {**rule}
                    if rule_id and "id" not in merged:
                        merged["id"] = rule_id
                validated = validate_rule_payload(merged)
                rec = store.upsert_rule(**validated)
                return _json_ok({"rule": asdict(rec)})

            if action == "remove":
                if not rule_id:
                    return _json_err("'rule_id' is required for action=remove")
                deleted = store.delete_rule(str(rule_id))
                if not deleted:
                    return _json_err(f"rule not found: {rule_id}")
                return _json_ok({"removed": str(rule_id)})

            if action == "toggle":
                if not rule_id:
                    return _json_err("'rule_id' is required for action=toggle")
                rec = store.set_rule_enabled(str(rule_id))
                if rec is None:
                    return _json_err(f"rule not found: {rule_id}")
                return _json_ok({"rule": asdict(rec)})

            return _json_err(f"unknown action: {action!r}")
        except ValueError as exc:
            return _json_err(str(exc))

    def handle_query_log(args: dict[str, Any], **_kw: Any) -> str:
        params = args or {}
        limit = int(params.get("limit", 50))
        source = params.get("source")
        decision = params.get("decision")
        entries = store.query_log(limit=limit, source=source, decision=decision)
        metrics = {
            name: store.get_meta(f"metric:{name}", 0)
            for name in ("discard", "log", "escalate", "escalation_delivered", "escalation_fallback")
        }
        return _json_ok({"events": entries, "count": len(entries), "metrics": metrics})

    def handle_test_event(args: dict[str, Any], **_kw: Any) -> str:
        params = args or {}
        source = str(params.get("source") or "").strip()
        if not source:
            return _json_err("'source' is required")
        payload = params.get("payload")
        if payload is None:
            return _json_err("'payload' is required")
        event = CortexEvent(source=source, payload=payload)
        outcome = evaluate_event_sync(event, store, decision_client, dry_run=True)
        return _json_ok({"dry_run": True, "outcome": outcome.to_dict()})

    ctx.register_tool(
        name="cortex_manage_watchers",
        toolset="cortex",
        schema={
            "name": "cortex_manage_watchers",
            "description": (
                "List, add, update, remove, or toggle dynamic Cortex event watchers "
                "(log_tail, http_poll, webhook_route, command_poll). Changes hot-reload without restarting."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "action": {
                        "type": "string",
                        "enum": ["list", "add", "update", "remove", "toggle"],
                    },
                    "watcher_id": {"type": "string"},
                    "watcher": {
                        "type": "object",
                        "properties": {
                            "id": {"type": "string"},
                            "type": {
                                "type": "string",
                                "enum": ["log_tail", "http_poll", "webhook_route", "command_poll"],
                            },
                            "config": {"type": "object"},
                            "enabled": {"type": "boolean"},
                        },
                    },
                },
                "required": ["action"],
            },
        },
        handler=handle_manage_watchers,
        description="Manage Cortex dynamic event watchers.",
    )

    ctx.register_tool(
        name="cortex_manage_rules",
        toolset="cortex",
        schema={
            "name": "cortex_manage_rules",
            "description": "List, add, update, remove, or toggle Cortex System 1 triage rules.",
            "parameters": {
                "type": "object",
                "properties": {
                    "action": {
                        "type": "string",
                        "enum": ["list", "add", "update", "remove", "toggle"],
                    },
                    "rule_id": {"type": "string"},
                    "rule": {
                        "type": "object",
                        "properties": {
                            "id": {"type": "string"},
                            "name": {"type": "string"},
                            "source": {"type": "string"},
                            "condition": {"type": "string"},
                            "severity": {"type": "string", "enum": ["low", "medium", "high"]},
                            "cooldown_seconds": {"type": "integer"},
                            "force_escalate_regex": {"type": "string"},
                            "auto_investigate": {"type": "boolean"},
                            "enabled": {"type": "boolean"},
                        },
                    },
                },
                "required": ["action"],
            },
        },
        handler=handle_manage_rules,
        description="Manage Cortex System 1 triage rules.",
    )

    ctx.register_tool(
        name="cortex_query_log",
        toolset="cortex",
        schema={
            "name": "cortex_query_log",
            "description": "Query logged Cortex events and triage metrics.",
            "parameters": {
                "type": "object",
                "properties": {
                    "limit": {"type": "integer", "default": 50},
                    "source": {"type": "string"},
                    "decision": {"type": "string", "enum": ["discard", "log", "escalate"]},
                },
            },
        },
        handler=handle_query_log,
        description="Query Cortex event log and metrics.",
    )

    ctx.register_tool(
        name="cortex_test_event",
        toolset="cortex",
        schema={
            "name": "cortex_test_event",
            "description": "Dry-run a synthetic event through Cortex rules and System 1 decision model without escalating.",
            "parameters": {
                "type": "object",
                "properties": {
                    "source": {"type": "string"},
                    "payload": {},
                },
                "required": ["source", "payload"],
            },
        },
        handler=handle_test_event,
        description="Dry-run an event through Cortex System 1 triage.",
    )
