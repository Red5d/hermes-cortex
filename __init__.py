"""Cortex Hermes plugin: System 1 event triage, dynamic watchers, and herald escalation."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Optional

from .daemon import CortexRuntime, resolve_secret_from_scope
from .decision import DecisionClient
from .store import CortexStore
from .tools import register_cortex_tools

logger = logging.getLogger("cortex")


def _default_shared_dir() -> Path:
    try:
        from hermes_constants import get_process_hermes_home

        return Path(get_process_hermes_home()) / "cortex"
    except Exception:
        return Path.home() / ".hermes" / "cortex"


def _resolve_api_key() -> Optional[str]:
    return resolve_secret_from_scope("CORTEX_DECISION_API_KEY") or resolve_secret_from_scope(
        "TYPESAFE_KEY"
    )


def register(ctx: Any) -> None:
    raw_shared = str(ctx.get_config("shared_state_dir", "") or "").strip()
    shared_dir = Path(raw_shared).expanduser() if raw_shared else _default_shared_dir()
    db_path = shared_dir / "cortex.db"
    probes_dir = shared_dir / "probes"
    lock_path = shared_dir / "daemon.lock"

    store = CortexStore(db_path)

    raw_roots = ctx.get_config("allowed_log_roots", ["/var/log", "/opt/data/logs"]) or []
    allowed_roots: list[Path] = [Path(str(p)).expanduser() for p in raw_roots]
    try:
        from hermes_constants import get_hermes_home, get_process_hermes_home

        for candidate in (
            Path(get_process_hermes_home()) / "logs",
            Path(get_hermes_home()) / "logs",
            shared_dir.parent / "logs",
        ):
            if candidate not in allowed_roots:
                allowed_roots.append(candidate)
    except Exception as exc:
        logger.debug("failed to append Hermes log roots: %s", exc)

    decision_client = DecisionClient(
        backend=str(ctx.get_config("decision_backend", "http_decision")),
        url=str(ctx.get_config("decision_url", "https://api.typesafe.ai/v1/systemone")),
        model=str(ctx.get_config("decision_model", "jev-latest")),
        request_format=str(ctx.get_config("decision_format", "systemone")),
        timeout_s=float(ctx.get_config("decision_timeout_s", 2.0)),
        secret_resolver=_resolve_api_key,
        llm_facade=getattr(ctx, "llm", None),
    )

    runtime = CortexRuntime(
        ctx=ctx,
        store=store,
        decision_client=decision_client,
        probes_dir=probes_dir,
        lock_path=lock_path,
        listen_host=str(ctx.get_config("listen_host", "127.0.0.1")),
        listen_port=int(ctx.get_config("listen_port", 8655)),
        reconcile_interval_s=float(ctx.get_config("reconcile_interval_s", 1.5)),
        escalation_origins=list(ctx.get_config("escalation_origins", []) or []),
        kanban_fallback=bool(ctx.get_config("kanban_fallback", True)),
        kanban_assignee=str(ctx.get_config("kanban_assignee", "herald")),
    )

    register_cortex_tools(
        ctx,
        store=store,
        decision_client=decision_client,
        probes_dir=probes_dir,
        allowed_log_roots=allowed_roots,
        allow_arbitrary_log_paths=bool(ctx.get_config("allow_arbitrary_log_paths", False)),
        on_watchers_changed=runtime.notify_watchers_changed,
    )

    def _on_session_start(**kwargs: Any) -> None:
        try:
            from gateway.session_context import get_session_env

            platform = str(get_session_env("HERMES_SESSION_PLATFORM") or kwargs.get("platform") or "").strip()
            chat_id = str(get_session_env("HERMES_SESSION_CHAT_ID") or "").strip()
            if platform and chat_id:
                origin: dict[str, Any] = {
                    "platform": platform,
                    "chat_id": chat_id,
                    "chat_type": str(get_session_env("HERMES_SESSION_CHAT_TYPE") or "dm"),
                }
                for opt_key, env_key in (
                    ("thread_id", "HERMES_SESSION_THREAD_ID"),
                    ("scope_id", "HERMES_SESSION_SCOPE_ID"),
                    ("user_id", "HERMES_SESSION_USER_ID"),
                    ("chat_name", "HERMES_SESSION_CHAT_NAME"),
                    ("user_name", "HERMES_SESSION_USER_NAME"),
                ):
                    val = get_session_env(env_key)
                    if val:
                        origin[opt_key] = str(val)
                store.set_meta("last_active_origin", origin)
        except Exception as exc:
            logger.debug("on_session_start origin capture failed: %s", exc)

    def _on_pre_gateway_dispatch(**kwargs: Any) -> None:
        event = kwargs.get("event")
        source = getattr(event, "source", None)
        if source is not None and callable(getattr(source, "to_dict", None)):
            try:
                origin = source.to_dict()
                if isinstance(origin, dict) and origin.get("platform") and origin.get("chat_id"):
                    store.set_meta("last_active_origin", origin)
            except Exception as exc:
                logger.debug("pre_gateway_dispatch origin capture failed: %s", exc)
        return None

    ctx.register_hook("on_session_start", _on_session_start)
    ctx.register_hook("pre_gateway_dispatch", _on_pre_gateway_dispatch)

    if hasattr(ctx, "register_skill"):
        skills_dir = Path(__file__).resolve().parent / "skills"
        for skill_name, desc in (
            ("cortex-routing", "Use when operating as Herald to route tasks and handle Cortex escalations."),
            ("cortex-monitoring", "Use when adding, tuning, or testing Cortex watchers, probes, and System 1 rules."),
        ):
            skill_md = skills_dir / skill_name / "SKILL.md"
            if skill_md.is_file():
                ctx.register_skill(skill_name, skill_md, description=desc)

    daemon_profile = str(ctx.get_config("daemon_profile", "herald"))
    if runtime.should_start_daemon(daemon_profile=daemon_profile):
        runtime.start()
        ctx.on_unload(runtime.stop)
