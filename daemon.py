"""Single-instance background runtime for Cortex watchers, HTTP ingestor, and escalations."""

from __future__ import annotations

import asyncio
import fcntl
import logging
import os
import shutil
import threading
from pathlib import Path
from typing import Any, Callable, Optional

from .decision import DecisionClient
from .ingestor import WebhookIngestor
from .rules import CortexEvent, evaluate_event, format_escalation_alert
from .store import CortexStore
from .watchers import WatcherManager

logger = logging.getLogger("cortex.daemon")


def resolve_secret_from_scope(env_name: str) -> Optional[str]:
    """Read a secret via Hermes's fail-closed secret_scope without touching os.environ directly."""
    try:
        from agent.secret_scope import get_secret

        return get_secret(env_name)
    except Exception:
        return os.environ.get(env_name)


def _spawn_daemon_thread(target: Callable[[], None], *, name: str) -> threading.Thread:
    """Use Hermes's spawn_context_thread when available so profile scope stays bound."""
    try:
        from agent.memory_provider import spawn_context_thread

        return spawn_context_thread(target, name=name, daemon=True)
    except Exception:
        t = threading.Thread(target=target, name=name, daemon=True)
        t.start()
        return t


class CortexRuntime:
    """Owns the single-instance async loop running WatcherManager and WebhookIngestor."""

    def __init__(
        self,
        *,
        ctx: Any,
        store: CortexStore,
        decision_client: DecisionClient,
        probes_dir: Path,
        lock_path: Path,
        listen_host: str = "127.0.0.1",
        listen_port: int = 8655,
        reconcile_interval_s: float = 1.5,
        escalation_origins: Optional[list[dict[str, Any]]] = None,
        kanban_fallback: bool = True,
        kanban_assignee: str = "herald",
    ):
        self.ctx = ctx
        self.store = store
        self.decision_client = decision_client
        self.probes_dir = Path(probes_dir)
        self.lock_path = Path(lock_path)
        self.listen_host = listen_host
        self.listen_port = int(listen_port)
        self.reconcile_interval_s = float(reconcile_interval_s)
        self.escalation_origins = list(escalation_origins or [])
        self.kanban_fallback = bool(kanban_fallback)
        self.kanban_assignee = str(kanban_assignee or "herald")

        self._lock_fh: Optional[Any] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._thread: Optional[threading.Thread] = None
        self._ready = threading.Event()
        self._watcher_manager: Optional[WatcherManager] = None
        self._ingestor: Optional[WebhookIngestor] = None
        self._stopped = False

    def should_start_daemon(self, *, daemon_profile: str) -> bool:
        if os.environ.get("HERMES_KANBAN_TASK"):
            return False
        if os.environ.get("HERMES_CORTEX_NO_DAEMON"):
            return False
        if os.environ.get("HERMES_SUBAGENT"):
            return False
        profile = getattr(self.ctx, "profile_name", "default")
        return profile == daemon_profile

    def _acquire_lock(self) -> bool:
        self.lock_path.parent.mkdir(parents=True, exist_ok=True)
        fh = self.lock_path.open("a+")
        try:
            fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            fh.seek(0)
            fh.truncate()
            fh.write(str(os.getpid()))
            fh.flush()
            self._lock_fh = fh
            return True
        except OSError:
            fh.close()
            return False

    def _release_lock(self) -> None:
        if self._lock_fh is not None:
            try:
                fcntl.flock(self._lock_fh.fileno(), fcntl.LOCK_UN)
            except OSError:
                pass
            try:
                self._lock_fh.close()
            except OSError:
                pass
            self._lock_fh = None

    def start(self) -> bool:
        if self._thread is not None:
            return True
        if not self._acquire_lock():
            logger.info("Cortex daemon lock already held by another process; running tools-only.")
            return False
        self._stopped = False
        self._ready.clear()
        self._thread = _spawn_daemon_thread(self._thread_main, name="cortex:daemon")
        self._ready.wait(timeout=5.0)
        return True

    def notify_watchers_changed(self) -> None:
        if self._loop is not None and self._watcher_manager is not None and not self._loop.is_closed():
            self._loop.call_soon_threadsafe(self._watcher_manager.trigger_reconcile)

    def stop(self) -> None:
        if self._stopped:
            return
        self._stopped = True
        loop = self._loop
        if loop is not None and not loop.is_closed():
            fut = asyncio.run_coroutine_threadsafe(self._async_stop(), loop)
            try:
                fut.result(timeout=5.0)
            except Exception:
                pass
            loop.call_soon_threadsafe(loop.stop)
        if self._thread is not None and self._thread.is_alive():
            self._thread.join(timeout=5.0)
        self._thread = None
        self._release_lock()

    def _thread_main(self) -> None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        self._loop = loop
        try:
            loop.run_until_complete(self._async_start())
            self._ready.set()
            loop.run_forever()
        except Exception:
            logger.exception("Cortex daemon loop crashed")
            self._ready.set()
        finally:
            try:
                loop.run_until_complete(self._async_stop())
            except Exception:
                pass
            loop.close()
            self._loop = None

    async def _async_start(self) -> None:
        self.probes_dir.mkdir(parents=True, exist_ok=True)
        self._watcher_manager = WatcherManager(
            store=self.store,
            on_event=self.handle_event,
            probes_dir=self.probes_dir,
            reconcile_interval_s=self.reconcile_interval_s,
        )
        self._ingestor = WebhookIngestor(
            store=self.store,
            on_event=self.handle_event,
            host=self.listen_host,
            port=self.listen_port,
            secret_lookup=resolve_secret_from_scope,
        )
        await self._watcher_manager.start()
        await self._ingestor.start()

    async def _async_stop(self) -> None:
        if self._watcher_manager is not None:
            await self._watcher_manager.stop()
            self._watcher_manager = None
        if self._ingestor is not None:
            await self._ingestor.stop()
            self._ingestor = None
        await self.decision_client.aclose()

    async def handle_event(self, event: CortexEvent) -> None:
        outcome = await evaluate_event(event, self.store, self.decision_client, dry_run=False)
        self.store.increment_metric(outcome.action, 1)

        if outcome.action == "discard":
            return

        self.store.record_event(
            source=outcome.source,
            rule_id=outcome.winning_rule_id,
            decision=outcome.action,
            excerpt=outcome.excerpt,
            metadata=outcome.to_dict(),
            ts=outcome.ts,
        )

        if outcome.action == "escalate":
            await self._deliver_escalation(outcome)

    async def _deliver_escalation(self, outcome: Any) -> None:
        alert_text = format_escalation_alert(outcome)
        targets: list[Optional[dict[str, Any]]] = []
        last_origin = self.store.get_meta("last_active_origin")
        if isinstance(last_origin, dict) and last_origin.get("platform"):
            targets.append(last_origin)
        for origin in self.escalation_origins:
            if isinstance(origin, dict) and origin.get("platform"):
                targets.append(origin)
        if not targets:
            targets.append(None)

        delivered = False
        for origin in targets:
            try:
                if self.ctx.inject_message(alert_text, role="user", origin=origin):
                    delivered = True
                    break
            except Exception as exc:
                logger.warning("inject_message failed for origin=%s: %s", origin, exc)

        if delivered:
            self.store.increment_metric("escalation_delivered", 1)
            return

        self.store.increment_metric("escalation_fallback", 1)
        if self.kanban_fallback:
            await self._create_kanban_fallback(outcome, alert_text)

    async def _create_kanban_fallback(self, outcome: Any, alert_text: str) -> None:
        hermes_bin = shutil.which("hermes")
        if not hermes_bin:
            logger.info("Kanban fallback skipped: 'hermes' binary not found on PATH")
            return
        assignee = "analyst" if outcome.auto_investigate else self.kanban_assignee
        title = f"[Cortex] {outcome.severity.upper()}: {outcome.winning_rule_name} ({outcome.source})"
        try:
            proc = await asyncio.create_subprocess_exec(
                hermes_bin,
                "kanban",
                "create",
                "--title",
                title[:120],
                "--assignee",
                assignee,
                "--body",
                alert_text,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            await asyncio.wait_for(proc.communicate(), timeout=10.0)
        except Exception as exc:
            logger.warning("Kanban fallback creation failed: %s", exc)
