"""Dynamic hot-reloaded watchers (log_tail, http_poll, webhook_route, command_poll)."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import re
from pathlib import Path
from typing import Any, Awaitable, Callable, Iterable, Optional

import httpx

from .rules import CortexEvent
from .store import CortexStore, WatcherRecord

logger = logging.getLogger("cortex.watchers")

VALID_WATCHER_TYPES = ("log_tail", "http_poll", "webhook_route", "command_poll")
_ROUTE_NAME_RE = re.compile(r"^[a-z0-9][a-z0-9_-]{0,63}$")
_MIN_INTERVAL_S = 5.0


def validate_watcher_config(
    watcher_type: str,
    raw_config: dict[str, Any],
    *,
    probes_dir: Path,
    allowed_log_roots: Iterable[Path],
    allow_arbitrary_log_paths: bool = False,
    require_probe_exists: bool = False,
) -> dict[str, Any]:
    """Validate and normalize a watcher definition; raises ValueError on unsafe config."""
    wtype = str(watcher_type or "").strip().lower()
    if wtype not in VALID_WATCHER_TYPES:
        raise ValueError(f"watcher type must be one of {VALID_WATCHER_TYPES}")
    if not isinstance(raw_config, dict):
        raise ValueError("watcher config must be an object")

    source = str(raw_config.get("source") or "").strip()

    if wtype == "log_tail":
        raw_path = str(raw_config.get("path") or "").strip()
        if not raw_path:
            raise ValueError("log_tail requires 'path'")
        resolved = Path(raw_path).expanduser().resolve(strict=False)
        if not allow_arbitrary_log_paths:
            roots = [Path(r).expanduser().resolve(strict=False) for r in allowed_log_roots]
            if not any(resolved == root or resolved.is_relative_to(root) for root in roots):
                raise ValueError(
                    f"log_tail path {resolved} is outside allowed_log_roots {[str(r) for r in roots]}"
                )
        regex_prefilter = raw_config.get("regex_prefilter")
        if regex_prefilter:
            try:
                re.compile(str(regex_prefilter))
            except re.error as exc:
                raise ValueError(f"invalid regex_prefilter: {exc}") from exc
        else:
            regex_prefilter = None
        batch_window_ms = max(50, min(10000, int(raw_config.get("batch_window_ms", 500))))
        max_batch_lines = max(1, min(200, int(raw_config.get("max_batch_lines", 20))))
        return {
            "source": source or f"log:{resolved.name}",
            "path": str(resolved),
            "regex_prefilter": regex_prefilter,
            "from_end": bool(raw_config.get("from_end", True)),
            "batch_window_ms": batch_window_ms,
            "max_batch_lines": max_batch_lines,
        }

    if wtype == "http_poll":
        url = str(raw_config.get("url") or "").strip()
        if not (url.startswith("http://") or url.startswith("https://")):
            raise ValueError("http_poll 'url' must start with http:// or https://")
        method = str(raw_config.get("method") or "GET").strip().upper()
        if method not in ("GET", "POST", "HEAD"):
            raise ValueError("http_poll 'method' must be GET, POST, or HEAD")
        interval_s = max(_MIN_INTERVAL_S, float(raw_config.get("interval_seconds", 30)))
        emit_on = str(raw_config.get("emit_on") or "status_error").strip().lower()
        if emit_on not in ("always", "status_error", "json_change"):
            raise ValueError("http_poll 'emit_on' must be always, status_error, or json_change")
        headers = raw_config.get("headers") or {}
        if not isinstance(headers, dict):
            raise ValueError("http_poll 'headers' must be an object")
        return {
            "source": source or f"http:{url}",
            "url": url,
            "method": method,
            "headers": {str(k): str(v) for k, v in headers.items()},
            "interval_seconds": interval_s,
            "timeout_seconds": max(1.0, min(30.0, float(raw_config.get("timeout_seconds", 5.0)))),
            "emit_on": emit_on,
        }

    if wtype == "webhook_route":
        route_name = str(raw_config.get("route_name") or "").strip().lower()
        if not _ROUTE_NAME_RE.fullmatch(route_name):
            raise ValueError("webhook_route 'route_name' must match ^[a-z0-9][a-z0-9_-]{0,63}$")
        out: dict[str, Any] = {
            "source": source or f"webhook:{route_name}",
            "route_name": route_name,
        }
        if raw_config.get("secret_env"):
            out["secret_env"] = str(raw_config["secret_env"]).strip()
        if raw_config.get("secret"):
            out["secret"] = str(raw_config["secret"])
        return out

    # command_poll
    raw_script = str(raw_config.get("script_path") or "").strip()
    if not raw_script:
        raise ValueError("command_poll requires 'script_path'")
    probes_root = Path(probes_dir).expanduser().resolve(strict=False)
    candidate = Path(raw_script).expanduser()
    if not candidate.is_absolute():
        candidate = probes_root / candidate
    resolved_script = candidate.resolve(strict=False)
    if resolved_script == probes_root or not resolved_script.is_relative_to(probes_root):
        raise ValueError(f"command_poll script_path must reside inside {probes_root}")
    if require_probe_exists and not resolved_script.is_file():
        raise ValueError(f"command_poll probe script does not exist: {resolved_script}")
    args = raw_config.get("args") or []
    if not isinstance(args, list):
        raise ValueError("command_poll 'args' must be a list of strings")
    return {
        "source": source or f"probe:{resolved_script.name}",
        "script_path": str(resolved_script),
        "args": [str(a) for a in args],
        "interval_seconds": max(_MIN_INTERVAL_S, float(raw_config.get("interval_seconds", 30))),
        "timeout_seconds": max(1.0, min(60.0, float(raw_config.get("timeout_seconds", 10.0)))),
        "emit_on_empty": bool(raw_config.get("emit_on_empty", False)),
    }


EventCallback = Callable[[CortexEvent], Awaitable[None]]


class WatcherManager:
    """Reconciles and runs active watchers from CortexStore with live hot-reload."""

    def __init__(
        self,
        *,
        store: CortexStore,
        on_event: EventCallback,
        probes_dir: Path,
        reconcile_interval_s: float = 1.5,
    ):
        self.store = store
        self.on_event = on_event
        self.probes_dir = Path(probes_dir).expanduser().resolve(strict=False)
        self.reconcile_interval_s = max(0.25, float(reconcile_interval_s))
        self._tasks: dict[str, tuple[float, asyncio.Task[Any]]] = {}
        self._stop_event = asyncio.Event()
        self._wake_event = asyncio.Event()
        self._reconcile_task: Optional[asyncio.Task[Any]] = None
        self._last_watermark: float = -1.0

    def trigger_reconcile(self) -> None:
        self._wake_event.set()

    async def start(self) -> None:
        self._stop_event.clear()
        await self.reconcile_once(force=True)
        self._reconcile_task = asyncio.create_task(self._reconcile_loop(), name="cortex:reconcile")

    async def stop(self) -> None:
        self._stop_event.set()
        self._wake_event.set()
        if self._reconcile_task is not None:
            self._reconcile_task.cancel()
            await asyncio.gather(self._reconcile_task, return_exceptions=True)
            self._reconcile_task = None
        tasks = [task for _, task in self._tasks.values()]
        self._tasks.clear()
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _reconcile_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                await asyncio.wait_for(self._wake_event.wait(), timeout=self.reconcile_interval_s)
            except asyncio.TimeoutError:
                pass
            self._wake_event.clear()
            if self._stop_event.is_set():
                break
            try:
                await self.reconcile_once()
            except Exception:
                logger.exception("WatcherManager reconcile error")

    async def reconcile_once(self, *, force: bool = False) -> None:
        watermark = self.store.get_watchers_watermark()
        if not force and watermark == self._last_watermark:
            return
        self._last_watermark = watermark

        active = {
            w.id: w
            for w in self.store.list_watchers(enabled_only=True)
            if w.type in ("log_tail", "http_poll", "command_poll")
        }

        # Cancel removed/disabled/updated watchers
        to_remove = []
        for wid, (updated_at, task) in self._tasks.items():
            rec = active.get(wid)
            if rec is None or rec.updated_at != updated_at or task.done():
                task.cancel()
                to_remove.append(wid)
        for wid in to_remove:
            _, task = self._tasks.pop(wid)
            await asyncio.gather(task, return_exceptions=True)

        # Spawn new/updated watchers
        for wid, rec in active.items():
            if wid in self._tasks:
                continue
            coro = self._make_runner(rec)
            if coro is not None:
                task = asyncio.create_task(coro, name=f"cortex:watcher:{wid}")
                self._tasks[wid] = (rec.updated_at, task)

    def _make_runner(self, rec: WatcherRecord) -> Optional[Awaitable[None]]:
        if rec.type == "log_tail":
            return self._run_log_tail(rec)
        if rec.type == "http_poll":
            return self._run_http_poll(rec)
        if rec.type == "command_poll":
            return self._run_command_poll(rec)
        return None

    async def _run_log_tail(self, rec: WatcherRecord) -> None:
        cfg = rec.config
        path = Path(cfg["path"])
        source = cfg.get("source") or f"log:{path.name}"
        pattern = re.compile(cfg["regex_prefilter"]) if cfg.get("regex_prefilter") else None
        from_end = bool(cfg.get("from_end", True))
        batch_window_s = max(0.05, float(cfg.get("batch_window_ms", 500)) / 1000.0)
        max_batch_lines = max(1, int(cfg.get("max_batch_lines", 20)))

        inode: Optional[tuple[int, int]] = None
        offset = 0
        initialized = False
        pending: list[str] = []
        batch_started_at: Optional[float] = None
        loop = asyncio.get_running_loop()

        async def _flush() -> None:
            nonlocal pending, batch_started_at
            if not pending:
                return
            lines = pending
            pending = []
            batch_started_at = None
            await self.on_event(
                CortexEvent(
                    source=source,
                    payload="\n".join(lines),
                    metadata={"watcher_id": rec.id, "line_count": len(lines), "path": str(path)},
                )
            )

        while not self._stop_event.is_set():
            try:
                if path.is_file():
                    st = path.stat()
                    cur_inode = (int(st.st_dev), int(st.st_ino))
                    if not initialized:
                        inode = cur_inode
                        offset = st.st_size if from_end else 0
                        initialized = True
                    elif cur_inode != inode or st.st_size < offset:
                        # Log rotated or truncated
                        inode = cur_inode
                        offset = 0

                    if st.st_size > offset:
                        with path.open("r", encoding="utf-8", errors="replace") as fh:
                            fh.seek(offset)
                            chunk = fh.read()
                            offset = fh.tell()
                        for line in chunk.splitlines():
                            if pattern is not None and not pattern.search(line):
                                continue
                            if not pending:
                                batch_started_at = loop.time()
                            pending.append(line)
                            if len(pending) >= max_batch_lines:
                                await _flush()
            except Exception as exc:
                logger.warning("log_tail %s error: %s", rec.id, exc)

            if pending and batch_started_at is not None:
                if (loop.time() - batch_started_at) >= batch_window_s:
                    await _flush()

            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=0.25)
            except asyncio.TimeoutError:
                pass

    async def _run_http_poll(self, rec: WatcherRecord) -> None:
        cfg = rec.config
        url = cfg["url"]
        method = cfg.get("method", "GET")
        headers = cfg.get("headers") or {}
        interval_s = max(_MIN_INTERVAL_S, float(cfg.get("interval_seconds", 30)))
        timeout_s = max(1.0, float(cfg.get("timeout_seconds", 5.0)))
        emit_on = cfg.get("emit_on", "status_error")
        source = cfg.get("source") or f"http:{url}"
        last_digest: Optional[str] = None

        async with httpx.AsyncClient(timeout=timeout_s) as client:
            while not self._stop_event.is_set():
                try:
                    resp = await client.request(method, url, headers=headers)
                    body_text = resp.text[:4000]
                    digest = hashlib.sha256(body_text.encode("utf-8", errors="replace")).hexdigest()
                    should_emit = (
                        emit_on == "always"
                        or (emit_on == "status_error" and resp.status_code >= 400)
                        or (emit_on == "json_change" and last_digest is not None and digest != last_digest)
                    )
                    last_digest = digest
                    if should_emit:
                        await self.on_event(
                            CortexEvent(
                                source=source,
                                payload={
                                    "url": url,
                                    "status_code": resp.status_code,
                                    "body": body_text,
                                },
                                metadata={"watcher_id": rec.id},
                            )
                        )
                except Exception as exc:
                    await self.on_event(
                        CortexEvent(
                            source=source,
                            payload={"url": url, "error": f"{type(exc).__name__}: {exc}"},
                            metadata={"watcher_id": rec.id},
                        )
                    )
                try:
                    await asyncio.wait_for(self._stop_event.wait(), timeout=interval_s)
                except asyncio.TimeoutError:
                    pass

    async def _run_command_poll(self, rec: WatcherRecord) -> None:
        cfg = rec.config
        script = Path(cfg["script_path"]).resolve(strict=False)
        args = [str(a) for a in (cfg.get("args") or [])]
        interval_s = max(_MIN_INTERVAL_S, float(cfg.get("interval_seconds", 30)))
        timeout_s = max(1.0, float(cfg.get("timeout_seconds", 10.0)))
        emit_on_empty = bool(cfg.get("emit_on_empty", False))
        source = cfg.get("source") or f"probe:{script.name}"

        while not self._stop_event.is_set():
            try:
                if not script.is_relative_to(self.probes_dir) or not script.is_file():
                    logger.warning("command_poll %s skipped invalid probe path: %s", rec.id, script)
                else:
                    cmd = [str(script), *args]
                    if script.suffix == ".py" and not os.access(script, os.X_OK):
                        import sys
                        cmd = [sys.executable, str(script), *args]
                    proc = await asyncio.create_subprocess_exec(
                        *cmd,
                        stdout=asyncio.subprocess.PIPE,
                        stderr=asyncio.subprocess.PIPE,
                    )
                    try:
                        stdout_b, stderr_b = await asyncio.wait_for(proc.communicate(), timeout=timeout_s)
                        stdout = stdout_b.decode("utf-8", errors="replace").strip()
                        stderr = stderr_b.decode("utf-8", errors="replace").strip()
                        if stdout or stderr or proc.returncode != 0 or emit_on_empty:
                            payload: Any
                            try:
                                payload = json.loads(stdout) if stdout.startswith(("{", "[")) else stdout
                            except ValueError:
                                payload = stdout
                            await self.on_event(
                                CortexEvent(
                                    source=source,
                                    payload={
                                        "returncode": proc.returncode,
                                        "output": payload,
                                        "stderr": stderr[:1000] if stderr else "",
                                    },
                                    metadata={"watcher_id": rec.id},
                                )
                            )
                    except asyncio.TimeoutError:
                        proc.kill()
                        await proc.wait()
                        await self.on_event(
                            CortexEvent(
                                source=source,
                                payload={"error": f"probe timed out after {timeout_s}s"},
                                metadata={"watcher_id": rec.id},
                            )
                        )
            except Exception as exc:
                logger.warning("command_poll %s error: %s", rec.id, exc)

            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=interval_s)
            except asyncio.TimeoutError:
                pass
