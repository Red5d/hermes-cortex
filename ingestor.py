"""Dynamic HTTP webhook ingestor for Cortex (/health, /ingest/<route_name>)."""

from __future__ import annotations

import hashlib
import hmac
import json
import logging
from typing import Any, Callable, Optional

from aiohttp import web

from .rules import CortexEvent
from .store import CortexStore, WatcherRecord
from .watchers import EventCallback

logger = logging.getLogger("cortex.ingestor")

_MAX_BODY_BYTES = 262_144


class WebhookIngestor:
    """Lightweight aiohttp listener serving dynamically registered webhook_route watchers."""

    def __init__(
        self,
        *,
        store: CortexStore,
        on_event: EventCallback,
        host: str = "127.0.0.1",
        port: int = 8655,
        secret_lookup: Optional[Callable[[str], Optional[str]]] = None,
    ):
        self.store = store
        self.on_event = on_event
        self.host = host
        self.port = int(port)
        self.secret_lookup = secret_lookup
        self._runner: Optional[web.AppRunner] = None
        self._site: Optional[web.TCPSite] = None
        self.bound_port: Optional[int] = None

    def _find_route(self, route_name: str) -> Optional[WatcherRecord]:
        for w in self.store.list_watchers(enabled_only=True):
            if w.type == "webhook_route" and w.config.get("route_name") == route_name:
                return w
        return None

    def _verify_signature(self, request: web.Request, body: bytes, watcher: WatcherRecord) -> bool:
        secret_env = str(watcher.config.get("secret_env") or "").strip()
        inline_secret = str(watcher.config.get("secret") or "")
        secret: Optional[str] = None
        if secret_env:
            if self.secret_lookup is not None:
                secret = self.secret_lookup(secret_env)
            if not secret:
                logger.warning(
                    "Webhook route %r requires secret_env %r but it is unset/empty; failing closed",
                    watcher.config.get("route_name"),
                    secret_env,
                )
                return False
        elif inline_secret:
            secret = inline_secret
        else:
            return True

        sig_header = (
            request.headers.get("X-Hub-Signature-256")
            or request.headers.get("X-Cortex-Signature")
            or ""
        ).strip()
        if not sig_header:
            return False
        if sig_header.startswith("sha256="):
            sig_header = sig_header[7:]
        expected = hmac.new(secret.encode("utf-8"), body, hashlib.sha256).hexdigest()
        return hmac.compare_digest(expected, sig_header.lower())

    async def _handle_health(self, _request: web.Request) -> web.Response:
        watchers = self.store.list_watchers()
        rules = self.store.list_rules()
        return web.json_response(
            {
                "status": "ok",
                "watchers_total": len(watchers),
                "watchers_enabled": sum(1 for w in watchers if w.enabled),
                "rules_total": len(rules),
                "rules_enabled": sum(1 for r in rules if r.enabled),
            }
        )

    async def _handle_ingest(self, request: web.Request) -> web.Response:
        route_name = request.match_info["route_name"].strip().lower()
        watcher = self._find_route(route_name)
        if watcher is None:
            return web.json_response({"error": f"unknown webhook route: {route_name}"}, status=404)

        raw_body = await request.read()
        if len(raw_body) > _MAX_BODY_BYTES:
            return web.json_response({"error": "payload too large"}, status=413)

        if not self._verify_signature(request, raw_body, watcher):
            return web.json_response({"error": "invalid signature"}, status=401)

        text = raw_body.decode("utf-8", errors="replace")
        payload: Any
        try:
            payload = json.loads(text) if text else {}
        except ValueError:
            payload = text

        source = watcher.config.get("source") or f"webhook:{route_name}"
        await self.on_event(
            CortexEvent(
                source=source,
                payload=payload,
                metadata={"watcher_id": watcher.id, "route_name": route_name},
            )
        )
        return web.json_response({"status": "accepted", "source": source}, status=202)

    async def start(self) -> None:
        app = web.Application(client_max_size=_MAX_BODY_BYTES + 1024)
        app.router.add_get("/health", self._handle_health)
        app.router.add_post("/ingest/{route_name}", self._handle_ingest)
        self._runner = web.AppRunner(app, access_log=None)
        await self._runner.setup()
        self._site = web.TCPSite(self._runner, self.host, self.port)
        await self._site.start()
        sockets = getattr(self._site._server, "sockets", None) or []
        if sockets:
            self.bound_port = sockets[0].getsockname()[1]
        else:
            self.bound_port = self.port
        logger.info("Cortex HTTP ingestor listening on %s:%s", self.host, self.bound_port)

    async def stop(self) -> None:
        if self._runner is not None:
            await self._runner.cleanup()
            self._runner = None
            self._site = None
            self.bound_port = None
