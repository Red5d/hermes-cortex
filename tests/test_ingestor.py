"""Tests for WebhookIngestor dynamic routing and HMAC verification."""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import tempfile
import unittest
from pathlib import Path

import httpx

from cortex.ingestor import WebhookIngestor
from cortex.rules import CortexEvent
from cortex.store import CortexStore


class TestWebhookIngestor(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.store = CortexStore(Path(self.tmp.name) / "cortex.db")

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def test_dynamic_route_and_hmac(self) -> None:
        received: list[CortexEvent] = []

        async def on_event(ev: CortexEvent) -> None:
            received.append(ev)

        async def run() -> None:
            ingestor = WebhookIngestor(
                store=self.store,
                on_event=on_event,
                host="127.0.0.1",
                port=0,
                secret_lookup=lambda name: "super-secret" if name == "HOOK_SECRET" else None,
            )
            await ingestor.start()
            try:
                base = f"http://127.0.0.1:{ingestor.bound_port}"
                async with httpx.AsyncClient() as client:
                    h = await client.get(f"{base}/health")
                    self.assertEqual(h.status_code, 200)

                    missing = await client.post(f"{base}/ingest/alerts", json={"msg": "hi"})
                    self.assertEqual(missing.status_code, 404)

                    # Register route dynamically without restarting ingestor
                    self.store.upsert_watcher(
                        watcher_id="w_alerts",
                        watcher_type="webhook_route",
                        config={"route_name": "alerts", "source": "beszel", "secret_env": "HOOK_SECRET"},
                    )

                    bad_sig = await client.post(f"{base}/ingest/alerts", json={"msg": "hi"})
                    self.assertEqual(bad_sig.status_code, 401)

                    body = b'{"cpu":99}'
                    sig = "sha256=" + hmac.new(b"super-secret", body, hashlib.sha256).hexdigest()
                    ok = await client.post(
                        f"{base}/ingest/alerts",
                        content=body,
                        headers={"Content-Type": "application/json", "X-Hub-Signature-256": sig},
                    )
                    self.assertEqual(ok.status_code, 202)
                    self.assertEqual(len(received), 1)
                    self.assertEqual(received[0].source, "beszel")
                    self.assertEqual(received[0].payload, {"cpu": 99})

                    # Fail-closed when secret_env is configured but unset
                    self.store.upsert_watcher(
                        watcher_id="w_unset",
                        watcher_type="webhook_route",
                        config={"route_name": "unset_secret", "source": "s2", "secret_env": "MISSING_SECRET"},
                    )
                    denied = await client.post(f"{base}/ingest/unset_secret", json={"msg": "hi"})
                    self.assertEqual(denied.status_code, 401)
            finally:
                await ingestor.stop()

        asyncio.run(run())


if __name__ == "__main__":
    unittest.main()
