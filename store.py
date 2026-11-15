"""Cross-profile SQLite WAL store for Cortex watchers, rules, logs, and metadata."""

from __future__ import annotations

import json
import sqlite3
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator, Optional


@dataclass(frozen=True)
class WatcherRecord:
    id: str
    type: str
    config: dict[str, Any]
    enabled: bool
    updated_at: float


@dataclass(frozen=True)
class RuleRecord:
    id: str
    name: str
    source: str
    condition: str
    severity: str
    cooldown_seconds: int
    force_escalate_regex: Optional[str]
    auto_investigate: bool
    enabled: bool
    updated_at: float


def _json_dumps(value: Any) -> str:
    return json.dumps(value, separators=(",", ":"), sort_keys=True)


class CortexStore:
    """Thread-safe and cross-process SQLite (WAL) persistence for Cortex."""

    def __init__(self, db_path: str | Path):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_schema()

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(str(self.db_path), timeout=10.0, isolation_level=None)
        conn.row_factory = sqlite3.Row
        try:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA busy_timeout=5000")
            yield conn
        finally:
            conn.close()

    def _init_schema(self) -> None:
        with self._connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS watchers (
                    id TEXT PRIMARY KEY,
                    type TEXT NOT NULL,
                    config_json TEXT NOT NULL,
                    enabled INTEGER NOT NULL DEFAULT 1,
                    updated_at REAL NOT NULL
                );

                CREATE TABLE IF NOT EXISTS rules (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    source TEXT NOT NULL,
                    condition TEXT NOT NULL,
                    severity TEXT NOT NULL DEFAULT 'medium',
                    cooldown_seconds INTEGER NOT NULL DEFAULT 300,
                    force_escalate_regex TEXT,
                    auto_investigate INTEGER NOT NULL DEFAULT 0,
                    enabled INTEGER NOT NULL DEFAULT 1,
                    updated_at REAL NOT NULL
                );

                CREATE TABLE IF NOT EXISTS event_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts REAL NOT NULL,
                    source TEXT NOT NULL,
                    rule_id TEXT,
                    decision TEXT NOT NULL,
                    excerpt TEXT NOT NULL,
                    metadata_json TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_event_log_ts ON event_log (ts DESC);
                CREATE INDEX IF NOT EXISTS idx_event_log_source ON event_log (source, ts DESC);

                CREATE TABLE IF NOT EXISTS cooldowns (
                    rule_key TEXT PRIMARY KEY,
                    last_fired_at REAL NOT NULL
                );

                CREATE TABLE IF NOT EXISTS meta (
                    key TEXT PRIMARY KEY,
                    value_json TEXT NOT NULL,
                    updated_at REAL NOT NULL
                );
                """
            )

    # ── Watchers ──────────────────────────────────────────────────────────────

    def _bump_watermark(self, conn: sqlite3.Connection, now: float) -> None:
        conn.execute(
            """
            INSERT INTO meta (key, value_json, updated_at)
            VALUES ('watchers_watermark', ?, ?)
            ON CONFLICT(key) DO UPDATE SET value_json=excluded.value_json, updated_at=excluded.updated_at
            """,
            (_json_dumps(now), now),
        )

    def get_watchers_watermark(self) -> float:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT value_json FROM meta WHERE key = 'watchers_watermark'"
            ).fetchone()
        if not row:
            return 0.0
        try:
            return float(json.loads(row["value_json"]))
        except (TypeError, ValueError):
            return 0.0

    def upsert_watcher(
        self,
        *,
        watcher_id: Optional[str],
        watcher_type: str,
        config: dict[str, Any],
        enabled: bool = True,
    ) -> WatcherRecord:
        wid = (watcher_id or "").strip() or f"w_{uuid.uuid4().hex[:10]}"
        now = time.time()
        with self._connect() as conn:
            conn.execute("BEGIN")
            try:
                conn.execute(
                    """
                    INSERT INTO watchers (id, type, config_json, enabled, updated_at)
                    VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT(id) DO UPDATE SET
                        type=excluded.type,
                        config_json=excluded.config_json,
                        enabled=excluded.enabled,
                        updated_at=excluded.updated_at
                    """,
                    (wid, watcher_type, _json_dumps(config), 1 if enabled else 0, now),
                )
                self._bump_watermark(conn, now)
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise
        return WatcherRecord(
            id=wid, type=watcher_type, config=dict(config), enabled=bool(enabled), updated_at=now
        )

    def get_watcher(self, watcher_id: str) -> Optional[WatcherRecord]:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT id, type, config_json, enabled, updated_at FROM watchers WHERE id = ?",
                (watcher_id,),
            ).fetchone()
        return self._row_to_watcher(row) if row else None

    def list_watchers(self, *, enabled_only: bool = False) -> list[WatcherRecord]:
        sql = "SELECT id, type, config_json, enabled, updated_at FROM watchers"
        if enabled_only:
            sql += " WHERE enabled = 1"
        sql += " ORDER BY id ASC"
        with self._connect() as conn:
            rows = conn.execute(sql).fetchall()
        return [self._row_to_watcher(r) for r in rows]

    def set_watcher_enabled(self, watcher_id: str, enabled: Optional[bool] = None) -> Optional[WatcherRecord]:
        now = time.time()
        with self._connect() as conn:
            conn.execute("BEGIN")
            try:
                row = conn.execute(
                    "SELECT id, type, config_json, enabled FROM watchers WHERE id = ?",
                    (watcher_id,),
                ).fetchone()
                if not row:
                    conn.execute("ROLLBACK")
                    return None
                new_enabled = (not bool(row["enabled"])) if enabled is None else bool(enabled)
                conn.execute(
                    "UPDATE watchers SET enabled = ?, updated_at = ? WHERE id = ?",
                    (1 if new_enabled else 0, now, watcher_id),
                )
                self._bump_watermark(conn, now)
                conn.execute("COMMIT")
                return WatcherRecord(
                    id=row["id"],
                    type=row["type"],
                    config=json.loads(row["config_json"]),
                    enabled=new_enabled,
                    updated_at=now,
                )
            except Exception:
                conn.execute("ROLLBACK")
                raise

    def delete_watcher(self, watcher_id: str) -> bool:
        now = time.time()
        with self._connect() as conn:
            conn.execute("BEGIN")
            try:
                cur = conn.execute("DELETE FROM watchers WHERE id = ?", (watcher_id,))
                deleted = cur.rowcount > 0
                if deleted:
                    self._bump_watermark(conn, now)
                conn.execute("COMMIT")
                return deleted
            except Exception:
                conn.execute("ROLLBACK")
                raise

    @staticmethod
    def _row_to_watcher(row: sqlite3.Row) -> WatcherRecord:
        return WatcherRecord(
            id=row["id"],
            type=row["type"],
            config=json.loads(row["config_json"]),
            enabled=bool(row["enabled"]),
            updated_at=float(row["updated_at"]),
        )

    # ── Rules ─────────────────────────────────────────────────────────────────

    def upsert_rule(
        self,
        *,
        rule_id: Optional[str],
        name: str,
        source: str,
        condition: str,
        severity: str = "medium",
        cooldown_seconds: int = 300,
        force_escalate_regex: Optional[str] = None,
        auto_investigate: bool = False,
        enabled: bool = True,
    ) -> RuleRecord:
        rid = (rule_id or "").strip() or f"r_{uuid.uuid4().hex[:10]}"
        now = time.time()
        with self._connect() as conn:
            conn.execute("BEGIN")
            try:
                conn.execute(
                    """
                    INSERT INTO rules (
                        id, name, source, condition, severity, cooldown_seconds,
                        force_escalate_regex, auto_investigate, enabled, updated_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(id) DO UPDATE SET
                        name=excluded.name,
                        source=excluded.source,
                        condition=excluded.condition,
                        severity=excluded.severity,
                        cooldown_seconds=excluded.cooldown_seconds,
                        force_escalate_regex=excluded.force_escalate_regex,
                        auto_investigate=excluded.auto_investigate,
                        enabled=excluded.enabled,
                        updated_at=excluded.updated_at
                    """,
                    (
                        rid,
                        name,
                        source,
                        condition,
                        severity,
                        int(cooldown_seconds),
                        force_escalate_regex,
                        1 if auto_investigate else 0,
                        1 if enabled else 0,
                        now,
                    ),
                )
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise
        return RuleRecord(
            id=rid,
            name=name,
            source=source,
            condition=condition,
            severity=severity,
            cooldown_seconds=int(cooldown_seconds),
            force_escalate_regex=force_escalate_regex,
            auto_investigate=bool(auto_investigate),
            enabled=bool(enabled),
            updated_at=now,
        )

    def get_rule(self, rule_id: str) -> Optional[RuleRecord]:
        with self._connect() as conn:
            row = conn.execute("SELECT * FROM rules WHERE id = ?", (rule_id,)).fetchone()
        return self._row_to_rule(row) if row else None

    def list_rules(self, *, source: Optional[str] = None, enabled_only: bool = False) -> list[RuleRecord]:
        clauses = []
        params: list[Any] = []
        if enabled_only:
            clauses.append("enabled = 1")
        if source is not None:
            clauses.append("(source = ? OR source = '*')")
            params.append(source)
        sql = "SELECT * FROM rules"
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY id ASC"
        with self._connect() as conn:
            rows = conn.execute(sql, params).fetchall()
        return [self._row_to_rule(r) for r in rows]

    def set_rule_enabled(self, rule_id: str, enabled: Optional[bool] = None) -> Optional[RuleRecord]:
        now = time.time()
        with self._connect() as conn:
            conn.execute("BEGIN")
            try:
                row = conn.execute("SELECT * FROM rules WHERE id = ?", (rule_id,)).fetchone()
                if not row:
                    conn.execute("ROLLBACK")
                    return None
                new_enabled = (not bool(row["enabled"])) if enabled is None else bool(enabled)
                conn.execute(
                    "UPDATE rules SET enabled = ?, updated_at = ? WHERE id = ?",
                    (1 if new_enabled else 0, now, rule_id),
                )
                conn.execute("COMMIT")
                rec = self._row_to_rule(row)
                return RuleRecord(
                    id=rec.id,
                    name=rec.name,
                    source=rec.source,
                    condition=rec.condition,
                    severity=rec.severity,
                    cooldown_seconds=rec.cooldown_seconds,
                    force_escalate_regex=rec.force_escalate_regex,
                    auto_investigate=rec.auto_investigate,
                    enabled=new_enabled,
                    updated_at=now,
                )
            except Exception:
                conn.execute("ROLLBACK")
                raise

    def delete_rule(self, rule_id: str) -> bool:
        with self._connect() as conn:
            cur = conn.execute("DELETE FROM rules WHERE id = ?", (rule_id,))
            return cur.rowcount > 0

    @staticmethod
    def _row_to_rule(row: sqlite3.Row) -> RuleRecord:
        return RuleRecord(
            id=row["id"],
            name=row["name"],
            source=row["source"],
            condition=row["condition"],
            severity=row["severity"],
            cooldown_seconds=int(row["cooldown_seconds"]),
            force_escalate_regex=row["force_escalate_regex"],
            auto_investigate=bool(row["auto_investigate"]),
            enabled=bool(row["enabled"]),
            updated_at=float(row["updated_at"]),
        )

    # ── Cooldowns, Event Log & Metadata ───────────────────────────────────────

    def try_acquire_cooldown(
        self, rule_id: str, source: str, cooldown_seconds: int, *, now: Optional[float] = None
    ) -> bool:
        """Return True and update timestamp if outside cooldown window; False if suppressed."""
        if cooldown_seconds <= 0:
            return True
        ts = time.time() if now is None else float(now)
        key = f"{rule_id}::{source}"
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                row = conn.execute(
                    "SELECT last_fired_at FROM cooldowns WHERE rule_key = ?", (key,)
                ).fetchone()
                if row and (ts - float(row["last_fired_at"])) < cooldown_seconds:
                    conn.execute("ROLLBACK")
                    return False
                conn.execute(
                    """
                    INSERT INTO cooldowns (rule_key, last_fired_at)
                    VALUES (?, ?)
                    ON CONFLICT(rule_key) DO UPDATE SET last_fired_at=excluded.last_fired_at
                    """,
                    (key, ts),
                )
                conn.execute("COMMIT")
                return True
            except Exception:
                conn.execute("ROLLBACK")
                raise

    def record_event(
        self,
        *,
        source: str,
        rule_id: Optional[str],
        decision: str,
        excerpt: str,
        metadata: Optional[dict[str, Any]] = None,
        ts: Optional[float] = None,
        max_rows: int = 2000,
    ) -> int:
        now = time.time() if ts is None else float(ts)
        with self._connect() as conn:
            conn.execute("BEGIN")
            try:
                cur = conn.execute(
                    """
                    INSERT INTO event_log (ts, source, rule_id, decision, excerpt, metadata_json)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (now, source, rule_id, decision, excerpt[:4000], _json_dumps(metadata or {})),
                )
                row_id = int(cur.lastrowid or 0)
                if max_rows > 0:
                    conn.execute(
                        """
                        DELETE FROM event_log
                        WHERE id NOT IN (
                            SELECT id FROM event_log ORDER BY id DESC LIMIT ?
                        )
                        """,
                        (max_rows,),
                    )
                conn.execute("COMMIT")
                return row_id
            except Exception:
                conn.execute("ROLLBACK")
                raise

    def query_log(
        self,
        *,
        limit: int = 50,
        source: Optional[str] = None,
        decision: Optional[str] = None,
    ) -> list[dict[str, Any]]:
        limit = max(1, min(int(limit), 500))
        clauses = []
        params: list[Any] = []
        if source:
            clauses.append("source = ?")
            params.append(source)
        if decision:
            clauses.append("decision = ?")
            params.append(decision)
        sql = "SELECT id, ts, source, rule_id, decision, excerpt, metadata_json FROM event_log"
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY id DESC LIMIT ?"
        params.append(limit)
        with self._connect() as conn:
            rows = conn.execute(sql, params).fetchall()
        return [
            {
                "id": r["id"],
                "ts": float(r["ts"]),
                "source": r["source"],
                "rule_id": r["rule_id"],
                "decision": r["decision"],
                "excerpt": r["excerpt"],
                "metadata": json.loads(r["metadata_json"]),
            }
            for r in rows
        ]

    def get_meta(self, key: str, default: Any = None) -> Any:
        with self._connect() as conn:
            row = conn.execute("SELECT value_json FROM meta WHERE key = ?", (key,)).fetchone()
        if not row:
            return default
        try:
            return json.loads(row["value_json"])
        except (TypeError, ValueError):
            return default

    def set_meta(self, key: str, value: Any) -> None:
        now = time.time()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO meta (key, value_json, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(key) DO UPDATE SET value_json=excluded.value_json, updated_at=excluded.updated_at
                """,
                (key, _json_dumps(value), now),
            )

    def increment_metric(self, name: str, delta: int = 1) -> None:
        key = f"metric:{name}"
        now = time.time()
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                row = conn.execute("SELECT value_json FROM meta WHERE key = ?", (key,)).fetchone()
                current = int(json.loads(row["value_json"])) if row else 0
                conn.execute(
                    """
                    INSERT INTO meta (key, value_json, updated_at)
                    VALUES (?, ?, ?)
                    ON CONFLICT(key) DO UPDATE SET value_json=excluded.value_json, updated_at=excluded.updated_at
                    """,
                    (key, _json_dumps(current + delta), now),
                )
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise
