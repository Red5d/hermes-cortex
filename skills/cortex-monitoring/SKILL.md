---
name: cortex-monitoring
description: Use when adding, tuning, or testing Cortex watchers, probes, and System 1 rules.
version: 0.1.0
---

# Cortex Monitoring (Watchers, Probes & System 1 Rules)

`cortex` uses a shared SQLite WAL store (`shared_state_dir/cortex.db`). Any watcher or rule added from `analyst` or `thinker` is hot-reloaded by `herald`'s `WatcherManager` within ~1–2 seconds with **no gateway restart**.

## Adding a Monitor (Standard Workflow)

1. **Inspect the target first** (`file` / `terminal`) to confirm exact log paths, HTTP endpoints, or command outputs.
2. **Choose the watcher type:**
   - `log_tail`: `{source, path, regex_prefilter?, from_end: true, batch_window_ms: 500, max_batch_lines: 20}`
     - Must reside under `allowed_log_roots` (`/var/log`, `/opt/data/logs`, `~/.hermes/logs`).
     - Always set `regex_prefilter` on noisy logs so only relevant lines reach System 1.
   - `http_poll`: `{source, url, method: "GET", interval_seconds: 30, emit_on: "status_error|json_change|always"}`
   - `webhook_route`: `{source, route_name, secret_env?}` — exposes `POST http://<listen_host>:<listen_port>/ingest/<route_name>`.
   - `command_poll`: `{source, script_path, args?, interval_seconds: 30, timeout_seconds: 10}`
     - Script MUST live inside `~/.hermes/cortex/probes/` (relative filenames resolve there).
     - Write and test the probe script in `terminal` *before* registering the watcher.
3. **Register the watcher:** `cortex_manage_watchers(action="add", watcher={"id": "...", "type": "...", "config": {...}})`.
4. **Register a System 1 rule:** `cortex_manage_rules(action="add", rule={"id": "...", "name": "...", "source": "...", "condition": "...", "severity": "low|medium|high", "cooldown_seconds": 300, "force_escalate_regex": "..."})`.
   - Write `condition` as a crisp, unambiguous predicate for the System 1 decision model (`discard | log | escalate`).
   - Use `force_escalate_regex` for critical patterns (e.g. OOM, panic, disk full) that must never be suppressed.
5. **Dry-run verify:** Call `cortex_test_event(source="...", payload=...)` with both a benign sample and an alert sample to confirm expected classification before finishing.
