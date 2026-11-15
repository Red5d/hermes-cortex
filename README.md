# Hermes Cortex (`cortex`)

System 1 event triage, hot-reloaded dynamic watchers, and escalation routing to **`herald`**, paired with 3-tier System 2 routing (**`herald`** → **`analyst`** / **`thinker`**) via **`advanced-delegation`**.

---

## Architecture

| Layer | Component | Role |
| :--- | :--- | :--- |
| **System 1 (Decision)** | `DecisionClient` (`cortex`) | Non-generative decision model (`discard \| log \| escalate`) evaluating incoming events with zero System 2 LLM tokens. |
| **System 2 (Front Door)** | `herald` profile | Fast, low-cost LLM with lean toolsets. Answers simple queries directly, relays `[CORTEX ESCALATION]` alerts, and routes deeper work via `advanced-delegation`. |
| **System 2 (Mid-Tier)** | `analyst` profile | Balanced LLM with full tools (`file`, `terminal`, `web`, `cortex_*`). Handles ~80% of delegated work, writes probe scripts, and adds/tests `cortex` watchers and rules. |
| **System 2 (Deep)** | `thinker` profile | Frontier reasoning LLM reserved for complex architecture, hard debugging, and high-stakes decisions. |

### Bundled Plugin Skills
`cortex` automatically registers two read-only skills when enabled (visible in `skills_list` as `cortex:cortex-routing` and `cortex:cortex-monitoring`):
- **`cortex-routing`** — Guides `herald` on direct replies vs `analyst`/`thinker` delegation, context handoffs, and handling `[CORTEX ESCALATION]` alerts.
- **`cortex-monitoring`** — Guides `analyst` and `thinker` on adding `log_tail`, `http_poll`, `webhook_route`, and `command_poll` watchers, writing probes in `~/.hermes/cortex/probes/`, crafting System 1 rules, and verifying with `cortex_test_event`.

---

## Installation

### Option A: Automated Install Script (Full 3-Profile Setup)
Clone the repo and run on the target Hermes host:
```bash
git clone https://github.com/Red5d/hermes-cortex.git
cd hermes-cortex
./scripts/install-remote.sh
```
This installs `cortex`, creates `herald`/`analyst`/`thinker` (`--clone`), enables `cortex` on all three, installs and enables `advanced-delegation` on `herald`, configures `delegation.backend: profile_routes` (`analyst` as default, `thinker` as deep option), sets `allow_gateway_injection: true`, and runs `hermes plugins validate`.

### Option B: Direct Plugin Install (`hermes plugins install`)
If you already have your profiles set up, `plugin.yaml` lives at the repo root so Hermes can install it directly:
```bash
hermes -p herald plugins install https://github.com/Red5d/hermes-cortex --enable
```

### Option C: Manual Install
1. Copy the plugin files (`plugin.yaml`, `*.py`, `skills/`, `tests/`) into `~/.hermes/plugins/cortex` and create `~/.hermes/cortex/probes`.
2. Create the three profiles and symlink `cortex` into each:
   ```bash
   for prof in herald analyst thinker; do
     hermes profile create "$prof" --clone
     cp profiles/$prof/SOUL.md ~/.hermes/profiles/$prof/SOUL.md
     mkdir -p ~/.hermes/profiles/$prof/plugins
     ln -s ~/.hermes/plugins/cortex ~/.hermes/profiles/$prof/plugins/cortex
     hermes -p "$prof" plugins enable cortex
     hermes -p "$prof" config set plugins.entries.cortex.settings.daemon_profile herald
     hermes -p "$prof" config set plugins.entries.cortex.settings.shared_state_dir ~/.hermes/cortex
   done
   ```
3. Install and configure `advanced-delegation` on `herald`:
   ```bash
   hermes -p herald plugins install advanced-delegation
   hermes -p herald plugins enable advanced-delegation
   hermes -p herald config set plugins.entries.cortex.allow_gateway_injection true
   hermes -p herald config set delegation.backend profile_routes
   ```
   Add `profiles` and `default` under `delegation.backends.profile_routes` in `~/.hermes/profiles/herald/config.yaml` (see `profiles/herald/config.yaml.template` for the exact list structure), or run `hermes -p herald delegation config`.

---

## Required Manual Configuration

1. **Per-Tier Models:** `--clone` copies your `default` profile's model. Set fast / mid / deep models on each profile:
   ```bash
   hermes -p herald model
   hermes -p analyst model
   hermes -p thinker model
   ```
2. **System 1 Decision Backend & API Key:**
   - **TypeSafe SystemOne (default):** Add `CORTEX_DECISION_API_KEY=<key>` (or `TYPESAFE_KEY=<key>`) to `~/.hermes/profiles/herald/.env` (and `analyst/.env` if `analyst` will run `cortex_test_event`).
   - **OpenRouter Decisions API (`openrouter_decisions` format):**
     ```bash
     for prof in herald analyst; do
       hermes -p "$prof" config set plugins.entries.cortex.settings.decision_backend http_decision
       hermes -p "$prof" config set plugins.entries.cortex.settings.decision_format openrouter_decisions
       hermes -p "$prof" config set plugins.entries.cortex.settings.decision_url https://openrouter.ai/api/alpha/decisions
       hermes -p "$prof" config set plugins.entries.cortex.settings.decision_model "~typesafe/jev-latest"
     done
     ```
     And set `CORTEX_DECISION_API_KEY=<openrouter-key>` in `herald/.env` and `analyst/.env`.
3. **Gateway / Messaging Channel:** Configure `herald`'s bot token if you want it on Discord/Telegram (`hermes -p herald setup`), then start a new session (`/reset` or restart the gateway) so the `cortex_*` tools and `profile_routes` schema load.

---

## Usage

### Talking to `herald`
- Ask normal questions directly.
- Ask for heavy work or monitoring changes (e.g., *"Monitor `/var/log/caddy/error.log` for 502 spikes and alert me"*). `herald` delegates to `analyst`, who inspects the log, registers the `log_tail` watcher + System 1 rule, tests it with `cortex_test_event`, and reports back. `herald`'s `WatcherManager` hot-reloads the new watcher within ~1.5s without restarting the gateway.

### Tools (`cortex_*`)
- `cortex_manage_watchers(action="list|add|update|remove|toggle", watcher=..., watcher_id=...)`
- `cortex_manage_rules(action="list|add|update|remove|toggle", rule=..., rule_id=...)`
- `cortex_query_log(limit=50, source=..., decision=...)`
- `cortex_test_event(source=..., payload=...)` — dry-runs rules + System 1 without triggering escalations.

### Webhook Endpoint
Enabled `webhook_route` watchers are served live at:
- `GET http://<listen_host>:<listen_port>/health` (default `127.0.0.1:8655`)
- `POST http://<listen_host>:<listen_port>/ingest/<route_name>` (supports `X-Hub-Signature-256` / `X-Cortex-Signature` HMAC verification when `secret_env` or `secret` is configured).
