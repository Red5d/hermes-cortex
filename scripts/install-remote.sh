#!/usr/bin/env bash
set -euo pipefail

HERMES_HOME="${HERMES_HOME:-$HOME/.hermes}"
HERMES_BIN="${HERMES_BIN:-$(command -v hermes || echo "$HOME/.local/bin/hermes")}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

echo "Installing cortex plugin into $HERMES_HOME/plugins/cortex ..."
mkdir -p "$HERMES_HOME/plugins/cortex" "$HERMES_HOME/cortex/probes"
rm -rf "$HERMES_HOME/plugins/cortex"
mkdir -p "$HERMES_HOME/plugins/cortex"
for item in plugin.yaml __init__.py daemon.py decision.py ingestor.py rules.py store.py tools.py watchers.py skills tests README.md; do
  if [ -e "$REPO_ROOT/$item" ]; then
    cp -R "$REPO_ROOT/$item" "$HERMES_HOME/plugins/cortex/$item"
  fi
done

for prof in herald analyst thinker; do
  prof_dir="$HERMES_HOME/profiles/$prof"
  if [ ! -d "$prof_dir" ]; then
    echo "Creating profile $prof via hermes (--clone)..."
    "$HERMES_BIN" profile create "$prof" --clone
  fi
  cp "$REPO_ROOT/profiles/$prof/SOUL.md" "$prof_dir/SOUL.md"
  cp "$REPO_ROOT/profiles/$prof/config.yaml.template" "$prof_dir/cortex.config.yaml.template"
  mkdir -p "$prof_dir/plugins"
  rm -rf "$prof_dir/plugins/cortex"
  ln -s "$HERMES_HOME/plugins/cortex" "$prof_dir/plugins/cortex"
  "$HERMES_BIN" -p "$prof" plugins enable cortex
  "$HERMES_BIN" -p "$prof" config set plugins.entries.cortex.settings.daemon_profile herald
  "$HERMES_BIN" -p "$prof" config set plugins.entries.cortex.settings.shared_state_dir "$HERMES_HOME/cortex"
done

echo "Installing and configuring advanced-delegation on herald..."
"$HERMES_BIN" -p herald plugins install advanced-delegation
"$HERMES_BIN" -p herald plugins enable advanced-delegation
"$HERMES_BIN" -p herald config set plugins.entries.cortex.allow_gateway_injection true
"$HERMES_BIN" -p herald config set delegation.backend profile_routes

# Write the profile_routes.profiles list + default cleanly using Hermes's own YAML library.
HERMES_VENV_PY="$(ls -d "$HERMES_HOME"/installs/*/environments/*/venv/bin/python 2>/dev/null | head -n1 || true)"
if [ -n "$HERMES_VENV_PY" ]; then
  PYTHONPATH="$HERMES_HOME/hermes-agent" "$HERMES_VENV_PY" - "$HERMES_HOME/profiles/herald/config.yaml" <<'PYEOF'
import sys, pathlib
import hermes_yaml as yaml
cfg_path = pathlib.Path(sys.argv[1])
data = yaml.safe_load(cfg_path.read_text(encoding="utf-8")) or {}
deleg = data.setdefault("delegation", {})
deleg["backend"] = "profile_routes"
pr = deleg.setdefault("backends", {}).setdefault("profile_routes", {})
pr.pop("routes", None)
pr["default"] = "analyst"
pr["profiles"] = [
    {
        "name": "analyst",
        "description": "Mid-tier worker for routine coding, log/system investigation, probe authoring, and cortex watcher/rule updates.",
    },
    {
        "name": "thinker",
        "description": "Deep reasoning model for complex architecture, hard root-cause debugging, and high-stakes decisions.",
    },
]
cfg_path.write_text(yaml.safe_dump(data, sort_keys=False), encoding="utf-8")
PYEOF
fi

echo "Validating cortex plugin..."
"$HERMES_BIN" plugins validate "$HERMES_HOME/plugins/cortex"

echo ""
echo "Done. Remaining manual steps:"
echo "  1. Set per-tier models for herald (fast), analyst (mid), and thinker (deep) via 'hermes -p <name> model'"
echo "  2. Add CORTEX_DECISION_API_KEY (or TYPESAFE_KEY) to $HERMES_HOME/profiles/herald/.env (and analyst/.env)"
echo "  3. Set System 1 endpoint/format if not using TypeSafe defaults (see README.md)"
echo "  4. Start a new session (/reset) or restart the gateway"
