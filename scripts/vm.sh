#!/usr/bin/env bash
# Run Docker-backed work on the test host named by $ARCHIVE_VM_HOST (an ssh alias).
# Every Docker command is scoped to compose project "mailroom-archive"; other projects
# on the host are checked before and after and must still be running.
set -euo pipefail
: "${ARCHIVE_VM_HOST:?set ARCHIVE_VM_HOST to the test host ssh alias}"
cd "$(dirname "$0")/.."
H="$ARCHIVE_VM_HOST"
COMPOSE="docker compose -p mailroom-archive -f ops/compose.yml"

others() { ssh "$H" "docker ps --format '{{.Names}}'" | grep -v '^mailroom-archive-' | sort || true; }

guarded() {
  local before after gone
  before=$(others)
  ssh "$H" "cd ~/mailroom-archive && $*"
  after=$(others)
  gone=$(comm -23 <(echo "$before") <(echo "$after"))
  if [ -n "$gone" ]; then echo "STOP: other containers no longer running: $gone" >&2; exit 1; fi
}

case "${1:-}" in
  sync)
    rsync -az --delete --exclude .git --exclude .venv --exclude data/ --exclude .superpowers/ \
      --exclude .publish-denylist --exclude .ruff_cache --exclude .pytest_cache \
      ./ "$H":mailroom-archive/ ;;
  run)
    shift; ssh "$H" "cd ~/mailroom-archive && ~/.local/bin/uv sync -q && $*" ;;
  up)
    profile="${2:?profile: single|small|full}"
    case "$profile" in
      single|small) env=small mem=1200m ;;
      full) env=full mem=2g ;;
      *) echo "unknown profile $profile" >&2; exit 2 ;;
    esac
    guarded "ARCHIVE_ENV=$env ARCHIVE_MEM=$mem $COMPOSE --profile $profile up -d --wait" ;;
  down)
    guarded "$COMPOSE --profile full down -v" ;;
  *)
    echo "usage: scripts/vm.sh sync | run <cmd...> | up <single|small|full> | down" >&2; exit 2 ;;
esac
