#!/usr/bin/env bash
# Perception write arbiter — CI grep rule.
#
# `agents.state` has exactly one sanctioned writer: the gate in
# `db/perception/gate.rs`. Every other direct `UPDATE agents SET state`
# (or `... SET status`) is a stray writer that can move an agent's lifecycle
# state without CAS, without a from-state guard, and without emitting the
# caller's reason — the failure class 1.6.0 removed. This checker keeps that
# property true over time instead of by luck.
#
# Usage:
#   check_state_write_gate.sh            scan the repo's own src/ (resolved from
#                                        this script's location) and compare
#                                        against the baseline ratchet below
#   check_state_write_gate.sh ROOT       scan ROOT hermetically: any direct write
#                                        outside the gate file fails, no baseline
#
# The baseline lists the pre-migration call sites that existed when the arbiter
# landed. It is a ratchet: adding writes fails, and removing writes also fails
# until the baseline is lowered, so migration progress cannot silently stall.
# Regenerate it with `--update-baseline` after a migration step.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASELINE_FILE="$SCRIPT_DIR/state_write_gate_baseline.txt"
GATE_SUFFIX="db/perception/gate.rs"

UPDATE_BASELINE=0
ROOT=""
for arg in "$@"; do
  case "$arg" in
    --update-baseline) UPDATE_BASELINE=1 ;;
    *) ROOT="$arg" ;;
  esac
done

HERMETIC=1
if [ -z "$ROOT" ]; then
  HERMETIC=0
  ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)/src"
fi

if [ ! -d "$ROOT" ]; then
  echo "check_state_write_gate: scan root does not exist: $ROOT" >&2
  exit 2
fi

# Counts direct writes in one file. Line comments are stripped first so prose
# about the rule is not counted as a violation of it. Whitespace is collapsed
# next because the SQL is written across several lines in Rust string literals,
# so a line-oriented grep would miss exactly the writers this rule exists to
# catch. `\b` keeps `SET state_version` — a version bump, not a state write —
# out of the count.
count_direct_writes() {
  sed 's|//.*||' "$1" \
    | tr -d '\\' \
    | tr '\n' ' ' \
    | sed 's/[[:space:]][[:space:]]*/ /g' \
    | grep -o -i -E 'UPDATE agents SET (state|status)\b' \
    | wc -l \
    | tr -d ' '
}

collect_counts() {
  find "$ROOT" -type f -name '*.rs' | LC_ALL=C sort | while read -r file; do
    case "$file" in
      *"$GATE_SUFFIX") continue ;;
    esac
    count="$(count_direct_writes "$file")"
    if [ "$count" -gt 0 ]; then
      printf '%s %s\n' "$count" "${file#"$ROOT"/}"
    fi
  done
}

COUNTS="$(collect_counts)"

if [ "$UPDATE_BASELINE" -eq 1 ]; then
  if [ "$HERMETIC" -eq 1 ]; then
    echo "check_state_write_gate: --update-baseline only applies to the repo scan" >&2
    exit 2
  fi
  printf '%s\n' "# Direct \`agents.state\` writers still outside the gate (ratchet: this list may only shrink)." > "$BASELINE_FILE"
  printf '%s\n' "# Format: <count> <path relative to src/>" >> "$BASELINE_FILE"
  if [ -n "$COUNTS" ]; then
    printf '%s\n' "$COUNTS" >> "$BASELINE_FILE"
  fi
  echo "check_state_write_gate: baseline updated at $BASELINE_FILE"
  exit 0
fi

if [ "$HERMETIC" -eq 1 ]; then
  if [ -n "$COUNTS" ]; then
    echo "check_state_write_gate: direct agents.state writes outside $GATE_SUFFIX:" >&2
    printf '%s\n' "$COUNTS" >&2
    exit 1
  fi
  exit 0
fi

if [ ! -f "$BASELINE_FILE" ]; then
  echo "check_state_write_gate: baseline missing at $BASELINE_FILE" >&2
  exit 2
fi

# `tr -d '\r'` keeps the checker working on a tree checked out with CRLF, where a
# trailing carriage return would otherwise make every baseline path fail to match
# and report the whole baseline as both new and removed.
BASELINE="$(tr -d '\r' < "$BASELINE_FILE" | grep -v '^[[:space:]]*#' | grep -v '^[[:space:]]*$' || true)"

failed=0

# New or grown writers: the gate is being bypassed.
while read -r count path; do
  [ -z "${path:-}" ] && continue
  baseline_count="$(printf '%s\n' "$BASELINE" | awk -v p="$path" '$2 == p { print $1 }' | head -1)"
  if [ -z "$baseline_count" ]; then
    echo "check_state_write_gate: new direct agents.state writer outside the gate: $path ($count)" >&2
    failed=1
  elif [ "$count" -gt "$baseline_count" ]; then
    echo "check_state_write_gate: $path has $count direct agents.state writes, baseline allows $baseline_count" >&2
    failed=1
  fi
done <<< "$COUNTS"

# Shrunk or removed writers: the ratchet must be lowered so progress is recorded.
while read -r baseline_count path; do
  [ -z "${path:-}" ] && continue
  count="$(printf '%s\n' "$COUNTS" | awk -v p="$path" '$2 == p { print $1 }' | head -1)"
  count="${count:-0}"
  if [ "$count" -lt "$baseline_count" ]; then
    echo "check_state_write_gate: $path now has $count direct writes, baseline still claims $baseline_count — rerun with --update-baseline to record the migration" >&2
    failed=1
  fi
done <<< "$BASELINE"

if [ "$failed" -ne 0 ]; then
  exit 1
fi

echo "check_state_write_gate: ok (gate is the only sanctioned agents.state writer; $(printf '%s\n' "$BASELINE" | wc -l | tr -d ' ') baselined pre-migration files)"
