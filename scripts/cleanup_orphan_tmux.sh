#!/usr/bin/env bash
# SPDX-License-Identifier: MIT
#
# Idempotent cleanup of orphan agent-hypervisor tmux sockets and residual tempdirs.
#
# A ah socket is stale only when `tmux -L <name> list-sessions` fails.
# Live ah tmux servers are preserved regardless of PPID.
#
# Usage:
#   bash scripts/cleanup_orphan_tmux.sh

set -euo pipefail

uid_num=$(id -u)
socket_dir="/tmp/tmux-${uid_num}"

if [ ! -d "$socket_dir" ]; then
  echo "No tmux socket dir at ${socket_dir}; nothing to do."
  exit 0
fi

orphan_count=0
preserved_count=0

# Iterate ah sockets and probe each one. A live server responds to list-sessions.
for sock in "$socket_dir"/ah-*; do
  [ -e "$sock" ] || continue
  name=$(basename "$sock")

  # Find any tmux process bound to this exact socket name. Socket names are
  # compared as argv fields, not as a regex.
  mapfile -t pids < <(ps -eo pid,args | awk -v sn="$name" '
    {
      for (i = 1; i <= NF; i++) {
        if ($i == "-L" && $(i + 1) == sn) {
          print $1
          next
        }
      }
    }
  ')

  if [ "${#pids[@]}" -gt 0 ]; then
    if timeout 2s tmux -L "$name" list-sessions >/dev/null 2>&1; then
      preserved_count=$((preserved_count + 1))
      continue
    fi
  fi

  # Stale socket: no live server responds, or no process is bound to this
  # exact socket. Terminate any hung matching processes before removing it.
  for pid in "${pids[@]}"; do
    kill -TERM "$pid" 2>/dev/null || true
  done
  if [ "${#pids[@]}" -gt 0 ]; then
    sleep 0.2
    for pid in "${pids[@]}"; do
      kill -KILL "$pid" 2>/dev/null || true
    done
  fi

  rm -f "$sock"
  orphan_count=$((orphan_count + 1))
done

echo "Cleanup: removed ${orphan_count} orphan socket(s); preserved ${preserved_count} live server(s)."

# Remove residual /tmp/.tmp?????? workdirs only when no live process references
# them. Active ah state dirs and tmux working directories are preserved.
find /tmp -maxdepth 1 -type d -name '.tmp??????' 2>/dev/null | while IFS= read -r dir; do
  if pgrep -af "$dir" >/dev/null 2>&1; then
    continue
  fi
  if pgrep -af "tmux .* -c $dir" >/dev/null 2>&1; then
    continue
  fi
  rm -rf "$dir"
done

echo "Cleanup complete."
