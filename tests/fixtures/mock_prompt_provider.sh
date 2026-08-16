#!/usr/bin/env bash
set -euo pipefail

prompt=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --prompt)
      prompt="${2:-}"
      shift 2
      ;;
    -h|--help)
      echo "usage: $0 --prompt codex_update|codex_update_ready|trust_path|unknown_eula|transient_ready|claude_try_ready|stable_unknown"
      exit 0
      ;;
    *)
      echo "unknown argument: $1" >&2
      exit 2
      ;;
  esac
done

render_codex_update_menu() {
  cat <<'EOF'
✨ Update available! 0.135.0 -> 0.139.0
› 1. Update now (runs `npm install -g @openai/codex`)
  2. Skip
  3. Skip until next version
  Press enter to continue
EOF
}

render_codex_update_menu_selected() {
  printf '\033[2J\033[H'
  printf '✨ Update available! 0.135.0 -> 0.139.0\n'
  if (( answer == 1 )); then printf '› '; else printf '  '; fi
  printf '1. Update now (runs `npm install -g @openai/codex`)\n'
  if (( answer == 2 )); then printf '› '; else printf '  '; fi
  printf '2. Skip\n'
  if (( answer == 3 )); then printf '› '; else printf '  '; fi
  printf '3. Skip until next version\n'
  printf '  Press enter to continue\n'
}

read_codex_update_selection() {
  answer=1
  local ch=""
  local seq1=""
  local seq2=""
  stty raw -echo 2>/dev/null || true
  while IFS= read -rsN1 ch; do
    case "$ch" in
      $'\r'|$'\n')
        break
        ;;
      $'\033')
        seq1=""
        seq2=""
        IFS= read -rsN1 -t 0.2 seq1 || true
        IFS= read -rsN1 -t 0.2 seq2 || true
        if [[ "$seq1$seq2" == "[B" || "$seq1$seq2" == "OB" ]]; then
          if (( answer < 3 )); then
            answer=$((answer + 1))
            render_codex_update_menu_selected
          fi
        elif [[ "$seq1$seq2" == "[A" || "$seq1$seq2" == "OA" ]]; then
          if (( answer > 1 )); then
            answer=$((answer - 1))
            render_codex_update_menu_selected
          fi
        fi
        ;;
    esac
  done
  stty sane 2>/dev/null || true
}

case "$prompt" in
  codex_update)
    printf '\033[?1049h\033[2J\033[H'
    render_codex_update_menu
    ;;
  codex_update_ready)
    printf '\033[?1049h\033[2J\033[H'
    render_codex_update_menu
    ;;
  trust_path)
    printf '\033[?1049h\033[2J\033[H'
    cat <<'EOF'
Do you trust this directory?

1) Yes, trust this workspace
2) No, exit
EOF
    ;;
  unknown_eula)
    printf '\033[?1049h\033[2J\033[H'
    cat <<'EOF'
New provider EULA requires review before continuing.

1) Accept terms and continue
2) Decline and exit
EOF
    ;;
  transient_ready)
    printf '\033[?1049h\033[2J\033[H'
    cat <<'EOF'
Loading provider shell...
New transient startup panel still rendering
EOF
    sleep 1
    printf '\033[2J\033[H'
    printf 'mock_prompt_provider: ready\n'
    printf '\033[60;1H  › '
    stty raw -echo 2>/dev/null || true
    while true; do
      probe=""
      while IFS= read -rsn1 probe; do
        [[ "$probe" != $'\r' && "$probe" != $'\n' ]] && break
      done
      [[ -z "$probe" ]] && break
      [[ "$probe" == $'\177' || "$probe" == $'\b' ]] && continue
      printf '%s' "$probe"
      sleep 0.05
      printf '\033[D \033[D'
    done
    stty sane 2>/dev/null || true
    sleep 30
    exit 0
    ;;
  claude_try_ready)
    printf '\033[?1049h\033[2J\033[H'
    printf 'Claude Code\n'
    printf 'Opus 4.8 (1M context)\n'
    printf '\033[60;1H❯ Try "fix lint errors"'
    stty raw -echo 2>/dev/null || true
    while true; do
      probe=""
      while IFS= read -rsn1 probe; do
        [[ "$probe" != $'\r' && "$probe" != $'\n' ]] && break
      done
      [[ -z "$probe" ]] && break
      [[ "$probe" == $'\177' || "$probe" == $'\b' ]] && continue
      printf '%s' "$probe"
      sleep 0.05
      printf '\033[D \033[D'
    done
    stty sane 2>/dev/null || true
    sleep 30
    exit 0
    ;;
  ghost_input)
    printf '\033[?1049h\033[2J\033[H'
    cat <<'EOF'
some command output
$ echo hello
EOF
    sleep 30
    exit 0
    ;;
  push_notif)
    printf '\033[?1049h\033[2J\033[H'
    cat <<'EOF'
[Notification] New version of CLI is available. Run update.
$ 
EOF
    sleep 30
    exit 0
    ;;
  stable_unknown)
    printf '\033[?1049h\033[2J\033[H'
    cat <<'EOF'
Mystery provider startup panel
No known prompt or seed readiness marker is visible.
EOF
    sleep 30
    exit 0
    ;;
  *)
    echo "missing or invalid --prompt value" >&2
    exit 2
    ;;
esac

if [[ "$prompt" == "codex_update" || "$prompt" == "codex_update_ready" ]]; then
  read_codex_update_selection
else
  IFS= read -r answer
fi
printf '\033[2J\033[H'
printf 'mock_prompt_provider: selected=%s\n' "$answer"
printf 'mock_prompt_provider: done\n'
if [[ "$prompt" == "codex_update_ready" ]]; then
  printf '\033[60;1H  › '
  stty raw -echo 2>/dev/null || true
    while true; do
      probe=""
      while IFS= read -rsn1 probe; do
        [[ "$probe" != $'\r' && "$probe" != $'\n' ]] && break
      done
      [[ -z "$probe" ]] && break
      if [[ "$probe" == $'\177' || "$probe" == $'\b' ]]; then
        printf '\033[D \033[D'
        continue
      fi
      printf '%s' "$probe"
    done
  stty sane 2>/dev/null || true
else
  printf '\033[60;1H$ '
fi
sleep 30
