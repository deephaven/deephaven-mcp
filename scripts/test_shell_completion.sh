#!/usr/bin/env bash
# Per-shell tab-completion exerciser for the dhcli CLI.
#
# Sources the actual `dhcli self completion <shell>` output in each real
# shell and requests a completion for the prefix "daem", asserting the
# suggestion "daemon" comes back. Depth per shell is the deepest each
# shell supports non-interactively:
#
#   bash  (>= 4.4)  source the script, invoke the generated completion
#                   function with COMP_WORDS/COMP_CWORD, assert COMPREPLY.
#   fish            source the script, query fish's native `complete -C`.
#   zsh             assert the script loads cleanly under compinit, then
#                   assert the protocol answer its completion widget
#                   calls (zsh's completion system has no clean
#                   non-interactive query API, so a full TAB simulation
#                   is not attempted here).
#
# Usage:
#   scripts/test_shell_completion.sh [--require shell[,shell...]]
#
# Without --require, a shell that is not installed (or too old) is
# skipped with a notice — local-run ergonomics. With --require, a
# listed shell that cannot be tested is a hard failure — CI passes
# --require bash,zsh,fish so coverage cannot silently shrink.
#
# Exit codes: 0 all executed checks passed; 1 a check failed or a
# required shell was unavailable.

set -euo pipefail

REQUIRED=""
if [[ "${1:-}" == "--require" ]]; then
    REQUIRED="${2:?--require needs a comma-separated shell list}"
elif [[ "${1:-}" == --require=* ]]; then
    REQUIRED="${1#--require=}"
elif [[ -n "${1:-}" ]]; then
    echo "usage: $0 [--require shell[,shell...]]" >&2
    exit 1
fi

if [[ -n "$REQUIRED" ]]; then
    IFS=',' read -ra _required_list <<<"$REQUIRED"
    for _shell in "${_required_list[@]}"; do
        case "$_shell" in
            bash|zsh|fish) ;;
            *)
                echo "unknown shell in --require: '${_shell}' (supported: bash, zsh, fish)" >&2
                exit 1
                ;;
        esac
    done
fi

FAILURES=0

is_required() {
    case ",${REQUIRED}," in
        *",$1,"*) return 0 ;;
        *) return 1 ;;
    esac
}

skip_or_fail() {
    local shell="$1" reason="$2"
    if is_required "$shell"; then
        echo "FAIL  ${shell}: required but ${reason}"
        FAILURES=$((FAILURES + 1))
    else
        echo "SKIP  ${shell}: ${reason}"
    fi
}

report() {
    local shell="$1" check="$2" ok="$3"
    if [[ "$ok" == 0 ]]; then
        echo "PASS  ${shell}: ${check}"
    else
        echo "FAIL  ${shell}: ${check}"
        FAILURES=$((FAILURES + 1))
    fi
}

if ! command -v dhcli >/dev/null 2>&1; then
    echo "FAIL  dhcli is not on PATH (install with 'uv sync' and run via 'uv run')" >&2
    exit 1
fi

# --------------------------------------------------------------------
# bash: source the script, drive the completion function directly.
# --------------------------------------------------------------------
test_bash() {
    local bash_bin
    bash_bin="$(command -v bash)" || { skip_or_fail bash "not installed"; return; }
    local major minor
    major="$("$bash_bin" -c 'echo "${BASH_VERSINFO[0]}"')"
    minor="$("$bash_bin" -c 'echo "${BASH_VERSINFO[1]}"')"
    if (( major < 4 || (major == 4 && minor < 4) )); then
        skip_or_fail bash "version ${major}.${minor} < 4.4 (click's completion floor)"
        return
    fi
    local out ok=1
    out="$("$bash_bin" --norc -c '
        source <(dhcli self completion bash) || exit 1
        COMP_WORDS=(dhcli daem)
        COMP_CWORD=1
        _dhcli_completion
        printf "%s\n" "${COMPREPLY[@]}"
    ')" && grep -qx "daemon" <<<"$out" && ok=0
    report bash "source + COMPREPLY suggests 'daemon'" "$ok"
}

# --------------------------------------------------------------------
# fish: source the script, query completions with `complete -C`.
# --------------------------------------------------------------------
test_fish() {
    command -v fish >/dev/null 2>&1 || { skip_or_fail fish "not installed"; return; }
    local out ok=1
    out="$(fish -c 'dhcli self completion fish | source; complete -C"dhcli daem"')" \
        && grep -q "^daemon" <<<"$out" && ok=0
    report fish "source + 'complete -C' suggests 'daemon'" "$ok"
}

# --------------------------------------------------------------------
# zsh: clean load under compinit, then the widget's protocol answer.
# --------------------------------------------------------------------
test_zsh() {
    command -v zsh >/dev/null 2>&1 || { skip_or_fail zsh "not installed"; return; }
    local ok=1
    zsh -f -c '
        autoload -Uz compinit
        compinit -u -d "$(mktemp -t zcompdump.XXXXXX)"
        source <(dhcli self completion zsh)
        (( $+functions[_dhcli_completion] ))
    ' >/dev/null 2>&1 && ok=0
    report zsh "script loads cleanly under compinit" "$ok"

    local out ok2=1
    out="$(_DHCLI_COMPLETE=zsh_complete COMP_WORDS="dhcli daem" COMP_CWORD=1 dhcli)" \
        && grep -qx "daemon" <<<"$out" && ok2=0
    report zsh "widget protocol (zsh_complete) suggests 'daemon'" "$ok2"
}

test_bash
test_fish
test_zsh

if (( FAILURES > 0 )); then
    echo "shell completion: ${FAILURES} check(s) failed" >&2
    exit 1
fi
echo "shell completion: all executed checks passed"
