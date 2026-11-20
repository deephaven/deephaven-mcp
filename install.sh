#!/bin/sh
# Installs the Deephaven CLI: curl -fsSL https://github.com/deephaven/deephaven-mcp/releases/download/cli-channel-release/install.sh | sh
# On another release channel: ... | DH_CHANNEL=rc sh
set -eu

# Rewritten from package.json "name" by scripts/release.ts.
BIN=dh

REPO_URL="${DH_INSTALL_REPO_URL:-https://github.com/deephaven/deephaven-mcp}"
CHANNEL="${DH_CHANNEL:-release}"
# Same rules as src/channel.ts; dev is reserved for builds run from source.
case "$CHANNEL" in
  dev)
    echo "$BIN: channel dev is reserved for builds run from source" >&2
    exit 1
    ;;
  [a-z]*) ;;
  *) CHANNEL_INVALID=1 ;;
esac
case "$CHANNEL" in *[!a-z0-9-]*) CHANNEL_INVALID=1 ;; esac
if [ -n "${CHANNEL_INVALID:-}" ] || [ "${#CHANNEL}" -gt 40 ]; then
  echo "$BIN: invalid channel \"$CHANNEL\": use lowercase letters, digits and hyphens, starting with a letter" >&2
  exit 1
fi
# Must be user-writable, or auto-update can't replace the binary.
DIR="${DH_INSTALL_DIR:-$HOME/.local/bin}"
# Made absolute now, because the script later cd's into a temp directory.
case "$DIR" in /*) ;; *) DIR="$PWD/$DIR" ;; esac

case "$(uname -s)-$(uname -m)" in
  Darwin-arm64) TARGET=darwin-arm64 ;;
  Darwin-x86_64)
    # Under Rosetta, uname reports x86_64 on Apple silicon; install the native build.
    if [ "$(sysctl -n sysctl.proc_translated 2>/dev/null)" = 1 ]; then
      TARGET=darwin-arm64
    else
      TARGET=darwin-x64
    fi
    ;;
  Linux-x86_64) TARGET=linux-x64 ;;
  Linux-aarch64 | Linux-arm64) TARGET=linux-arm64 ;;
  *)
    echo "$BIN: unsupported platform $(uname -sm)" >&2
    exit 1
    ;;
esac
NAME="$BIN-$TARGET"

# HTTPS only; plain HTTP only as the starting URL of a loopback test server.
insecure() {
  echo "$BIN: refusing to install over insecure URL $REPO_URL" >&2
  exit 1
}
case "$REPO_URL" in
  https://*) ;;
  # Userinfo can disguise the real host, e.g. http://127.0.0.1:x@example.com.
  http://*@*) insecure ;;
  http://127.0.0.1 | http://127.0.0.1[:/]* | http://localhost | http://localhost[:/]* | "http://[::1]" | "http://[::1]"[:/]*) ;;
  *) insecure ;;
esac
# Redirects must be HTTPS even from loopback; release downloads never need more.
fetch() { curl --proto '=http,https' --proto-redir '=https' "$@"; }

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
cd "$tmp"

# Pin one tag so the binary and checksums come from the same release.
if [ -n "${DH_INSTALL_VERSION:-}" ]; then
  # Accepts 3.1.1, v3.1.1 or cli-v3.1.1.
  version="${DH_INSTALL_VERSION#cli-}"
  TAG="cli-v${version#v}"
else
  # The channel release's manifest names its current version.
  url="$REPO_URL/releases/download/cli-channel-$CHANNEL/manifest.json"
  status=$(fetch -sSL -o manifest.json -w '%{http_code}' "$url")
  if [ "$status" = 404 ]; then
    echo "$BIN: no channel $CHANNEL at $REPO_URL" >&2
    exit 1
  elif [ "$status" != 200 ]; then
    echo "$BIN: HTTP $status fetching $url" >&2
    exit 1
  fi
  # build.ts writes one key per line, so the first "version" line is the release's.
  version=$(sed -n 's/^ *"version": *"\([^"]*\)".*/\1/p' manifest.json | head -n 1)
  case "$version" in
    [0-9]*) TAG="cli-v$version" ;;
    *)
      echo "$BIN: no version in $url" >&2
      exit 1
      ;;
  esac
fi
BASE="$REPO_URL/releases/download/$TAG"

echo "Downloading $BIN $TAG for $TARGET..."
fetch -fsSL -o "$NAME" "$BASE/$NAME"
fetch -fsSL -o SHA256SUMS "$BASE/SHA256SUMS"

if ! grep " $NAME\$" SHA256SUMS >expected; then
  echo "$BIN: $TAG has no binary for $TARGET" >&2
  exit 1
fi
if command -v sha256sum >/dev/null 2>&1; then
  sha256sum -c expected >/dev/null
else
  shasum -a 256 -c expected >/dev/null
fi

mkdir -p "$DIR"
chmod 755 "$NAME"
mv "$NAME" "$DIR/$BIN"
# Install metadata the CLI reads (see src/install-meta.ts); written fresh on every install.
printf '{"channel":"%s"}\n' "$CHANNEL" >"$DIR/$BIN.meta.$$.tmp"
mv "$DIR/$BIN.meta.$$.tmp" "$DIR/$BIN.meta"
echo "Installed $BIN v${TAG#cli-v} ($CHANNEL channel) to $DIR/$BIN"

# Adds DIR to PATH in the user's shell startup file, once.
add_to_path() {
  # These would break (or inject into) the generated shell line; ':' would split the PATH entry.
  case "$DIR" in *'"'* | *'$'* | *'`'* | *'\'* | *:*)
    echo "Add $DIR to your PATH to run $BIN."
    return
    ;;
  esac
  line="export PATH=\"$DIR:\$PATH\""
  case "$(basename "${SHELL:-sh}")" in
    zsh) rc="${ZDOTDIR:-$HOME}/.zshrc" ;;
    bash)
      # macOS terminals start login shells, which read .bash_profile instead.
      if [ "$(uname -s)" = Darwin ]; then rc="$HOME/.bash_profile"; else rc="$HOME/.bashrc"; fi
      ;;
    fish)
      rc="$HOME/.config/fish/config.fish"
      line="fish_add_path \"$DIR\""
      ;;
    *) rc="$HOME/.profile" ;;
  esac
  if ! { [ -f "$rc" ] && grep -qxF "$line" "$rc"; }; then
    mkdir -p "$(dirname "$rc")"
    printf '\n# Added by the %s installer\n%s\n' "$BIN" "$line" >>"$rc"
  fi
  echo "Added $DIR to PATH in $rc. Open a new terminal, or run: $line"
}

case ":$PATH:" in
  *":$DIR:"*) ;;
  *)
    if [ -n "${DH_INSTALL_NO_MODIFY_PATH:-}" ]; then
      echo "Add $DIR to your PATH to run $BIN."
    else
      add_to_path
    fi
    ;;
esac
