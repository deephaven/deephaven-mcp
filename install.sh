#!/bin/sh
# Installs the Deephaven CLI: curl -fsSL https://github.com/deephaven/deephaven-mcp/releases/latest/download/install.sh | sh
set -eu

# Rewritten from deno.json "bin" by scripts/release.ts.
BIN=dh

REPO_URL="${DH_INSTALL_REPO_URL:-https://github.com/deephaven/deephaven-mcp}"
# Must be user-writable, or auto-update can't replace the binary.
DIR="${DH_INSTALL_DIR:-$HOME/.local/bin}"
# Made absolute now, because the script later cd's into a temp directory.
case "$DIR" in /*) ;; *) DIR="$PWD/$DIR" ;; esac

case "$(uname -s)-$(uname -m)" in
  Darwin-arm64) TARGET=aarch64-apple-darwin ;;
  Darwin-x86_64) TARGET=x86_64-apple-darwin ;;
  Linux-x86_64) TARGET=x86_64-unknown-linux-gnu ;;
  Linux-aarch64 | Linux-arm64) TARGET=aarch64-unknown-linux-gnu ;;
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

# Pin one tag so the binary and checksums come from the same release.
if [ -n "${DH_INSTALL_VERSION:-}" ]; then
  TAG="v${DH_INSTALL_VERSION#v}"
else
  # Read the tag from the first redirect (.../releases/tag/vX) without following it.
  latest=$(fetch -fsSI -o /dev/null -w '%{redirect_url}' "$REPO_URL/releases/latest")
  TAG="${latest##*/}"
fi
case "$TAG" in
  v*) ;;
  *)
    echo "$BIN: no release found at $REPO_URL/releases" >&2
    exit 1
    ;;
esac
BASE="$REPO_URL/releases/download/$TAG"

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
cd "$tmp"

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
echo "Installed $BIN $TAG to $DIR/$BIN"

# Adds DIR to PATH in the user's shell startup file, once.
add_to_path() {
  # These would break (or inject into) the generated shell line.
  case "$DIR" in *'"'* | *'$'* | *'`'* | *'\'*)
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
