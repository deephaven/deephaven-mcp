#!/bin/sh
# Installs dh: curl -fsSL https://github.com/deephaven/deephaven-mcp/releases/latest/download/install.sh | sh
set -eu

REPO_URL="${DH_INSTALL_REPO_URL:-https://github.com/deephaven/deephaven-mcp}"
# Must be user-writable, or auto-update can't replace the binary.
DIR="${DH_INSTALL_DIR:-$HOME/.local/bin}"

case "$(uname -s)-$(uname -m)" in
  Darwin-arm64) TARGET=aarch64-apple-darwin ;;
  Darwin-x86_64) TARGET=x86_64-apple-darwin ;;
  Linux-x86_64) TARGET=x86_64-unknown-linux-gnu ;;
  Linux-aarch64 | Linux-arm64) TARGET=aarch64-unknown-linux-gnu ;;
  *)
    echo "dh: unsupported platform $(uname -sm)" >&2
    exit 1
    ;;
esac
NAME="dh-$TARGET"

# Every redirect hop must stay on HTTPS (plain HTTP is only for a local test server).
fetch() {
  case "$REPO_URL" in
    https://*) curl --proto '=https' --proto-redir '=https' "$@" ;;
    *) curl "$@" ;;
  esac
}

# Pin one tag so the binary and checksums come from the same release.
if [ -n "${DH_INSTALL_VERSION:-}" ]; then
  TAG="v${DH_INSTALL_VERSION#v}"
else
  latest=$(fetch -fsSLI -o /dev/null -w '%{url_effective}' "$REPO_URL/releases/latest")
  TAG="${latest##*/}"
fi
BASE="$REPO_URL/releases/download/$TAG"

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
cd "$tmp"

echo "Downloading dh $TAG for $TARGET..."
fetch -fsSL -o "$NAME" "$BASE/$NAME"
fetch -fsSL -o SHA256SUMS "$BASE/SHA256SUMS"

if ! grep " $NAME\$" SHA256SUMS >expected; then
  echo "dh: $TAG has no binary for $TARGET" >&2
  exit 1
fi
if command -v sha256sum >/dev/null 2>&1; then
  sha256sum -c expected >/dev/null
else
  shasum -a 256 -c expected >/dev/null
fi

mkdir -p "$DIR"
chmod 755 "$NAME"
mv "$NAME" "$DIR/dh"
echo "Installed dh $TAG to $DIR/dh"

case ":$PATH:" in
  *":$DIR:"*) ;;
  *) echo "Add $DIR to your PATH to run dh." ;;
esac
