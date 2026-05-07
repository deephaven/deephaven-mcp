#!/usr/bin/env bash
# check-deps-fresh.sh
#
# Force a fresh dependency resolve (ignoring uv's HTTP cache) and run the
# strict checks that CI runs: mypy + unit tests. Use this to:
#   - Reproduce a `Dependency Freshness` CI failure locally.
#   - Sanity-check a PR right before merging when an upstream release is
#     suspected (e.g., pydeephaven, deephaven-server, deephaven-coreplus-client).
#   - Manually probe upstream compatibility at any time.
#
# This repo intentionally does NOT commit `uv.lock`, so a normal `uv sync`
# already resolves fresh; the `--refresh` flag here additionally invalidates
# the HTTP cache so we re-fetch package metadata from PyPI.
#
# Usage:
#   bash bin/check-deps-fresh.sh

set -euo pipefail

REPO_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${REPO_ROOT}"

echo "==> Syncing dependencies with --refresh (invalidates HTTP cache; re-fetches metadata from PyPI)..."
uv sync --extra dev --refresh

echo
echo "==> Resolved versions:"
uv pip list

echo
echo "==> Running mypy..."
uv run mypy src --exclude _version.py --exclude .venv

echo
echo "==> Running unit tests..."
uv run pytest -q --no-cov

echo
echo "==> Freshness check passed."
