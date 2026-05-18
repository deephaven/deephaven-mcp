#!/usr/bin/env python3
"""Build standalone, fully self-contained binaries for the deephaven-mcp console scripts using PyApp.

This script produces one native binary per console script declared in
``[project.scripts]`` of ``pyproject.toml``:

- ``dh-mcp-community-server``  -> deephaven_mcp.mcp_systems_server.server:community
- ``dh-mcp-enterprise-server`` -> deephaven_mcp.mcp_systems_server.server:enterprise
- ``dh-mcp-docs-server``       -> deephaven_mcp.mcp_docs_server.main:main

The binaries are fully offline: they embed both a relocatable CPython
distribution (from python-build-standalone) AND the ``deephaven-mcp`` wheel
together with all of its transitive dependencies pre-installed. At runtime
PyApp simply unpacks this bundle into a per-user cache directory; **no
network access is required**.

High-level pipeline
-------------------

1. Build the ``deephaven-mcp`` wheel from the working tree (``python -m build``).
2. Download a ``python-build-standalone`` "install_only" archive matching the
   host platform and the requested Python version.
3. Extract the archive, then use its bundled ``pip`` to install the wheel
   plus its optional dependency groups (``community,enterprise`` by default)
   *into* the distribution itself.
4. Re-archive the (now-populated) Python distribution as a ``.tar.gz``.
5. Invoke ``cargo install pyapp`` once per binary, passing environment
   variables that tell PyApp to embed the prepared distribution and skip
   installation at runtime.
6. Copy each produced binary into ``dist/pyapp/`` with a descriptive name.

Cross-compilation is not supported because step 3 must execute platform
specific wheels. Run this script on each target platform (the companion
GitHub Actions workflow at ``.github/workflows/build-pyapp.yml`` does this
via a matrix of native runners).

Requirements
------------

- Python 3.12+
- Rust toolchain (``cargo``) on ``PATH``
- ``build`` Python package (auto-installed into a temporary venv if missing)
- Network access **at build time** to fetch python-build-standalone, the
  project's runtime dependencies, and the ``pyapp`` crate. The resulting
  binaries themselves require no network at runtime.

Usage
-----

::

    uv run scripts/build_pyapp.py                       # build all 3 binaries for host
    uv run scripts/build_pyapp.py --binaries docs       # only build the docs server
    uv run scripts/build_pyapp.py --python-version 3.13
    uv run scripts/build_pyapp.py --extras community    # omit enterprise

Run ``uv run scripts/build_pyapp.py --help`` for the full option list.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import platform
import shutil
import subprocess
import sys
import tarfile
import urllib.error
import urllib.request
import zipfile
from dataclasses import dataclass
from pathlib import Path

_LOGGER = logging.getLogger("build_pyapp")

# ---------------------------------------------------------------------------
# Configuration constants
# ---------------------------------------------------------------------------

# Pinned default python-build-standalone release. Update periodically.
# See https://github.com/astral-sh/python-build-standalone/releases
DEFAULT_PBS_RELEASE = "20260510"
DEFAULT_PYTHON_VERSION = "3.12.13"
DEFAULT_PYAPP_VERSION = "0.29.0"
DEFAULT_EXTRAS = "community,enterprise"

# Mapping of console-script name -> (binary-name, PYAPP_EXEC_SPEC).
# Keys are the short identifiers accepted via --binaries.
BINARIES: dict[str, tuple[str, str]] = {
    "community": (
        "dh-mcp-community-server",
        "deephaven_mcp.mcp_systems_server.server:community",
    ),
    "enterprise": (
        "dh-mcp-enterprise-server",
        "deephaven_mcp.mcp_systems_server.server:enterprise",
    ),
    "docs": (
        "dh-mcp-docs-server",
        "deephaven_mcp.mcp_docs_server.main:main",
    ),
}


# ---------------------------------------------------------------------------
# Target / platform detection
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Target:
    """A build target for PyApp.

    Attributes:
        triple: The python-build-standalone target triple
            (e.g. ``x86_64-unknown-linux-gnu``).
        os: One of ``linux``, ``macos``, ``windows``.
        python_exe_rel: Relative path to the Python interpreter inside the
            unpacked python-build-standalone archive.
        site_packages_rel: Relative path to ``site-packages`` inside the
            unpacked archive (template containing ``{minor}`` for the Python
            minor version).
        exe_suffix: Suffix for executables on this OS (``.exe`` on Windows).
    """

    triple: str
    os: str
    python_exe_rel: str
    site_packages_rel: str
    exe_suffix: str

    @property
    def is_windows(self) -> bool:
        return self.os == "windows"


# Supported targets. Keys are short aliases.
_TARGETS: dict[str, Target] = {
    "linux-x86_64": Target(
        triple="x86_64-unknown-linux-gnu",
        os="linux",
        python_exe_rel="bin/python3",
        site_packages_rel="lib/python{minor}/site-packages",
        exe_suffix="",
    ),
    "linux-aarch64": Target(
        triple="aarch64-unknown-linux-gnu",
        os="linux",
        python_exe_rel="bin/python3",
        site_packages_rel="lib/python{minor}/site-packages",
        exe_suffix="",
    ),
    "macos-x86_64": Target(
        triple="x86_64-apple-darwin",
        os="macos",
        python_exe_rel="bin/python3",
        site_packages_rel="lib/python{minor}/site-packages",
        exe_suffix="",
    ),
    "macos-aarch64": Target(
        triple="aarch64-apple-darwin",
        os="macos",
        python_exe_rel="bin/python3",
        site_packages_rel="lib/python{minor}/site-packages",
        exe_suffix="",
    ),
    "windows-x86_64": Target(
        triple="x86_64-pc-windows-msvc",
        os="windows",
        python_exe_rel="python.exe",
        site_packages_rel="Lib/site-packages",
        exe_suffix=".exe",
    ),
}


def detect_host_target() -> str:
    """Detect the short target alias for the current host."""
    machine = platform.machine().lower()
    system = platform.system().lower()

    if system == "linux":
        if machine in ("x86_64", "amd64"):
            return "linux-x86_64"
        if machine in ("aarch64", "arm64"):
            return "linux-aarch64"
    elif system == "darwin":
        if machine in ("x86_64", "amd64"):
            return "macos-x86_64"
        if machine in ("arm64", "aarch64"):
            return "macos-aarch64"
    elif system == "windows":
        if machine in ("amd64", "x86_64"):
            return "windows-x86_64"

    raise SystemExit(f"[build_pyapp] Unsupported host platform: {system}/{machine}")


# ---------------------------------------------------------------------------
# Shell helpers
# ---------------------------------------------------------------------------


def run(
    cmd: list[str],
    *,
    cwd: Path | None = None,
    env: dict[str, str] | None = None,
    check: bool = True,
) -> subprocess.CompletedProcess[str]:
    """Run a subprocess, streaming its output."""
    _LOGGER.info(f"[run] $ {' '.join(cmd)}")
    return subprocess.run(
        cmd,
        cwd=str(cwd) if cwd else None,
        env=env,
        check=check,
        text=True,
    )


def download(url: str, dest: Path) -> None:
    """Download ``url`` to ``dest`` with a basic progress indicator."""
    _LOGGER.info(f"[download] Fetching {url}")
    dest.parent.mkdir(parents=True, exist_ok=True)
    try:
        with urllib.request.urlopen(url) as resp, open(dest, "wb") as f:
            shutil.copyfileobj(resp, f)
    except urllib.error.HTTPError as exc:
        raise SystemExit(f"[build_pyapp] HTTP error fetching {url}: {exc}") from exc


# ---------------------------------------------------------------------------
# Pipeline steps
# ---------------------------------------------------------------------------


def build_project_wheel(repo_root: Path, out_dir: Path) -> Path:
    """Build the deephaven-mcp wheel into ``out_dir`` and return its path.

    Prefers ``uv build`` (no extra deps required) when ``uv`` is on PATH,
    otherwise falls back to ``python -m build`` which requires the ``build``
    PyPI package to be installed into the active interpreter.
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    if shutil.which("uv") is not None:
        run(
            ["uv", "build", "--wheel", "--out-dir", str(out_dir)],
            cwd=repo_root,
        )
    else:
        run(
            [
                sys.executable,
                "-m",
                "build",
                "--wheel",
                "--outdir",
                str(out_dir),
            ],
            cwd=repo_root,
        )
    wheels = sorted(out_dir.glob("deephaven_mcp-*.whl"))
    if not wheels:
        raise SystemExit("[build_pyapp] No wheel produced under " + str(out_dir))
    # The most recently built wheel; there should normally be exactly one.
    return wheels[-1]


def pbs_archive_name(python_version: str, pbs_release: str, target: Target) -> str:
    """Return the python-build-standalone asset filename for the target."""
    return (
        f"cpython-{python_version}+{pbs_release}-"
        f"{target.triple}-install_only.tar.gz"
    )


def pbs_download_url(python_version: str, pbs_release: str, target: Target) -> str:
    """Return the python-build-standalone download URL for the target."""
    return (
        "https://github.com/astral-sh/python-build-standalone/releases/"
        f"download/{pbs_release}/"
        f"{pbs_archive_name(python_version, pbs_release, target)}"
    )


def download_and_extract_pbs(
    python_version: str,
    pbs_release: str,
    target: Target,
    cache_dir: Path,
    extract_dir: Path,
) -> Path:
    """Download and extract a python-build-standalone archive.

    Returns the path to the top-level ``python`` directory inside the archive.
    """
    archive_name = pbs_archive_name(python_version, pbs_release, target)
    archive_path = cache_dir / archive_name
    if not archive_path.exists():
        download(pbs_download_url(python_version, pbs_release, target), archive_path)
    else:
        _LOGGER.info(f"[pbs] Using cached archive: {archive_path}")

    if extract_dir.exists():
        shutil.rmtree(extract_dir)
    extract_dir.mkdir(parents=True)

    _LOGGER.info(f"[pbs] Extracting to {extract_dir}")
    with tarfile.open(archive_path, "r:gz") as tf:
        tf.extractall(extract_dir)  # noqa: S202 - trusted upstream archive

    python_root = extract_dir / "python"
    if not python_root.is_dir():
        raise SystemExit(
            "[build_pyapp] Expected 'python/' directory inside PBS archive"
        )
    return python_root


def install_project_into_distribution(
    python_root: Path,
    wheel_path: Path,
    target: Target,
    extras: str,
) -> None:
    """Install the project wheel (with optional extras) into the bundled Python."""
    python_exe = python_root / target.python_exe_rel
    if not python_exe.exists():
        raise SystemExit(
            f"[build_pyapp] Python interpreter not found at {python_exe}"
        )

    # Make sure pip is up to date inside the bundled distribution.
    run([str(python_exe), "-m", "pip", "install", "--upgrade", "pip"])

    requirement = f"{wheel_path}[{extras}]" if extras else str(wheel_path)
    run(
        [
            str(python_exe),
            "-m",
            "pip",
            "install",
            "--no-warn-script-location",
            requirement,
        ]
    )


def repackage_distribution(python_root: Path, out_archive: Path) -> None:
    """Re-archive the (populated) distribution as a tar.gz suitable for PyApp.

    The resulting archive has a top-level ``python/`` directory matching the
    original layout, so ``PYAPP_DISTRIBUTION_PATH_PREFIX=python/`` works
    unchanged.
    """
    if out_archive.exists():
        out_archive.unlink()
    _LOGGER.info(f"[repack] Creating {out_archive}")
    with tarfile.open(out_archive, "w:gz") as tf:
        tf.add(python_root, arcname="python")


def get_project_version(wheel_path: Path) -> str:
    """Derive ``deephaven-mcp`` version from the wheel filename."""
    # Wheel filename: deephaven_mcp-<version>-py3-none-any.whl
    name = wheel_path.name
    parts = name.split("-")
    if len(parts) < 2:
        raise SystemExit(f"[build_pyapp] Cannot parse wheel name: {name}")
    return parts[1]


def python_minor_version(python_version: str) -> str:
    """Return ``major.minor`` from ``major.minor.patch``."""
    return ".".join(python_version.split(".")[:2])


def ensure_cargo() -> None:
    """Ensure ``cargo`` is available on PATH; exit with a friendly error otherwise."""
    if shutil.which("cargo") is None:
        raise SystemExit(
            "[build_pyapp] 'cargo' not found on PATH. Install the Rust toolchain "
            "from https://rustup.rs and retry."
        )


def build_one_binary(
    *,
    alias: str,
    binary_name: str,
    exec_spec: str,
    project_version: str,
    distribution_archive: Path,
    python_version: str,
    target: Target,
    pyapp_version: str,
    work_dir: Path,
    output_dir: Path,
) -> Path:
    """Build a single PyApp binary and return its path under ``output_dir``."""
    ensure_cargo()

    cargo_root = work_dir / f"pyapp-{alias}"
    target_dir = work_dir / f"cargo-target-{alias}"
    if cargo_root.exists():
        shutil.rmtree(cargo_root)

    minor = python_minor_version(python_version)
    env = os.environ.copy()
    env.update(
        {
            # Project identity (used as metadata only when skipping install).
            "PYAPP_PROJECT_NAME": "deephaven-mcp",
            "PYAPP_PROJECT_VERSION": project_version,
            # Execute the chosen console-script entry point.
            "PYAPP_EXEC_SPEC": exec_spec,
            # Embed the pre-populated Python distribution.
            "PYAPP_DISTRIBUTION_PATH": str(distribution_archive.resolve()),
            "PYAPP_DISTRIBUTION_PATH_PREFIX": "python/",
            "PYAPP_DISTRIBUTION_PYTHON_PATH": target.python_exe_rel,
            "PYAPP_DISTRIBUTION_SITE_PACKAGES_PATH": target.site_packages_rel.format(
                minor=minor
            ),
            "PYAPP_DISTRIBUTION_PIP_AVAILABLE": "1",
            # Fully offline: deps + project are already inside the distribution.
            "PYAPP_SKIP_INSTALL": "1",
            "PYAPP_FULL_ISOLATION": "1",
            # Force this build to use its own target dir so cached build.rs
            # output from a sibling binary doesn't shadow the new env vars.
            "CARGO_TARGET_DIR": str(target_dir),
        }
    )

    run(
        [
            "cargo",
            "install",
            "pyapp",
            "--version",
            pyapp_version,
            "--force",
            "--locked",
            "--root",
            str(cargo_root),
        ],
        env=env,
    )

    produced = cargo_root / "bin" / ("pyapp" + target.exe_suffix)
    if not produced.exists():
        raise SystemExit(f"[build_pyapp] cargo did not produce {produced}")

    output_dir.mkdir(parents=True, exist_ok=True)
    final = output_dir / f"{binary_name}{target.exe_suffix}"
    shutil.copy2(produced, final)
    if not target.is_windows:
        final.chmod(0o755)
    _LOGGER.info(f"[build_one_binary] Wrote {final}")
    return final


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--target",
        choices=sorted(_TARGETS),
        default=None,
        help="Target platform (default: auto-detect from host).",
    )
    parser.add_argument(
        "--python-version",
        default=DEFAULT_PYTHON_VERSION,
        help=f"CPython version to embed (default: {DEFAULT_PYTHON_VERSION}).",
    )
    parser.add_argument(
        "--pbs-release",
        default=DEFAULT_PBS_RELEASE,
        help=(
            "python-build-standalone release date tag, e.g. '20250902' "
            f"(default: {DEFAULT_PBS_RELEASE})."
        ),
    )
    parser.add_argument(
        "--pyapp-version",
        default=DEFAULT_PYAPP_VERSION,
        help=f"PyApp crate version (default: {DEFAULT_PYAPP_VERSION}).",
    )
    parser.add_argument(
        "--extras",
        default=DEFAULT_EXTRAS,
        help=(
            "Comma-separated optional dependency groups to bundle, or empty "
            f"to install no extras (default: '{DEFAULT_EXTRAS}')."
        ),
    )
    parser.add_argument(
        "--binaries",
        nargs="+",
        choices=sorted(BINARIES),
        default=sorted(BINARIES),
        help="Which binaries to build (default: all).",
    )
    parser.add_argument(
        "--output-dir",
        default="dist/pyapp",
        help="Where to place produced binaries (default: dist/pyapp).",
    )
    parser.add_argument(
        "--work-dir",
        default="build/pyapp",
        help="Scratch directory for intermediate artifacts (default: build/pyapp).",
    )
    parser.add_argument(
        "--keep-work",
        action="store_true",
        help="Do not delete the work directory on success.",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Enable debug logging.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    repo_root = Path(__file__).resolve().parent.parent
    target_alias = args.target or detect_host_target()
    target = _TARGETS[target_alias]
    _LOGGER.info(f"[main] Building for {target_alias} ({target.triple})")

    work_dir = (repo_root / args.work_dir).resolve()
    # Per-target subdirectory keeps filenames identical across architectures.
    base_output_dir = (repo_root / args.output_dir).resolve()
    output_dir = base_output_dir / target_alias
    cache_dir = work_dir / "cache"
    extract_dir = work_dir / "pbs" / target_alias
    wheel_dir = work_dir / "wheel"

    work_dir.mkdir(parents=True, exist_ok=True)
    cache_dir.mkdir(parents=True, exist_ok=True)

    wheel_path = build_project_wheel(repo_root, wheel_dir)
    _LOGGER.info(f"[main] Built wheel: {wheel_path}")

    python_root = download_and_extract_pbs(
        python_version=args.python_version,
        pbs_release=args.pbs_release,
        target=target,
        cache_dir=cache_dir,
        extract_dir=extract_dir,
    )

    install_project_into_distribution(
        python_root=python_root,
        wheel_path=wheel_path,
        target=target,
        extras=args.extras,
    )

    distribution_archive = work_dir / f"distribution-{target_alias}.tar.gz"
    repackage_distribution(python_root, distribution_archive)

    project_version = get_project_version(wheel_path)
    _LOGGER.info(f"[main] Project version: {project_version}")

    produced: list[Path] = []
    for alias in args.binaries:
        binary_name, exec_spec = BINARIES[alias]
        binary_path = build_one_binary(
            alias=alias,
            binary_name=binary_name,
            exec_spec=exec_spec,
            project_version=project_version,
            distribution_archive=distribution_archive,
            python_version=args.python_version,
            target=target,
            pyapp_version=args.pyapp_version,
            work_dir=work_dir,
            output_dir=output_dir,
        )
        produced.append(binary_path)

    _LOGGER.info("[main] Build complete. Produced binaries:")
    for path in produced:
        _LOGGER.info(f"  - {path}")

    # Emit a small JSON manifest alongside the binaries to aid release tooling.
    manifest = {
        "project": "deephaven-mcp",
        "version": project_version,
        "target": target_alias,
        "triple": target.triple,
        "python_version": args.python_version,
        "pbs_release": args.pbs_release,
        "pyapp_version": args.pyapp_version,
        "extras": args.extras,
        "binaries": [p.name for p in produced],
    }
    manifest_path = output_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n")
    _LOGGER.info(f"[main] Wrote manifest: {manifest_path}")

    # Bundle the per-target outputs into a single archive suitable for
    # distribution. Archive contents are flat (no leading directory) so users
    # can extract directly into a bin/ folder.
    archive_basename = (
        f"deephaven-mcp-pyapp-{project_version}-{target_alias}"
    )
    if target.is_windows:
        archive_path = base_output_dir / f"{archive_basename}.zip"
        if archive_path.exists():
            archive_path.unlink()
        with zipfile.ZipFile(archive_path, "w", zipfile.ZIP_DEFLATED) as zf:
            for item in sorted(output_dir.iterdir()):
                zf.write(item, arcname=item.name)
    else:
        archive_path = base_output_dir / f"{archive_basename}.tar.gz"
        if archive_path.exists():
            archive_path.unlink()
        with tarfile.open(archive_path, "w:gz") as tf:
            for item in sorted(output_dir.iterdir()):
                tf.add(item, arcname=item.name)
    _LOGGER.info(f"[main] Wrote distribution archive: {archive_path}")

    if not args.keep_work:
        # Keep the cache (slow to refetch) but drop the extracted PBS tree.
        shutil.rmtree(extract_dir, ignore_errors=True)

    return 0


if __name__ == "__main__":
    sys.exit(main())
