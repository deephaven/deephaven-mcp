#!/usr/bin/env python3
"""Build standalone, fully self-contained binaries for deephaven-mcp console scripts.

Each binary embeds a relocatable CPython together with the ``deephaven-mcp`` wheel and
every dependency, so it runs with no Python install and no network access — on the very
first launch, on a machine that has never seen the project.

The design follows from one fact about how PyApp works (verified against its
``build.rs``/``distribution.rs`` source): a PyApp binary embeds a Python distribution and
*one* project wheel, and on first run it ``pip install``s that wheel — fetching the wheel's
dependencies from a network index at that moment. PyApp cannot embed the dependency wheels.
The only way to make the binary need no network is to set ``PYAPP_SKIP_INSTALL`` and hand
PyApp a distribution whose ``site-packages`` *already* contains the project and all of its
dependencies. Producing that pre-populated distribution is what this script does:

1. ``uv build`` the wheel.
2. ``uv python install`` a managed, relocatable CPython, install the wheel and its
   dependencies into it with ``uv pip install``, and archive the tree.
3. ``cargo install pyapp`` once per console script, embedding that distribution with
   ``PYAPP_DISTRIBUTION_PATH`` + ``PYAPP_SKIP_INSTALL`` + ``PYAPP_FULL_ISOLATION``.

Build settings live in ``[tool.pyapp]`` of ``pyproject.toml`` (``python-version``,
``pyapp-version``, ``binaries``); each binary's entry point comes from
``[project.scripts]``. Cross-compilation is unsupported (step 2 installs
platform-specific wheels), so a build always targets the host machine.

Requirements: ``uv`` and a Rust toolchain (``cargo``) on ``PATH``, plus build-time
network access. Run ``uv run scripts/build_pyapp.py --help`` for options.
"""

from __future__ import annotations

import argparse
import logging
import os
import platform
import shutil
import subprocess
import sys
import tarfile
import tomllib
import zipfile
from dataclasses import dataclass
from pathlib import Path

_LOGGER = logging.getLogger("build_pyapp")

_OS_ALIASES = {"Linux": "linux", "Darwin": "macos", "Windows": "windows"}
_ARCH_ALIASES = {
    "x86_64": "x86_64",
    "amd64": "x86_64",
    "aarch64": "aarch64",
    "arm64": "aarch64",
}


# --- Low-level helpers -------------------------------------------------------------------


def run(cmd: list[str], *, env: dict[str, str] | None = None) -> None:
    """Run a subprocess command, streaming its output, and raise on failure.

    Args:
        cmd (list[str]): The command argv to execute.
        env (dict[str, str] | None): Environment for the child process, or ``None`` to
            inherit the current environment.

    Raises:
        subprocess.CalledProcessError: If the command exits non-zero.
    """
    _LOGGER.info(f"[run] $ {' '.join(cmd)}")
    # ``cmd`` is a list (no ``shell=True``), which prevents shell injection; ``S603`` is
    # bandit's generic subprocess warning.
    subprocess.run(cmd, env=env, check=True, text=True)  # noqa: S603


def _write_tar_gz(src_dir: Path, dest: Path) -> None:
    """Write the contents of ``src_dir`` into ``dest`` as a flat ``.tar.gz``.

    Args:
        src_dir (Path): Directory whose entries are archived (no wrapping directory).
        dest (Path): Output archive path; overwritten if present.
    """
    dest.unlink(missing_ok=True)
    with tarfile.open(dest, "w:gz") as tf:
        for item in sorted(src_dir.iterdir()):
            tf.add(item, arcname=item.name)


def _write_zip(src_dir: Path, dest: Path) -> None:
    """Write the contents of ``src_dir`` into ``dest`` as a flat ``.zip``.

    Args:
        src_dir (Path): Directory whose entries are archived (no wrapping directory).
        dest (Path): Output archive path; overwritten if present.
    """
    dest.unlink(missing_ok=True)
    with zipfile.ZipFile(dest, "w", zipfile.ZIP_DEFLATED) as zf:
        for item in sorted(src_dir.iterdir()):
            zf.write(item, arcname=item.name)


# --- Value types -------------------------------------------------------------------------


@dataclass(frozen=True)
class PythonVersion:
    """A requested CPython version (e.g. ``3.12`` or ``3.12.13``)."""

    requested: str
    """The version string as configured and passed to ``uv python install``."""

    @property
    def minor(self) -> str:
        """The ``major.minor`` prefix of :attr:`requested` (e.g. ``3.12``)."""
        return ".".join(self.requested.split(".")[:2])


@dataclass(frozen=True)
class Host:
    """The platform being built for (always the runner; cross-compilation is unsupported)."""

    alias: str
    """Short platform name used in output directory and archive names (e.g. ``macos-aarch64``)."""

    is_windows: bool
    """Whether this host is Windows (selects executable suffix, distribution paths, archive format)."""

    @classmethod
    def detect(cls) -> Host:
        """Return the :class:`Host` for the current machine.

        Returns:
            Host: The detected host.

        Raises:
            SystemExit: If the platform or architecture is unsupported.
        """
        system = platform.system()
        machine = platform.machine().lower()
        os_alias = _OS_ALIASES.get(system)
        arch_alias = _ARCH_ALIASES.get(machine)
        if os_alias is None or arch_alias is None:
            raise SystemExit(
                f"[build_pyapp] Unsupported host platform: {system}/{machine}"
            )
        return cls(alias=f"{os_alias}-{arch_alias}", is_windows=os_alias == "windows")

    @property
    def exe_suffix(self) -> str:
        """Suffix for executables: ``.exe`` on Windows, else empty."""
        return ".exe" if self.is_windows else ""

    @property
    def python_exe_rel(self) -> str:
        """Relative path to the interpreter inside the distribution."""
        return "python.exe" if self.is_windows else "bin/python3"

    def site_packages_rel(self, version: PythonVersion) -> str:
        """Return the ``site-packages`` path relative to the distribution root.

        Args:
            version (PythonVersion): The embedded Python version.

        Returns:
            str: The relative ``site-packages`` path for this host.
        """
        return (
            "Lib/site-packages"
            if self.is_windows
            else f"lib/python{version.minor}/site-packages"
        )

    def archive(self, src_dir: Path, dest_stem: Path) -> Path:
        """Archive ``src_dir`` flat in this host's native shipping format.

        Writes a ``.zip`` on Windows and a ``.tar.gz`` elsewhere, storing the directory's
        entries flat (no wrapping directory).

        Args:
            src_dir (Path): Directory whose contents are archived.
            dest_stem (Path): Output path without an extension; the format-specific
                extension is appended.

        Returns:
            Path: The created archive.
        """
        if self.is_windows:
            dest = dest_stem.with_name(dest_stem.name + ".zip")
            _write_zip(src_dir, dest)
        else:
            dest = dest_stem.with_name(dest_stem.name + ".tar.gz")
            _write_tar_gz(src_dir, dest)
        return dest


@dataclass(frozen=True)
class BuildConfig:
    """Build settings read from ``[tool.pyapp]`` in ``pyproject.toml``."""

    project_name: str
    """Distribution name (``[project].name``), used as ``PYAPP_PROJECT_NAME``."""

    python_version: str
    """CPython version to embed (e.g. ``3.12``)."""

    pyapp_version: str
    """PyApp crate version to compile (e.g. ``0.29.0``)."""

    binaries: dict[str, str]
    """Binary name -> entry point (``PYAPP_EXEC_SPEC``), resolved from ``[project.scripts]``."""

    @classmethod
    def from_pyproject(cls, repo_root: Path) -> BuildConfig:
        """Read the ``[tool.pyapp]`` build settings from ``pyproject.toml``.

        ``binaries`` lists the console scripts to ship; each one's entry point is taken
        from ``[project.scripts]`` (whose ``module:function`` value is PyApp's
        ``EXEC_SPEC``).

        Args:
            repo_root (Path): Repository root containing ``pyproject.toml``.

        Returns:
            BuildConfig: The parsed build settings.

        Raises:
            SystemExit: If ``[tool.pyapp].binaries`` is empty or names a script missing
                from ``[project.scripts]``.
        """
        with open(repo_root / "pyproject.toml", "rb") as f:
            data = tomllib.load(f)
        project = data.get("project", {})
        pyapp = data.get("tool", {}).get("pyapp", {})
        scripts = project.get("scripts", {})
        names = pyapp.get("binaries")
        if not names:
            raise SystemExit(
                "[build_pyapp] pyproject.toml [tool.pyapp].binaries is empty"
            )
        missing = [name for name in names if name not in scripts]
        if missing:
            raise SystemExit(
                f"[build_pyapp] [tool.pyapp].binaries not in [project.scripts]: {missing}"
            )
        return cls(
            project_name=project["name"],
            python_version=pyapp["python-version"],
            pyapp_version=pyapp["pyapp-version"],
            binaries={name: scripts[name] for name in names},
        )


# --- Build steps -------------------------------------------------------------------------


def build_wheel(repo_root: Path, out_dir: Path) -> Path:
    """Build the ``deephaven-mcp`` wheel into ``out_dir``.

    Args:
        repo_root (Path): Repository root to build from.
        out_dir (Path): Directory to write the wheel into; created if absent.

    Returns:
        Path: The built wheel.

    Raises:
        SystemExit: If no wheel is produced.
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    run(["uv", "build", "--wheel", str(repo_root), "--out-dir", str(out_dir)])
    wheels = sorted(out_dir.glob("deephaven_mcp-*.whl"))
    if not wheels:
        raise SystemExit(f"[build_pyapp] No wheel produced under {out_dir}")
    return wheels[-1]


def project_version_from_wheel(wheel_path: Path) -> str:
    """Return the ``deephaven-mcp`` version parsed from a wheel filename.

    Args:
        wheel_path (Path): Path to a ``deephaven_mcp-<version>-...`` wheel.

    Returns:
        str: The version segment of the wheel filename.

    Raises:
        SystemExit: If the filename does not start with the expected prefix.
    """
    # PEP 427 wheel name: {distribution}-{version}-...; stripping the known distribution
    # prefix yields the version up to the next '-'.
    prefix = "deephaven_mcp-"
    name = wheel_path.name
    if not name.startswith(prefix):
        raise SystemExit(f"[build_pyapp] Unexpected wheel name: {name}")
    return name[len(prefix) :].split("-", 1)[0]


def install_python(version: PythonVersion, install_dir: Path) -> Path:
    """Download a managed CPython into ``install_dir`` and return its tree root.

    The interpreter is always a managed python-build-standalone download pinned to
    ``version`` — ``--managed-python`` keeps uv from ever selecting the system Python — so
    the result is a complete, relocatable interpreter + standard library suitable for
    embedding.

    Args:
        version (PythonVersion): CPython version to install.
        install_dir (Path): Throwaway directory to install into; recreated each call.

    Returns:
        Path: The distribution root (the ``cpython-<full-version>-<platform>`` directory).

    Raises:
        SystemExit: If uv does not produce exactly one matching interpreter.
    """
    # Start from an empty directory so the post-install discovery below is unambiguous.
    if install_dir.exists():
        shutil.rmtree(install_dir)

    # Download a managed CPython pinned to the configured version. --managed-python keeps
    # uv from ever falling back to the system Python; --no-bin skips launcher shims.
    run(
        [
            "uv",
            "python",
            "install",
            "--managed-python",
            "--no-bin",
            "--install-dir",
            str(install_dir),
            version.requested,
        ]
    )

    # uv lays the interpreter out as cpython-<full-version>-<platform>/; matching a patch
    # digit skips the cpython-<minor>-<platform>/ alias. The freshly-cleaned directory
    # must hold exactly one such tree -- any other count means uv behaved unexpectedly.
    installs = sorted(
        p for p in install_dir.glob(f"cpython-{version.minor}.*") if p.is_dir()
    )
    if len(installs) != 1:
        raise SystemExit(
            f"[build_pyapp] expected exactly one cpython-{version.minor}.* install under "
            f"{install_dir}, found {[p.name for p in installs]}"
        )
    return installs[0]


def install_project(
    dist_root: Path, host: Host, version: PythonVersion, wheel_path: Path
) -> None:
    """Install the project wheel and all dependencies into a distribution.

    Args:
        dist_root (Path): Distribution root from :func:`install_python`.
        host (Host): Target host (selects interpreter and site-packages paths).
        version (PythonVersion): The embedded Python version.
        wheel_path (Path): The ``deephaven-mcp`` wheel to install.
    """
    # uv stamps its managed interpreters with a PEP 668 EXTERNALLY-MANAGED marker to stop
    # callers from mutating the shared install. This tree is a private, disposable copy we
    # are deliberately turning into a distribution, so remove the marker (it sits in the
    # stdlib dir, the parent of site-packages); uv then installs with --system and no
    # --break-system-packages override.
    marker = (
        dist_root / Path(host.site_packages_rel(version)).parent / "EXTERNALLY-MANAGED"
    )
    marker.unlink(missing_ok=True)
    run(
        [
            "uv",
            "pip",
            "install",
            "--python",
            str(dist_root / host.python_exe_rel),
            "--system",
            str(wheel_path),
        ]
    )


def archive_distribution(dist_root: Path, archive_path: Path) -> Path:
    """Archive a distribution tree flat into ``archive_path`` as a ``.tar.gz``.

    Always a ``.tar.gz`` regardless of host — PyApp reads tar.gz distributions on every
    platform. The archive's top level is the distribution root itself (``bin/``, ``lib/``,
    ...) with no wrapping directory, so PyApp's ``DISTRIBUTION_PYTHON_PATH`` /
    ``SITE_PACKAGES_PATH`` (relative to the unpack root) resolve directly and
    ``PYAPP_DISTRIBUTION_PATH_PREFIX`` is not needed.

    Args:
        dist_root (Path): Distribution root to archive.
        archive_path (Path): Output ``.tar.gz`` path; overwritten if present.

    Returns:
        Path: ``archive_path``.
    """
    _LOGGER.info(f"[archive] Writing distribution to {archive_path}")
    _write_tar_gz(dist_root, archive_path)
    return archive_path


def build_distribution(
    version: PythonVersion, host: Host, wheel_path: Path, work_dir: Path
) -> Path:
    """Build a self-contained, pre-populated Python distribution and archive it.

    Produces the artifact PyApp embeds: a relocatable CPython whose ``site-packages``
    already contains the project and every dependency, so PyApp runs it under
    ``PYAPP_SKIP_INSTALL`` with no network. A ``uv venv`` cannot be used here — its
    ``bin/python`` is a symlink to an external base interpreter and its tree omits the
    standard library, so an embedded venv dangles on a clean machine.

    Args:
        version (PythonVersion): CPython version to embed.
        host (Host): Target host.
        wheel_path (Path): The ``deephaven-mcp`` wheel to install.
        work_dir (Path): Scratch directory for the interpreter and the archive.

    Returns:
        Path: The distribution ``.tar.gz``.
    """
    dist_root = install_python(version, work_dir / "py")
    install_project(dist_root, host, version, wheel_path)
    return archive_distribution(dist_root, work_dir / "distribution.tar.gz")


def pyapp_build_env(
    *,
    exec_spec: str,
    project_name: str,
    project_version: str,
    distribution_archive: Path,
    host: Host,
    version: PythonVersion,
) -> dict[str, str]:
    """Return the ``PYAPP_*`` build variables that encode the offline contract.

    Together these tell PyApp to embed the pre-populated distribution and run it without
    any install or download.

    Args:
        exec_spec (str): The ``module:function`` entry point the binary runs.
        project_name (str): Distribution name (``PYAPP_PROJECT_NAME``).
        project_version (str): Project version (``PYAPP_PROJECT_VERSION``).
        distribution_archive (Path): The pre-populated distribution archive to embed.
        host (Host): Target host (selects interpreter and site-packages paths).
        version (PythonVersion): The embedded Python version.

    Returns:
        dict[str, str]: Environment overrides to pass to ``cargo install pyapp``.
    """
    return {
        # build.rs reads the project identity from these even under SKIP_INSTALL, and
        # panics without them when no PYAPP_PROJECT_PATH wheel is embedded.
        "PYAPP_PROJECT_NAME": project_name,
        "PYAPP_PROJECT_VERSION": project_version,
        "PYAPP_EXEC_SPEC": exec_spec,
        # A local distribution path implicitly enables embedding.
        "PYAPP_DISTRIBUTION_PATH": str(distribution_archive.resolve()),
        # Required for a custom distribution: PyApp's built-in default paths apply only to
        # distributions it downloads itself, and build.rs panics without these.
        "PYAPP_DISTRIBUTION_PYTHON_PATH": host.python_exe_rel,
        "PYAPP_DISTRIBUTION_SITE_PACKAGES_PATH": host.site_packages_rel(version),
        # Skip the first-run pip install (the deps are already present), and run from a
        # full copy of the distribution rather than a virtualenv so its site-packages deps
        # are importable (without FULL_ISOLATION PyApp builds a venv lacking them, causing
        # ModuleNotFoundError; verified empirically).
        "PYAPP_SKIP_INSTALL": "true",
        "PYAPP_FULL_ISOLATION": "true",
    }


def build_binary(
    *,
    binary_name: str,
    exec_spec: str,
    project_name: str,
    project_version: str,
    version: PythonVersion,
    pyapp_version: str,
    distribution_archive: Path,
    host: Host,
    work_dir: Path,
    output_dir: Path,
) -> Path:
    """Compile one PyApp binary with the distribution embedded.

    Args:
        binary_name (str): Output binary name (e.g. ``dhcli``).
        exec_spec (str): The ``module:function`` entry point.
        project_name (str): Distribution name (``PYAPP_PROJECT_NAME``).
        project_version (str): Project version (``PYAPP_PROJECT_VERSION``).
        version (PythonVersion): The embedded Python version.
        pyapp_version (str): PyApp crate version to compile.
        distribution_archive (Path): Pre-populated distribution to embed.
        host (Host): Target host.
        work_dir (Path): Scratch directory for the cargo install root.
        output_dir (Path): Directory the finished binary is copied into.

    Returns:
        Path: The finished binary under ``output_dir``.

    Raises:
        SystemExit: If cargo does not produce the expected binary.
    """
    cargo_root = work_dir / f"pyapp-{binary_name}"
    if cargo_root.exists():
        shutil.rmtree(cargo_root)

    env = os.environ.copy()
    env.update(
        pyapp_build_env(
            exec_spec=exec_spec,
            project_name=project_name,
            project_version=project_version,
            distribution_archive=distribution_archive,
            host=host,
            version=version,
        )
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

    produced = cargo_root / "bin" / ("pyapp" + host.exe_suffix)
    if not produced.exists():
        raise SystemExit(f"[build_pyapp] cargo did not produce {produced}")

    output_dir.mkdir(parents=True, exist_ok=True)
    final = output_dir / f"{binary_name}{host.exe_suffix}"
    shutil.copy2(produced, final)  # copy2 preserves the executable mode
    _LOGGER.info(f"[build_binary] Wrote {final}")
    return final


def package_archive(
    output_dir: Path, base_output_dir: Path, project_version: str, host: Host
) -> Path:
    """Bundle the host's binaries into one flat distribution archive.

    Args:
        output_dir (Path): Directory holding the built binaries.
        base_output_dir (Path): Directory to write the archive into.
        project_version (str): Version used in the archive name.
        host (Host): Target host (selects archive format and name).

    Returns:
        Path: The written archive.
    """
    basename = f"deephaven-mcp-pyapp-{project_version}-{host.alias}"
    archive = host.archive(output_dir, base_output_dir / basename)
    _LOGGER.info(f"[package] Wrote distribution archive: {archive}")
    return archive


# --- Orchestration -----------------------------------------------------------------------


def select_binaries(config: BuildConfig, requested: list[str] | None) -> dict[str, str]:
    """Return the binary -> entry-point map to build, applying an optional subset.

    Args:
        config (BuildConfig): The parsed build settings.
        requested (list[str] | None): Subset of binary names to build, or ``None`` for all.

    Returns:
        dict[str, str]: The selected binary -> entry-point mapping.

    Raises:
        SystemExit: If ``requested`` names a binary absent from ``[tool.pyapp].binaries``.
    """
    if requested is None:
        return config.binaries
    unknown = [name for name in requested if name not in config.binaries]
    if unknown:
        raise SystemExit(
            f"[build_pyapp] --binaries {unknown} not in [tool.pyapp].binaries "
            f"{list(config.binaries)}"
        )
    return {name: config.binaries[name] for name in requested}


def run_build(
    repo_root: Path,
    host: Host,
    config: BuildConfig,
    version: PythonVersion,
    binaries: dict[str, str],
    *,
    keep_work: bool,
) -> list[Path]:
    """Build the wheel and distribution, compile each binary, and package the result.

    Args:
        repo_root (Path): Repository root to build from.
        host (Host): Target host.
        config (BuildConfig): The parsed build settings.
        version (PythonVersion): CPython version to embed.
        binaries (dict[str, str]): Binary -> entry-point mapping to build.
        keep_work (bool): If true, leave the scratch ``build/pyapp`` directory in place.

    Returns:
        list[Path]: The finished binaries.
    """
    work_dir = (repo_root / "build" / "pyapp").resolve()
    base_output_dir = (repo_root / "dist" / "pyapp").resolve()
    output_dir = base_output_dir / host.alias
    work_dir.mkdir(parents=True, exist_ok=True)

    wheel_path = build_wheel(repo_root, work_dir / "wheel")
    project_version = project_version_from_wheel(wheel_path)
    _LOGGER.info(f"[build] Built wheel for {config.project_name} {project_version}")

    distribution_archive = build_distribution(version, host, wheel_path, work_dir)

    produced = [
        build_binary(
            binary_name=name,
            exec_spec=exec_spec,
            project_name=config.project_name,
            project_version=project_version,
            version=version,
            pyapp_version=config.pyapp_version,
            distribution_archive=distribution_archive,
            host=host,
            work_dir=work_dir,
            output_dir=output_dir,
        )
        for name, exec_spec in binaries.items()
    ]

    package_archive(output_dir, base_output_dir, project_version, host)

    # Keep the scratch tree on failure (it never reaches here) so debugging artifacts
    # survive; only a clean run removes it.
    if not keep_work:
        shutil.rmtree(work_dir, ignore_errors=True)
    return produced


# --- Command-line interface --------------------------------------------------------------


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse command-line arguments.

    Args:
        argv (list[str] | None): Arguments to parse, or ``None`` to use ``sys.argv``.

    Returns:
        argparse.Namespace: The parsed arguments.
    """
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--python-version",
        default=None,
        help="Override the embedded CPython version (default: [tool.pyapp].python-version).",
    )
    parser.add_argument(
        "--binaries",
        nargs="+",
        default=None,
        metavar="NAME",
        help="Subset of [tool.pyapp].binaries to build (default: all).",
    )
    parser.add_argument(
        "--keep-work",
        action="store_true",
        help="Do not delete the work directory on success.",
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true", help="Enable debug logging."
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Build the requested PyApp binaries for the host platform.

    Args:
        argv (list[str] | None): Command-line arguments, or ``None`` for ``sys.argv``.

    Returns:
        int: Process exit code (``0`` on success).

    Raises:
        SystemExit: On an unsupported host or an unknown ``--binaries`` selection.
    """
    args = parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    repo_root = Path(__file__).resolve().parent.parent
    host = Host.detect()
    config = BuildConfig.from_pyproject(repo_root)
    binaries = select_binaries(config, args.binaries)
    version = PythonVersion(args.python_version or config.python_version)

    _LOGGER.info(
        f"[main] Building {host.alias} with embedded Python {version.requested}"
    )
    produced = run_build(
        repo_root, host, config, version, binaries, keep_work=args.keep_work
    )

    _LOGGER.info("[main] Build complete. Produced binaries:")
    for path in produced:
        _LOGGER.info(f"  - {path}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
