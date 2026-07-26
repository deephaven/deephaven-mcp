"""End-to-end integration tests for the ``dhcli config`` authoring surface.

These tests drive the real ``dhcli`` binary as an OS subprocess and
exercise the offline configuration-authoring verbs — ``config
get/set/unset/keys/files/validate/show``, ``config session
add/list/remove``, ``config system add/list/remove``, and the
interactive-only refusal paths of ``config init``/``config edit`` — the
verbs the in-process ``CliRunner`` unit tests
(``tests/cli/_commands/test_config.py``) cannot prove at the real
argv / entry-point / filesystem level. No daemon, worker, or Java is
required: every verb here operates purely on configuration files.

Isolation (two layers, so a developer's real ``~/.deephaven/ai`` is
unreachable):

- Explicit ``--config-dir`` / ``--runtime-dir`` flags at pytest
  ``tmp_path`` (the established integration-test pattern).
- ``DH_AI_DATA_DIR`` in every subprocess environment pointing at a
  sandbox directory, so even a code path that ignored the flags would
  resolve inside the sandbox.

They are marked ``@pytest.mark.integration`` and skipped by the
default ``uv run pytest`` run. Invoke with::

    uv run pytest -s -m integration tests/cli/test__config_integration.py

Prerequisites:

- ``dhcli`` must be on ``$PATH`` (provided by ``uv sync``).

The daemon-backed CLI flows live in ``test__daemon_integration.py``;
this file deliberately never starts the daemon.
"""

from __future__ import annotations

import json
import logging
import os
import shutil
import stat
import subprocess
from pathlib import Path

import pytest

_LOGGER = logging.getLogger(__name__)

_requires_dhcli = pytest.mark.skipif(
    shutil.which("dhcli") is None,
    reason="dhcli entry point not on PATH",
)


def _sandbox_env(sandbox: Path) -> dict[str, str]:
    """Build the subprocess environment for a sandboxed invocation.

    ``DH_AI_DATA_DIR`` points every default-path resolution inside the
    sandbox (belt-and-suspenders on top of the explicit ``--config-dir``
    / ``--runtime-dir`` flags), and ``DHCLI_OUTPUT`` is stripped so a
    developer's shell setting cannot change the asserted output mode.
    """
    env = dict(os.environ)
    env["DH_AI_DATA_DIR"] = str(sandbox / "data-root")
    env.pop("DHCLI_OUTPUT", None)
    return env


def _run_cli(
    args: list[str],
    *,
    sandbox: Path,
    explicit_dirs: bool = True,
    root_flags: list[str] | None = None,
    env_extra: dict[str, str] | None = None,
    timeout: int = 60,
) -> subprocess.CompletedProcess[str]:
    """Drive the ``dhcli`` CLI as a subprocess inside ``sandbox``.

    ``stdin`` is ``/dev/null`` so ``can_prompt`` deterministically
    reports no TTY regardless of how pytest itself was invoked; the
    interactive-refusal tests depend on this.
    """
    config_dir = sandbox / "cfg"
    runtime_dir = sandbox / "rt"
    cmd = ["dhcli"]
    if explicit_dirs:
        cmd += ["--config-dir", str(config_dir), "--runtime-dir", str(runtime_dir)]
    cmd += ["--output", "json", *(root_flags or []), *args]
    env = _sandbox_env(sandbox)
    if env_extra:
        env.update(env_extra)
    _LOGGER.info(f"[integration:_run_cli] {' '.join(cmd)}")
    return subprocess.run(  # noqa: S603 - argv is fully constructed locally
        cmd,
        capture_output=True,
        text=True,
        check=False,
        timeout=timeout,
        env=env,
        stdin=subprocess.DEVNULL,
    )


def _structured_error(stderr: str) -> dict:
    """Extract the structured-error JSON object emitted on stderr.

    The CLI writes the ``-o json`` error payload to stderr, but the
    root logger also targets stderr; tests that read the payload run
    with ``-q`` to silence non-error logs, and this helper additionally
    slices to the brace-delimited object so any stray line cannot break
    the parse.
    """
    start = stderr.index("{")
    end = stderr.rindex("}") + 1
    return json.loads(stderr[start:end])


def _mode(path: Path) -> int:
    """Return the permission bits of ``path``."""
    return stat.S_IMODE(os.stat(path).st_mode)


def _assert_inside(path: Path, sandbox: Path) -> None:
    """Assert a CLI-reported path landed inside the sandbox."""
    assert path.resolve().is_relative_to(
        sandbox.resolve()
    ), f"{path} escaped the sandbox {sandbox}"


# ---------------------------------------------------------------------------
# greenfield field authoring: files → keys → set → get → validate → show → unset
# ---------------------------------------------------------------------------


@pytest.mark.integration
@pytest.mark.timeout(300)
@_requires_dhcli
def test_greenfield_field_authoring_lifecycle(tmp_path: Path) -> None:
    """Author a configuration from nothing using only field verbs.

    Starts with no configuration directory at all and walks the
    discover → write → read → validate → revert loop, asserting the
    0o700/0o600 permission contract on everything the store creates.
    """
    cfg_dir = tmp_path / "cfg"

    # files: works on a completely absent tree; nothing exists yet.
    result = _run_cli(["config", "files"], sandbox=tmp_path)
    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout)
    assert payload["config_dir"] == str(cfg_dir)
    assert payload["files"], "expected at least the unnamed file kinds"
    assert all(not entry["exists"] for entry in payload["files"])

    # keys: schema-generated settable paths are discoverable up front.
    result = _run_cli(["config", "keys"], sandbox=tmp_path)
    assert result.returncode == 0, result.stderr
    keys_payload = json.loads(result.stdout)
    assert keys_payload["keys"], "expected a non-empty settable-path list"
    assert "cli.output.format" in json.dumps(keys_payload)

    # set: first write creates cfg_dir and cli.json with private modes.
    result = _run_cli(["config", "set", "cli.output.format=human"], sandbox=tmp_path)
    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout)
    assert payload["paths"] == ["cli.output.format"]
    written = Path(payload["files"][0])
    _assert_inside(written, tmp_path)
    assert written.is_file()
    assert _mode(cfg_dir) == 0o700
    assert _mode(written) == 0o600

    # get: reads back the raw on-disk value.
    result = _run_cli(["config", "get", "cli.output.format"], sandbox=tmp_path)
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == "human"

    # A cli.json-only (zero-system) tree is valid: validation no longer
    # requires a servable system.
    result = _run_cli(["config", "validate"], sandbox=tmp_path)
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout)["valid"] is True

    # Author a minimal community settings block so 'show' has a system
    # to display — still using only field verbs.
    result = _run_cli(
        [
            "config",
            "set",
            "community.settings.session_creation.max_concurrent_sessions=2",
        ],
        sandbox=tmp_path,
    )
    assert result.returncode == 0, result.stderr

    # validate: the authored tree passes the full pre-body load.
    result = _run_cli(["config", "validate"], sandbox=tmp_path)
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout)["valid"] is True

    # show: the resolved view includes both authored sections. (The
    # root '--output json' flag overrides cli.output.format in the
    # effective view, so assert on the un-overridden community field.)
    result = _run_cli(["config", "show"], sandbox=tmp_path)
    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout)
    assert "cli" in payload
    settings = payload["community"]["settings"]
    assert settings["session_creation"]["max_concurrent_sessions"] == 2

    # unset: the field reverts to its schema default (absent on disk).
    result = _run_cli(["config", "unset", "cli.output.format"], sandbox=tmp_path)
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout)["paths"] == ["cli.output.format"]
    result = _run_cli(
        ["config", "get", "cli.output.format"],
        sandbox=tmp_path,
        root_flags=["-q"],
    )
    assert result.returncode == 2, (result.stdout, result.stderr)
    assert _structured_error(result.stderr)["error_code"] == "not_found"


@pytest.mark.integration
@pytest.mark.timeout(300)
@_requires_dhcli
def test_validate_empty_config_dir_is_valid(tmp_path: Path) -> None:
    """An empty (but existing) config dir is a valid zero-system tree.

    ``config validate`` checks validity, not servability: the
    no-systems invariant is enforced only where a system is required
    (systems-server startup and CLI daemon acquisition), so validation
    of a half-finished tree succeeds.
    """
    cfg_dir = tmp_path / "cfg"
    cfg_dir.mkdir(mode=0o700)
    result = _run_cli(["config", "validate"], sandbox=tmp_path)
    assert result.returncode == 0, (result.stdout, result.stderr)
    assert json.loads(result.stdout)["valid"] is True


# ---------------------------------------------------------------------------
# session entity lifecycle: add → list → set → validate → remove
# ---------------------------------------------------------------------------


@pytest.mark.integration
@pytest.mark.timeout(300)
@_requires_dhcli
def test_session_entity_lifecycle(tmp_path: Path) -> None:
    """Declare, edit, and remove a community session end to end."""
    result = _run_cli(
        [
            "config",
            "session",
            "add",
            "ci",
            "--host",
            "127.0.0.1",
            "--port",
            "10000",
            "--auth",
            "anonymous",
        ],
        sandbox=tmp_path,
    )
    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout)
    assert payload["name"] == "ci"
    assert payload["path"] == "community.sessions.ci"
    session_file = Path(payload["file"])
    _assert_inside(session_file, tmp_path)
    assert _mode(session_file) == 0o600
    assert _mode(session_file.parent) == 0o700
    data = json.loads(session_file.read_text())
    assert data["host"] == "127.0.0.1"
    assert data["port"] == 10000
    assert data["auth"]["credentials"]["type"] == "anonymous"

    # add refuses to overwrite an existing session.
    result = _run_cli(
        ["config", "session", "add", "ci", "--auth", "anonymous"],
        sandbox=tmp_path,
        root_flags=["-q"],
    )
    assert result.returncode == 2, (result.stdout, result.stderr)
    assert _structured_error(result.stderr)["error_code"] == "already_exists"

    # list shows the declared session as valid.
    result = _run_cli(["config", "session", "list"], sandbox=tmp_path)
    assert result.returncode == 0, result.stderr
    sessions = json.loads(result.stdout)["sessions"]
    assert [s["name"] for s in sessions] == ["ci"]
    assert sessions[0]["valid"] is True

    # set edits a field inside the entity file.
    result = _run_cli(
        ["config", "set", "community.sessions.ci.port=10500"], sandbox=tmp_path
    )
    assert result.returncode == 0, result.stderr
    result = _run_cli(["config", "get", "community.sessions.ci.port"], sandbox=tmp_path)
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == 10500

    # The mutated tree still passes the full validation load.
    result = _run_cli(["config", "validate"], sandbox=tmp_path)
    assert result.returncode == 0, result.stderr

    # remove without --yes on a non-TTY is a structured refusal.
    result = _run_cli(
        ["config", "session", "remove", "ci"],
        sandbox=tmp_path,
        root_flags=["-q"],
    )
    assert result.returncode == 2, (result.stdout, result.stderr)
    error = _structured_error(result.stderr)
    assert error["error_code"] == "missing_required_option"
    assert session_file.is_file(), "refusal must not delete the file"

    # remove --yes deletes the file; list is empty again.
    result = _run_cli(["config", "session", "remove", "ci", "--yes"], sandbox=tmp_path)
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout)["name"] == "ci"
    assert not session_file.exists()
    result = _run_cli(["config", "session", "list"], sandbox=tmp_path)
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout)["sessions"] == []


# ---------------------------------------------------------------------------
# system entity lifecycle: add → list → show redaction → remove
# ---------------------------------------------------------------------------


@pytest.mark.integration
@pytest.mark.timeout(300)
@_requires_dhcli
def test_system_entity_lifecycle(tmp_path: Path) -> None:
    """Declare, inspect, and remove an enterprise system end to end.

    The password is a ``${env:...}`` templating ref stored verbatim;
    the resolving views run with the variable set in the subprocess
    environment and must never leak the resolved secret.
    """
    secret = "s3cret-integration-password"
    env_extra = {"CI_E2E_DHE_PASSWORD": secret}
    result = _run_cli(
        [
            "config",
            "system",
            "add",
            "stg",
            "--url",
            "https://stg.example.com/iris/connection.json",
            "--auth",
            "password",
            "--username",
            "alice",
            "--password",
            "${env:CI_E2E_DHE_PASSWORD}",
        ],
        sandbox=tmp_path,
        env_extra=env_extra,
    )
    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout)
    assert payload["name"] == "stg"
    assert payload["path"] == "enterprise.systems.stg"
    system_file = Path(payload["file"])
    _assert_inside(system_file, tmp_path)
    assert _mode(system_file) == 0o600
    data = json.loads(system_file.read_text())
    assert data["connection_json_url"] == "https://stg.example.com/iris/connection.json"
    credentials = data["auth"]["credentials"]
    assert credentials["type"] == "password"
    assert credentials["username"] == "alice"
    # Templating refs are stored verbatim, not resolved at write time.
    assert credentials["password"] == "${env:CI_E2E_DHE_PASSWORD}"

    # list shows the declared system as valid.
    result = _run_cli(
        ["config", "system", "list"], sandbox=tmp_path, env_extra=env_extra
    )
    assert result.returncode == 0, result.stderr
    systems = json.loads(result.stdout)["systems"]
    assert [s["name"] for s in systems] == ["stg"]
    assert systems[0]["valid"] is True

    # validate + show: the tree loads with the env ref resolvable, and
    # the redacted resolved view never leaks the secret.
    result = _run_cli(["config", "validate"], sandbox=tmp_path, env_extra=env_extra)
    assert result.returncode == 0, result.stderr
    result = _run_cli(["config", "show"], sandbox=tmp_path, env_extra=env_extra)
    assert result.returncode == 0, result.stderr
    assert secret not in result.stdout
    assert "stg" in result.stdout

    # remove --yes deletes the declaration file.
    result = _run_cli(
        ["config", "system", "remove", "stg", "--yes"],
        sandbox=tmp_path,
        env_extra=env_extra,
    )
    assert result.returncode == 0, result.stderr
    assert not system_file.exists()
    result = _run_cli(
        ["config", "system", "list"], sandbox=tmp_path, env_extra=env_extra
    )
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout)["systems"] == []


# ---------------------------------------------------------------------------
# interactive-only verbs refuse deterministically without a TTY
# ---------------------------------------------------------------------------


@pytest.mark.integration
@pytest.mark.timeout(300)
@_requires_dhcli
def test_interactive_only_verbs_refuse_without_tty(tmp_path: Path) -> None:
    """``init``/``edit`` fail with ``no_tty``; prompting verbs name the flag.

    stdin is ``/dev/null`` in every subprocess here, so ``can_prompt``
    is deterministically ``False`` — the scripted (flags-only) contract
    these verbs promise agents.
    """
    # init: interactive-only, no scripted mode.
    result = _run_cli(["config", "init"], sandbox=tmp_path, root_flags=["-q"])
    assert result.returncode == 2, (result.stdout, result.stderr)
    assert _structured_error(result.stderr)["error_code"] == "no_tty"

    # edit: interactive-only, even with $EDITOR set.
    result = _run_cli(
        ["config", "edit", "cli"],
        sandbox=tmp_path,
        root_flags=["-q"],
        env_extra={"EDITOR": "true"},
    )
    assert result.returncode == 2, (result.stdout, result.stderr)
    assert _structured_error(result.stderr)["error_code"] == "no_tty"

    # session add without --auth: the prompt fallback is unavailable, so
    # the structured error names the missing flag.
    result = _run_cli(
        ["config", "session", "add", "noauth"],
        sandbox=tmp_path,
        root_flags=["-q"],
    )
    assert result.returncode == 2, (result.stdout, result.stderr)
    error = _structured_error(result.stderr)
    assert error["error_code"] == "missing_required_option"
    assert "--auth" in error["error"]

    # No refusal path may leave a partial file behind.
    assert not (tmp_path / "cfg" / "community").exists()


# ---------------------------------------------------------------------------
# error paths: wrong paths, absent entities, JSON5 files, malformed trees
# ---------------------------------------------------------------------------


@pytest.mark.integration
@pytest.mark.timeout(300)
@_requires_dhcli
def test_authoring_error_paths(tmp_path: Path) -> None:
    """Every documented failure mode surfaces its stable error code."""
    cfg_dir = tmp_path / "cfg"

    # set on a session that does not exist: entity creation is add's job.
    result = _run_cli(
        ["config", "set", "community.sessions.ghost.port=1"],
        sandbox=tmp_path,
        root_flags=["-q"],
    )
    assert result.returncode == 2, (result.stdout, result.stderr)
    assert _structured_error(result.stderr)["error_code"] == "not_found"

    # get on a path outside the logical tree.
    result = _run_cli(
        ["config", "get", "bogus.path"], sandbox=tmp_path, root_flags=["-q"]
    )
    assert result.returncode == 2, (result.stdout, result.stderr)
    assert _structured_error(result.stderr)["error_code"] == "config_path_invalid"

    # set refuses to rewrite a JSON5 file (comments would be destroyed).
    # The earlier failing 'set' already created the private config dir
    # (mutating verbs now always create + lock it), so tolerate it here.
    cfg_dir.mkdir(mode=0o700, exist_ok=True)
    cli_json = cfg_dir / "cli.json"
    cli_json.write_text(
        '{\n  // hand-written comment\n  "output": {"format": "human"}\n}\n'
    )
    os.chmod(cli_json, 0o600)
    result = _run_cli(
        ["config", "set", "cli.output.format=json"],
        sandbox=tmp_path,
        root_flags=["-q"],
    )
    assert result.returncode == 2, (result.stdout, result.stderr)
    assert _structured_error(result.stderr)["error_code"] == "config_not_rewritable"
    # The refusal left the JSON5 file untouched.
    assert "hand-written comment" in cli_json.read_text()

    # A malformed session file: 'config files' still succeeds and flags
    # it, while 'config validate' fails fast with config_invalid.
    sessions_dir = cfg_dir / "community" / "sessions"
    sessions_dir.mkdir(parents=True, mode=0o700)
    os.chmod(cfg_dir / "community", 0o700)
    bad = sessions_dir / "bad.json"
    bad.write_text("{ this is not valid json")
    os.chmod(bad, 0o600)

    result = _run_cli(["config", "files"], sandbox=tmp_path)
    assert result.returncode == 0, result.stderr
    entries = {e["path"]: e for e in json.loads(result.stdout)["files"]}
    bad_entry = entries["community.sessions.bad"]
    assert bad_entry["exists"] is True
    assert bad_entry["valid"] is False
    assert bad_entry["error"]

    result = _run_cli(["config", "validate"], sandbox=tmp_path, root_flags=["-q"])
    assert result.returncode == 2, (result.stdout, result.stderr)
    assert _structured_error(result.stderr)["error_code"] == "config_invalid"


# ---------------------------------------------------------------------------
# DH_AI_DATA_DIR alone (no --config-dir/--runtime-dir flags) sandboxes fully
# ---------------------------------------------------------------------------


@pytest.mark.integration
@pytest.mark.timeout(300)
@_requires_dhcli
def test_env_var_isolation_without_flags(tmp_path: Path) -> None:
    """With no path flags, ``DH_AI_DATA_DIR`` alone confines every write.

    This is the guarantee that lets developers and CI point the whole
    tool at an alternative data root without risking a locally
    configured ``~/.deephaven/ai``.
    """
    # Deliberately not created: a fresh machine has no data root, and
    # the store must create the missing ancestors on first write.
    data_root = tmp_path / "data-root"

    result = _run_cli(
        ["config", "session", "add", "envonly", "--auth", "anonymous"],
        sandbox=tmp_path,
        explicit_dirs=False,
    )
    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout)
    session_file = Path(payload["file"])
    assert (
        session_file == data_root / "config" / "community" / "sessions" / "envonly.json"
    )
    assert session_file.is_file()
    assert _mode(session_file) == 0o600

    # The full runtime load (validate) also resolves under the data root.
    result = _run_cli(["config", "validate"], sandbox=tmp_path, explicit_dirs=False)
    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout)
    assert payload["config_dir"] == str(data_root / "config")
    assert (data_root / "runtime").is_dir()

    # Everything the invocation created lives inside the sandbox.
    _assert_inside(session_file, tmp_path)
