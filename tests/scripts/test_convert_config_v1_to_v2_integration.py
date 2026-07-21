"""Integration tests for the v1->v2 config converter.

These prove the converter end-to-end against the *real* v2 loader: the
standalone script (run as a subprocess, so its stdlib-only / no-project-import
property is exercised) produces a config tree that ``dhcli config validate``
accepts. A bad field mapping surfaces as ``config_invalid`` (exit 2) and fails
the test.

Marked ``@pytest.mark.integration`` (skipped by the default ``uv run pytest``).
Invoke with::

    uv run pytest -s -m integration -k convert_config

Prerequisites: ``dhcli`` on ``$PATH`` (provided by ``uv sync``).
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

_SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "convert_config_v1_to_v2.py"
_FIXTURE = Path(__file__).resolve().parent / "fixtures" / "v1_config.json"

pytestmark = pytest.mark.integration

_NO_DHCLI = pytest.mark.skipif(
    shutil.which("dhcli") is None, reason="dhcli entry point not on PATH"
)


def _convert(v1_path: Path, out: Path) -> None:
    """Run the standalone converter as a subprocess (no project on sys.path)."""
    result = subprocess.run(  # noqa: S603 - argv fully constructed locally
        [sys.executable, str(_SCRIPT), str(v1_path), "--output", str(out), "--yes"],
        capture_output=True,
        text=True,
        check=False,
        cwd=os.path.dirname(_SCRIPT.parent),
        env={k: v for k, v in os.environ.items() if k != "PYTHONPATH"},
        timeout=60,
    )
    assert result.returncode == 0, result.stderr


def _validate(config_dir: Path, runtime_dir: Path, env: dict[str, str]) -> None:
    """Run ``dhcli config validate`` and assert it accepts the tree."""
    result = subprocess.run(  # noqa: S603 - argv fully constructed locally
        [
            "dhcli",
            "--config-dir",
            str(config_dir),
            "--runtime-dir",
            str(runtime_dir),
            "config",
            "validate",
        ],
        capture_output=True,
        text=True,
        check=False,
        env={**os.environ, **env},
        timeout=60,
    )
    assert result.returncode == 0, f"stdout={result.stdout}\nstderr={result.stderr}"


@_NO_DHCLI
def test_converted_fixture_validates(tmp_path: Path) -> None:
    """The checked-in v1 fixture converts to a tree the v2 loader accepts.

    The fixture is a near-maximal v1 config: every file-independent conversion
    shape is produced here so this single validation proves the real v2 loader
    accepts all of them. Both secret forms that do not need a file are covered:
    the literal (regular) form and the ``${env:}`` form, for PSK
    (local_dev/psk_env), Basic->custom (basic/basic_env), and password
    (analytics/prod). The remaining shapes (anonymous, never_timeout, Groovy,
    docker + python creation defaults, full enterprise defaults) are covered
    once each. The ``${file:}`` form is covered by
    ``test_converted_tls_and_private_key_validate``.
    """
    out = tmp_path / "config"
    _convert(_FIXTURE, out)
    for name in ("local_dev", "anon", "psk_env", "basic", "basic_env"):
        assert (out / "community" / "sessions" / f"{name}.json").is_file()
    assert (out / "community" / "settings.json").is_file()
    assert (out / "enterprise" / "systems" / "prod.json").is_file()
    assert (out / "enterprise" / "systems" / "analytics.json").is_file()
    _validate(
        out,
        tmp_path / "runtime",
        {
            "DH_COMMUNITY_PSK": "community-psk",
            "DH_BASIC_TOKEN": "basic-token",
            "DH_DYNAMIC_PSK": "dynamic-psk",
            "DH_PROD_PASSWORD": "secret",
        },
    )


@_NO_DHCLI
def test_converted_tls_and_private_key_validate(tmp_path: Path) -> None:
    """TLS certs and an enterprise private key (absolute ${file:} paths) validate.

    Exercises every file-dependent (``${file:}``) conversion shape against the
    real v2 loader: the community ``tls`` block's ``root_certs`` and
    ``client_certificate`` (cert_chain + private_key), and the enterprise
    ``private_key`` credential's ``key_text``. Also exercises the relaxed
    ``${file:}`` containment: every referenced PEM lives outside the config
    directory and is read by absolute path.
    """
    ca = tmp_path / "ca.pem"
    ca.write_text("-----BEGIN CERTIFICATE-----\nCA\n-----END CERTIFICATE-----\n")
    client_cert = tmp_path / "client.pem"
    client_cert.write_text(
        "-----BEGIN CERTIFICATE-----\nCLIENT\n-----END CERTIFICATE-----\n"
    )
    client_key = tmp_path / "client-key.pem"
    client_key.write_text(
        "-----BEGIN PRIVATE KEY-----\nCLIENTKEY\n-----END PRIVATE KEY-----\n"
    )
    key = tmp_path / "key.pem"
    key.write_text("-----BEGIN PRIVATE KEY-----\nKEY\n-----END PRIVATE KEY-----\n")

    v1 = {
        "community": {
            "sessions": {
                "secure": {
                    "host": "secure.example.com",
                    "port": 443,
                    "use_tls": True,
                    "tls_root_certs": str(ca),
                    "client_cert_chain": str(client_cert),
                    "client_private_key": str(client_key),
                    "auth_type": "Anonymous",
                }
            }
        },
        "enterprise": {
            "systems": {
                "stg": {
                    "connection_json_url": "https://stg.example.com:8123/iris/connection.json",
                    "auth_type": "private_key",
                    "private_key_path": str(key),
                }
            }
        },
    }
    v1_path = tmp_path / "v1.json"
    v1_path.write_text(json.dumps(v1), encoding="utf-8")

    out = tmp_path / "config"
    _convert(v1_path, out)
    _validate(out, tmp_path / "runtime", {})
