import logging

import pytest

from deephaven_mcp.io import load_bytes


@pytest.mark.asyncio
async def test_load_bytes_reads_file(tmp_path):
    file_path = tmp_path / "cert.pem"
    content = b"test-bytes"
    file_path.write_bytes(content)
    result = await load_bytes(str(file_path))
    assert result == content


@pytest.mark.asyncio
async def test_load_bytes_none():
    result = await load_bytes(None)
    assert result is None


@pytest.mark.asyncio
async def test_load_bytes_error(tmp_path, caplog):
    file_path = tmp_path / "does_not_exist.pem"
    caplog.set_level(logging.ERROR)
    with pytest.raises(Exception):
        await load_bytes(str(file_path))
    assert any("Error loading binary file" in r for r in caplog.text.splitlines())
