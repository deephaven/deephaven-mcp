import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pyarrow
import pytest

from deephaven_mcp.sessions._session._queries import (
    get_dh_versions,
    get_meta_table,
    get_pip_packages_table,
    get_table,
)


@pytest.mark.asyncio
async def test_get_table_success():
    table_mock = MagicMock()
    arrow_mock = MagicMock(spec=pyarrow.Table)
    table_mock.to_arrow = MagicMock(return_value=arrow_mock)
    session_mock = MagicMock()
    session_mock.open_table = MagicMock(return_value=table_mock)
    result = await get_table(session_mock, "foo")
    assert result is arrow_mock
    session_mock.open_table.assert_called_once_with("foo")
    table_mock.to_arrow.assert_called_once()


@pytest.mark.asyncio
async def test_get_table_open_table_error():
    session_mock = MagicMock()
    session_mock.open_table = MagicMock(side_effect=RuntimeError("fail open"))
    with pytest.raises(RuntimeError, match="fail open"):
        await get_table(session_mock, "foo")


@pytest.mark.asyncio
async def test_get_table_to_arrow_error():
    table_mock = MagicMock()
    table_mock.to_arrow = MagicMock(side_effect=RuntimeError("fail arrow"))
    session_mock = MagicMock()
    session_mock.open_table = MagicMock(return_value=table_mock)
    with pytest.raises(RuntimeError, match="fail arrow"):
        await get_table(session_mock, "foo")


@pytest.mark.asyncio
async def test_get_meta_table_success():
    session_mock = MagicMock()
    table_mock = MagicMock()
    meta_table_mock = MagicMock()
    arrow_mock = object()
    session_mock.open_table.return_value = table_mock
    type(table_mock).meta_table = property(lambda self: meta_table_mock)

    def to_arrow():
        return arrow_mock

    meta_table_mock.to_arrow = to_arrow

    async def fake_to_thread(fn, *args, **kwargs):
        return fn(*args, **kwargs)

    with patch("deephaven_mcp.sessions._session._queries.asyncio.to_thread", new=fake_to_thread):
        result = await get_meta_table(session_mock, "foo")
        assert result is arrow_mock
        session_mock.open_table.assert_called_once_with("foo")


@pytest.mark.asyncio
async def test_get_meta_table_open_table_error():
    session_mock = MagicMock()
    session_mock.open_table.side_effect = RuntimeError("fail-open")
    with pytest.raises(RuntimeError) as excinfo:
        await get_meta_table(session_mock, "foo")
    assert "fail-open" in str(excinfo.value)
    session_mock.open_table.assert_called_once_with("foo")


@pytest.mark.asyncio
async def test_get_meta_table_to_arrow_error():
    session_mock = MagicMock()
    table_mock = MagicMock()
    meta_table_mock = MagicMock()
    session_mock.open_table.return_value = table_mock
    type(table_mock).meta_table = property(lambda self: meta_table_mock)

    def to_arrow():
        raise RuntimeError("fail-arrow")

    meta_table_mock.to_arrow = to_arrow

    async def fake_to_thread(fn, *args, **kwargs):
        return fn(*args, **kwargs)

    with patch("deephaven_mcp.sessions._session._queries.asyncio.to_thread", new=fake_to_thread):
        with pytest.raises(RuntimeError) as excinfo:
            await get_meta_table(session_mock, "foo")
        assert "fail-arrow" in str(excinfo.value)
        session_mock.open_table.assert_called_once_with("foo")


@pytest.mark.asyncio
async def test_get_pip_packages_table_success(caplog):
    session_mock = MagicMock()

    async def fake_to_thread(fn, *args, **kwargs):
        return fn(*args, **kwargs)

    with patch("deephaven_mcp.sessions._session._queries.asyncio.to_thread", new=fake_to_thread):
        arrow_mock = MagicMock()
        with patch(
            "deephaven_mcp.sessions._session._queries.get_table",
            AsyncMock(return_value=arrow_mock),
        ) as mock_get_table:
            with caplog.at_level("INFO"):
                result = await get_pip_packages_table(session_mock)
            assert result is arrow_mock
            assert "Running pip packages script in session..." in caplog.text
            assert "Script executed successfully." in caplog.text
            assert "Table retrieved successfully." in caplog.text
            session_mock.run_script.assert_called_once()
            mock_get_table.assert_awaited_once_with(session_mock, "_pip_packages_table")


@pytest.mark.asyncio
async def test_get_pip_packages_table_script_failure():
    session_mock = MagicMock()

    async def fake_to_thread(fn, *args, **kwargs):
        if fn == session_mock.run_script:
            raise RuntimeError("fail-script")
        return fn(*args, **kwargs)

    with patch("deephaven_mcp.sessions._session._queries.asyncio.to_thread", new=fake_to_thread):
        with pytest.raises(RuntimeError, match="fail-script"):
            await get_pip_packages_table(session_mock)


@pytest.mark.asyncio
async def test_get_pip_packages_table_table_failure():
    session_mock = MagicMock()

    async def fake_to_thread(fn, *args, **kwargs):
        return fn(*args, **kwargs)

    with (
        patch("deephaven_mcp.sessions._session._queries.asyncio.to_thread", new=fake_to_thread),
        patch(
            "deephaven_mcp.sessions._session._queries.get_table",
            AsyncMock(side_effect=RuntimeError("fail-table")),
        ),
    ):
        with pytest.raises(RuntimeError, match="fail-table"):
            await get_pip_packages_table(session_mock)
        session_mock.run_script.assert_called_once()


@pytest.mark.asyncio
async def test_get_dh_versions_both_versions():
    session = MagicMock()
    df = MagicMock()
    df.to_dict.return_value = [
        {"Package": "deephaven-core", "Version": "1.2.3"},
        {"Package": "deephaven_coreplus_worker", "Version": "4.5.6"},
    ]
    arrow_table = MagicMock()
    arrow_table.to_pandas.return_value = df
    with patch(
        "deephaven_mcp.sessions._session._queries.get_pip_packages_table",
        new=AsyncMock(return_value=arrow_table),
    ):
        core, coreplus = await get_dh_versions(session)
        assert core == "1.2.3"
        assert coreplus == "4.5.6"


@pytest.mark.asyncio
async def test_get_dh_versions_only_core():
    session = MagicMock()
    df = MagicMock()
    df.to_dict.return_value = [
        {"Package": "deephaven-core", "Version": "1.2.3"},
        {"Package": "numpy", "Version": "2.0.0"},
    ]
    arrow_table = MagicMock()
    arrow_table.to_pandas.return_value = df
    with patch(
        "deephaven_mcp.sessions._session._queries.get_pip_packages_table",
        new=AsyncMock(return_value=arrow_table),
    ):
        core, coreplus = await get_dh_versions(session)
        assert core == "1.2.3"
        assert coreplus is None


@pytest.mark.asyncio
async def test_get_dh_versions_only_coreplus():
    session = MagicMock()
    df = MagicMock()
    df.to_dict.return_value = [
        {"Package": "deephaven_coreplus_worker", "Version": "4.5.6"},
        {"Package": "pandas", "Version": "2.0.0"},
    ]
    arrow_table = MagicMock()
    arrow_table.to_pandas.return_value = df
    with patch(
        "deephaven_mcp.sessions._session._queries.get_pip_packages_table",
        new=AsyncMock(return_value=arrow_table),
    ):
        core, coreplus = await get_dh_versions(session)
        assert core is None
        assert coreplus == "4.5.6"


@pytest.mark.asyncio
async def test_get_dh_versions_neither():
    session = MagicMock()
    df = MagicMock()
    df.to_dict.return_value = [
        {"Package": "numpy", "Version": "2.0.0"},
        {"Package": "pandas", "Version": "2.0.0"},
    ]
    arrow_table = MagicMock()
    arrow_table.to_pandas.return_value = df
    with patch(
        "deephaven_mcp.sessions._session._queries.get_pip_packages_table",
        new=AsyncMock(return_value=arrow_table),
    ):
        core, coreplus = await get_dh_versions(session)
        assert core is None
        assert coreplus is None


@pytest.mark.asyncio
async def test_get_dh_versions_malformed():
    session = MagicMock()
    df = MagicMock()
    df.to_dict.return_value = [{"NotPackage": "foo", "NotVersion": "bar"}]
    arrow_table = MagicMock()
    arrow_table.to_pandas.return_value = df
    with patch(
        "deephaven_mcp.sessions._session._queries.get_pip_packages_table",
        AsyncMock(return_value=arrow_table),
    ):
        core, coreplus = await get_dh_versions(session)
        assert core is None
        assert coreplus is None


@pytest.mark.asyncio
async def test_get_dh_versions_arrow_table_none():
    session = MagicMock()
    with patch(
        "deephaven_mcp.sessions._session._queries.get_pip_packages_table",
        new=AsyncMock(return_value=None),
    ):
        core, coreplus = await get_dh_versions(session)
        assert core is None
        assert coreplus is None
