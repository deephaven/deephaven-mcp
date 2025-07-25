import asyncio
import logging
from unittest.mock import AsyncMock, MagicMock, patch

import pyarrow
import pytest

from deephaven_mcp._exceptions import UnsupportedOperationError
from deephaven_mcp.queries import (
    get_dh_versions,
    get_meta_table,
    get_pip_packages_table,
    get_programming_language_version,
    get_programming_language_version_table,
    get_table,
)


@pytest.mark.asyncio
async def test_get_table_success():
    table_mock = MagicMock()
    arrow_mock = MagicMock(spec=pyarrow.Table)
    table_mock.to_arrow = lambda: arrow_mock
    session_mock = MagicMock()
    session_mock.open_table = AsyncMock(return_value=table_mock)
    result = await get_table(session_mock, "foo")
    assert result is arrow_mock
    session_mock.open_table.assert_awaited_once_with("foo")


@pytest.mark.asyncio
async def test_get_table_open_table_error():
    session_mock = MagicMock()
    session_mock.open_table = AsyncMock(side_effect=RuntimeError("fail open"))
    with pytest.raises(RuntimeError, match="fail open"):
        await get_table(session_mock, "foo")


@pytest.mark.asyncio
async def test_get_table_to_arrow_error():
    table_mock = MagicMock()
    table_mock.to_arrow = MagicMock(side_effect=RuntimeError("fail arrow"))
    session_mock = MagicMock()
    session_mock.open_table = AsyncMock(return_value=table_mock)
    with pytest.raises(RuntimeError, match="fail arrow"):
        await get_table(session_mock, "foo")


@pytest.mark.asyncio
async def test_get_meta_table_success():
    session_mock = MagicMock()
    table_mock = MagicMock()
    meta_table_mock = MagicMock()
    arrow_mock = object()
    session_mock.open_table = AsyncMock(return_value=table_mock)
    type(table_mock).meta_table = property(lambda self: meta_table_mock)

    def to_arrow():
        return arrow_mock

    meta_table_mock.to_arrow = to_arrow

    async def fake_to_thread(fn, *args, **kwargs):
        return fn(*args, **kwargs)

    with patch("deephaven_mcp.queries.asyncio.to_thread", new=fake_to_thread):
        result = await get_meta_table(session_mock, "foo")
        assert result is arrow_mock
        session_mock.open_table.assert_awaited_once_with("foo")


@pytest.mark.asyncio
async def test_get_meta_table_open_table_error():
    session_mock = MagicMock()
    session_mock.open_table = AsyncMock(side_effect=RuntimeError("fail-open"))
    with pytest.raises(RuntimeError) as excinfo:
        await get_meta_table(session_mock, "foo")
    assert "fail-open" in str(excinfo.value)
    session_mock.open_table.assert_awaited_once_with("foo")


@pytest.mark.asyncio
async def test_get_meta_table_to_arrow_error():
    session_mock = MagicMock()
    table_mock = MagicMock()
    meta_table_mock = MagicMock()
    session_mock.open_table = AsyncMock(return_value=table_mock)
    type(table_mock).meta_table = property(lambda self: meta_table_mock)

    def to_arrow():
        raise RuntimeError("fail-arrow")

    meta_table_mock.to_arrow = to_arrow

    async def fake_to_thread(fn, *args, **kwargs):
        return fn(*args, **kwargs)

    with patch("deephaven_mcp.queries.asyncio.to_thread", new=fake_to_thread):
        with pytest.raises(RuntimeError) as excinfo:
            await get_meta_table(session_mock, "foo")
        assert "fail-arrow" in str(excinfo.value)
        session_mock.open_table.assert_awaited_once_with("foo")


@pytest.mark.asyncio
async def test_get_pip_packages_table_success(caplog):
    session_mock = MagicMock()
    session_mock.programming_language = "python"
    session_mock.run_script = AsyncMock()

    async def fake_to_thread(fn, *args, **kwargs):
        return fn(*args, **kwargs)

    with patch("deephaven_mcp.queries.asyncio.to_thread", new=fake_to_thread):
        arrow_mock = MagicMock()
        with patch(
            "deephaven_mcp.queries.get_table",
            AsyncMock(return_value=arrow_mock),
        ) as mock_get_table:
            with caplog.at_level("DEBUG"):
                result = await get_pip_packages_table(session_mock)
            assert result is arrow_mock
            assert "Running pip packages script in session..." in caplog.text
            assert "Script executed successfully." in caplog.text
            assert "Table '_pip_packages_table' retrieved successfully." in caplog.text
            session_mock.run_script.assert_awaited_once()
            mock_get_table.assert_awaited_once_with(session_mock, "_pip_packages_table")


@pytest.mark.asyncio
async def test_get_pip_packages_table_script_failure():
    session_mock = MagicMock()
    session_mock.programming_language = "python"
    session_mock.run_script = AsyncMock(side_effect=RuntimeError("fail-script"))

    async def fake_to_thread(fn, *args, **kwargs):
        if fn == session_mock.run_script:
            raise RuntimeError("fail-script")
        return fn(*args, **kwargs)

    with patch("deephaven_mcp.queries.asyncio.to_thread", new=fake_to_thread):
        with pytest.raises(RuntimeError, match="fail-script"):
            await get_pip_packages_table(session_mock)


@pytest.mark.asyncio
async def test_get_pip_packages_table_table_failure():
    session_mock = MagicMock()
    session_mock.programming_language = "python"
    session_mock.run_script = AsyncMock()

    async def fake_to_thread(fn, *args, **kwargs):
        return fn(*args, **kwargs)

    with (
        patch(
            "deephaven_mcp.queries.asyncio.to_thread",
            new=fake_to_thread,
        ),
        patch(
            "deephaven_mcp.queries.get_table",
            AsyncMock(side_effect=RuntimeError("fail-table")),
        ),
    ):
        with pytest.raises(RuntimeError, match="fail-table"):
            await get_pip_packages_table(session_mock)
        session_mock.run_script.assert_awaited_once()


@pytest.mark.asyncio
async def test_get_pip_packages_table_unsupported_language():
    session_mock = MagicMock()
    session_mock.programming_language = "groovy"

    with pytest.raises(
        UnsupportedOperationError, match="only supports Python sessions"
    ):
        await get_pip_packages_table(session_mock)


@pytest.mark.asyncio
async def test_get_dh_versions_both_versions():
    session_mock = MagicMock()
    session_mock.programming_language = "python"
    arrow_table = MagicMock()
    arrow_table.to_pydict.return_value = {
        "Package": ["deephaven-core", "deephaven_coreplus_worker"],
        "Version": ["1.2.3", "4.5.6"],
    }
    with patch(
        "deephaven_mcp.queries.get_pip_packages_table",
        new=AsyncMock(return_value=arrow_table),
    ):
        core, coreplus = await get_dh_versions(session_mock)
        assert core == "1.2.3"
        assert coreplus == "4.5.6"


@pytest.mark.asyncio
async def test_get_dh_versions_only_core():
    session_mock = MagicMock()
    session_mock.programming_language = "python"
    arrow_table = MagicMock()
    arrow_table.to_pydict.return_value = {
        "Package": ["deephaven-core", "numpy"],
        "Version": ["1.2.3", "2.0.0"],
    }
    with patch(
        "deephaven_mcp.queries.get_pip_packages_table",
        new=AsyncMock(return_value=arrow_table),
    ):
        core, coreplus = await get_dh_versions(session_mock)
        assert core == "1.2.3"
        assert coreplus is None


@pytest.mark.asyncio
async def test_get_dh_versions_only_coreplus():
    session_mock = MagicMock()
    session_mock.programming_language = "python"
    arrow_table = MagicMock()
    arrow_table.to_pydict.return_value = {
        "Package": ["deephaven_coreplus_worker", "pandas"],
        "Version": ["4.5.6", "2.0.0"],
    }
    with patch(
        "deephaven_mcp.queries.get_pip_packages_table",
        new=AsyncMock(return_value=arrow_table),
    ):
        core, coreplus = await get_dh_versions(session_mock)
        assert core is None
        assert coreplus == "4.5.6"


@pytest.mark.asyncio
async def test_get_dh_versions_neither():
    session_mock = MagicMock()
    session_mock.programming_language = "python"
    arrow_table = MagicMock()
    arrow_table.to_pydict.return_value = {
        "Package": ["numpy", "pandas"],
        "Version": ["2.0.0", "2.0.0"],
    }
    with patch(
        "deephaven_mcp.queries.get_pip_packages_table",
        new=AsyncMock(return_value=arrow_table),
    ):
        core, coreplus = await get_dh_versions(session_mock)
        assert core is None
        assert coreplus is None


@pytest.mark.asyncio
async def test_get_dh_versions_malformed():
    session_mock = MagicMock()
    session_mock.programming_language = "python"
    df = MagicMock()
    df.to_dict.return_value = [{"NotPackage": "foo", "NotVersion": "bar"}]
    arrow_table = MagicMock()
    arrow_table.to_pandas.return_value = df
    with patch(
        "deephaven_mcp.queries.get_pip_packages_table",
        AsyncMock(return_value=arrow_table),
    ):
        core, coreplus = await get_dh_versions(session_mock)
        assert core is None
        assert coreplus is None


@pytest.mark.asyncio
async def test_get_dh_versions_unsupported_language():
    session_mock = MagicMock()
    session_mock.programming_language = "groovy"

    with pytest.raises(
        UnsupportedOperationError, match="only supports Python sessions"
    ):
        await get_dh_versions(session_mock)


@pytest.mark.asyncio
async def test_get_dh_versions_arrow_table_none():
    session = MagicMock()
    session.programming_language = "python"
    with patch(
        "deephaven_mcp.queries.get_pip_packages_table",
        new=AsyncMock(return_value=None),
    ):
        core, coreplus = await get_dh_versions(session)
        assert core is None
        assert coreplus is None


@pytest.mark.asyncio
async def test_get_programming_language_version_table_success(caplog):
    session_mock = MagicMock()
    session_mock.programming_language = "python"
    session_mock.run_script = AsyncMock()

    async def fake_to_thread(fn, *args, **kwargs):
        return fn(*args, **kwargs)

    with patch("deephaven_mcp.queries.asyncio.to_thread", new=fake_to_thread):
        arrow_mock = MagicMock()
        with patch(
            "deephaven_mcp.queries.get_table",
            AsyncMock(return_value=arrow_mock),
        ) as mock_get_table:
            with caplog.at_level("DEBUG"):
                result = await get_programming_language_version_table(session_mock)
            assert result is arrow_mock
            assert "Running Python version script in session..." in caplog.text
            assert "Script executed successfully." in caplog.text
            assert (
                "Table '_python_version_table' retrieved successfully." in caplog.text
            )
            session_mock.run_script.assert_awaited_once()
            mock_get_table.assert_awaited_once_with(
                session_mock, "_python_version_table"
            )


@pytest.mark.asyncio
async def test_get_programming_language_version_table_script_failure():
    session_mock = MagicMock()
    session_mock.programming_language = "python"
    session_mock.run_script = AsyncMock(side_effect=RuntimeError("fail-script"))

    async def fake_to_thread(fn, *args, **kwargs):
        if fn == session_mock.run_script:
            raise RuntimeError("fail-script")
        return fn(*args, **kwargs)

    with patch("deephaven_mcp.queries.asyncio.to_thread", new=fake_to_thread):
        with pytest.raises(RuntimeError, match="fail-script"):
            await get_programming_language_version_table(session_mock)


@pytest.mark.asyncio
async def test_get_programming_language_version_table_table_failure():
    session_mock = MagicMock()
    session_mock.programming_language = "python"
    session_mock.run_script = AsyncMock()

    async def fake_to_thread(fn, *args, **kwargs):
        return fn(*args, **kwargs)

    with (
        patch(
            "deephaven_mcp.queries.asyncio.to_thread",
            new=fake_to_thread,
        ),
        patch(
            "deephaven_mcp.queries.get_table",
            AsyncMock(side_effect=RuntimeError("fail-table")),
        ),
    ):
        with pytest.raises(RuntimeError, match="fail-table"):
            await get_programming_language_version_table(session_mock)
        session_mock.run_script.assert_awaited_once()


@pytest.mark.asyncio
async def test_get_programming_language_version_table_unsupported_language():
    session_mock = MagicMock()
    session_mock.programming_language = "groovy"

    with pytest.raises(
        UnsupportedOperationError, match="only supports Python sessions"
    ):
        await get_programming_language_version_table(session_mock)


@pytest.mark.asyncio
async def test_get_programming_language_version_success(caplog):
    """Test successful extraction of version string from pyarrow table."""
    caplog.set_level(logging.DEBUG)

    # Create a mock pyarrow table with Version column
    version_column_mock = MagicMock()
    version_value_mock = MagicMock()
    version_value_mock.as_py.return_value = "3.9.7"
    version_column_mock.__getitem__.return_value = version_value_mock

    version_table_mock = MagicMock(spec=pyarrow.Table)
    version_table_mock.column.return_value = version_column_mock

    session_mock = MagicMock()

    # Mock get_programming_language_version_table to return our mock table
    with patch(
        "deephaven_mcp.queries.get_programming_language_version_table",
        return_value=version_table_mock,
    ) as mock_get_table:
        result = await get_programming_language_version(session_mock)

    # Verify the result
    assert result == "3.9.7"

    # Verify the function calls (lines 200-204)
    mock_get_table.assert_awaited_once_with(session_mock)
    version_table_mock.column.assert_called_once_with("Version")  # line 200
    version_column_mock.__getitem__.assert_called_once_with(0)  # line 201
    version_value_mock.as_py.assert_called_once()  # line 201

    # Verify logging
    assert (
        "[queries:get_programming_language_version] Retrieving programming language version..."
        in caplog.text
    )
    assert (
        "[queries:get_programming_language_version] Retrieved version: 3.9.7"
        in caplog.text
    )


@pytest.mark.asyncio
async def test_get_programming_language_version_column_access_error():
    """Test error handling when accessing Version column fails."""
    version_table_mock = MagicMock(spec=pyarrow.Table)
    version_table_mock.column.side_effect = KeyError("Version column not found")

    session_mock = MagicMock()

    with patch(
        "deephaven_mcp.queries.get_programming_language_version_table",
        return_value=version_table_mock,
    ):
        with pytest.raises(KeyError, match="Version column not found"):
            await get_programming_language_version(session_mock)


@pytest.mark.asyncio
async def test_get_programming_language_version_index_error():
    """Test error handling when accessing first row fails (empty table)."""
    version_column_mock = MagicMock()
    version_column_mock.__getitem__.side_effect = IndexError("list index out of range")

    version_table_mock = MagicMock(spec=pyarrow.Table)
    version_table_mock.column.return_value = version_column_mock

    session_mock = MagicMock()

    with patch(
        "deephaven_mcp.queries.get_programming_language_version_table",
        return_value=version_table_mock,
    ):
        with pytest.raises(IndexError, match="list index out of range"):
            await get_programming_language_version(session_mock)


@pytest.mark.asyncio
async def test_get_programming_language_version_as_py_error():
    """Test error handling when converting pyarrow value to Python fails."""
    version_value_mock = MagicMock()
    version_value_mock.as_py.side_effect = RuntimeError("Conversion failed")

    version_column_mock = MagicMock()
    version_column_mock.__getitem__.return_value = version_value_mock

    version_table_mock = MagicMock(spec=pyarrow.Table)
    version_table_mock.column.return_value = version_column_mock

    session_mock = MagicMock()

    with patch(
        "deephaven_mcp.queries.get_programming_language_version_table",
        return_value=version_table_mock,
    ):
        with pytest.raises(RuntimeError, match="Conversion failed"):
            await get_programming_language_version(session_mock)


@pytest.mark.asyncio
async def test_get_programming_language_version_table_failure():
    """Test error handling when get_programming_language_version_table fails."""
    session_mock = MagicMock()

    with patch(
        "deephaven_mcp.queries.get_programming_language_version_table",
        side_effect=RuntimeError("Table retrieval failed"),
    ):
        with pytest.raises(RuntimeError, match="Table retrieval failed"):
            await get_programming_language_version(session_mock)
