import asyncio
import logging
from unittest.mock import AsyncMock, MagicMock, patch

import pyarrow
import pytest

from deephaven_mcp._exceptions import UnsupportedOperationError
from deephaven_mcp.queries import (
    _apply_filters,
    _apply_row_limit,
    _extract_meta_table,
    _load_catalog_table,
    _validate_python_session,
    get_catalog_meta_table,
    get_catalog_table,
    get_catalog_table_data,
    get_dh_versions,
    get_pip_packages_table,
    get_programming_language_version,
    get_programming_language_version_table,
    get_session_meta_table,
    get_table,
)

# ===== Helper function tests =====


def test_validate_python_session_success():
    """Test _validate_python_session with a Python session."""
    session_mock = MagicMock()
    session_mock.programming_language = "Python"

    # Should not raise
    _validate_python_session("test_function", session_mock)


def test_validate_python_session_case_insensitive():
    """Test _validate_python_session is case-insensitive."""
    session_mock = MagicMock()
    session_mock.programming_language = "PYTHON"

    # Should not raise
    _validate_python_session("test_function", session_mock)


def test_validate_python_session_failure():
    """Test _validate_python_session with non-Python session."""
    session_mock = MagicMock()
    session_mock.programming_language = "Groovy"

    with pytest.raises(
        UnsupportedOperationError,
        match="test_function only supports Python sessions.*Groovy",
    ):
        _validate_python_session("test_function", session_mock)


@pytest.mark.asyncio
async def test_apply_filters_with_filters():
    """Test _apply_filters with filters provided."""
    table_mock = MagicMock()
    filtered_table_mock = MagicMock()
    table_mock.where = MagicMock(return_value=filtered_table_mock)

    async def fake_to_thread(fn, *args, **kwargs):
        return fn(*args, **kwargs)

    with patch("deephaven_mcp.queries.asyncio.to_thread", new=fake_to_thread):
        result_table = await _apply_filters(
            table_mock,
            filters=["Column1 > 10", "Column2 = `value`"],
            context_name="test table",
        )
        assert result_table is filtered_table_mock
        table_mock.where.assert_called_once_with(["Column1 > 10", "Column2 = `value`"])


@pytest.mark.asyncio
async def test_apply_filters_no_filters():
    """Test _apply_filters with no filters (None)."""
    table_mock = MagicMock()

    result_table = await _apply_filters(
        table_mock, filters=None, context_name="test table"
    )
    assert result_table is table_mock
    # where() should not be called
    assert not hasattr(table_mock, "where") or not table_mock.where.called


@pytest.mark.asyncio
async def test_apply_filters_empty_list():
    """Test _apply_filters with empty filter list."""
    table_mock = MagicMock()

    result_table = await _apply_filters(
        table_mock, filters=[], context_name="test table"
    )
    assert result_table is table_mock
    # where() should not be called for empty list
    assert not hasattr(table_mock, "where") or not table_mock.where.called


@pytest.mark.asyncio
async def test_apply_row_limit_with_head_complete():
    """Test _apply_row_limit with head=True when table is smaller than max_rows."""
    table_mock = MagicMock()
    table_mock.size = 500
    limited_table_mock = MagicMock()
    table_mock.head = lambda n: limited_table_mock

    async def fake_to_thread(fn, *args, **kwargs):
        return fn(*args, **kwargs)

    with patch("deephaven_mcp.queries.asyncio.to_thread", new=fake_to_thread):
        result_table, is_complete = await _apply_row_limit(
            table_mock, max_rows=1000, head=True, context_name="test table"
        )
        assert result_table is limited_table_mock
        assert is_complete is True  # 500 <= 1000


@pytest.mark.asyncio
async def test_apply_row_limit_with_head_incomplete():
    """Test _apply_row_limit with head=True when table is larger than max_rows."""
    table_mock = MagicMock()
    table_mock.size = 2000
    limited_table_mock = MagicMock()
    table_mock.head = lambda n: limited_table_mock

    async def fake_to_thread(fn, *args, **kwargs):
        return fn(*args, **kwargs)

    with patch("deephaven_mcp.queries.asyncio.to_thread", new=fake_to_thread):
        result_table, is_complete = await _apply_row_limit(
            table_mock, max_rows=1000, head=True, context_name="test table"
        )
        assert result_table is limited_table_mock
        assert is_complete is False  # 2000 > 1000


@pytest.mark.asyncio
async def test_apply_row_limit_with_tail_complete():
    """Test _apply_row_limit with head=False (tail) when table is smaller than max_rows."""
    table_mock = MagicMock()
    table_mock.size = 500
    limited_table_mock = MagicMock()
    table_mock.tail = lambda n: limited_table_mock

    async def fake_to_thread(fn, *args, **kwargs):
        return fn(*args, **kwargs)

    with patch("deephaven_mcp.queries.asyncio.to_thread", new=fake_to_thread):
        result_table, is_complete = await _apply_row_limit(
            table_mock, max_rows=1000, head=False, context_name="test table"
        )
        assert result_table is limited_table_mock
        assert is_complete is True  # 500 <= 1000


@pytest.mark.asyncio
async def test_apply_row_limit_with_tail_incomplete():
    """Test _apply_row_limit with head=False (tail) when table is larger than max_rows."""
    table_mock = MagicMock()
    table_mock.size = 2000
    limited_table_mock = MagicMock()
    table_mock.tail = lambda n: limited_table_mock

    async def fake_to_thread(fn, *args, **kwargs):
        return fn(*args, **kwargs)

    with patch("deephaven_mcp.queries.asyncio.to_thread", new=fake_to_thread):
        result_table, is_complete = await _apply_row_limit(
            table_mock, max_rows=1000, head=False, context_name="test table"
        )
        assert result_table is limited_table_mock
        assert is_complete is False  # 2000 > 1000


@pytest.mark.asyncio
async def test_apply_row_limit_no_limit():
    """Test _apply_row_limit with max_rows=None (full table)."""
    table_mock = MagicMock()

    result_table, is_complete = await _apply_row_limit(
        table_mock, max_rows=None, head=True, context_name="test table"
    )
    assert result_table is table_mock
    assert is_complete is True


@pytest.mark.asyncio
async def test_apply_row_limit_exact_size_match():
    """Test _apply_row_limit when table size exactly matches max_rows."""
    table_mock = MagicMock()
    table_mock.size = 1000
    limited_table_mock = MagicMock()
    table_mock.head = lambda n: limited_table_mock

    async def fake_to_thread(fn, *args, **kwargs):
        return fn(*args, **kwargs)

    with patch("deephaven_mcp.queries.asyncio.to_thread", new=fake_to_thread):
        result_table, is_complete = await _apply_row_limit(
            table_mock, max_rows=1000, head=True, context_name="test table"
        )
        assert result_table is limited_table_mock
        assert is_complete is True  # 1000 <= 1000


# ===== get_table tests =====


@pytest.mark.asyncio
async def test_get_table_success_full_table():
    """Test get_table with max_rows=None (full table)"""
    table_mock = MagicMock()
    arrow_mock = MagicMock(spec=pyarrow.Table)
    table_mock.to_arrow = lambda: arrow_mock
    session_mock = MagicMock()
    session_mock.open_table = AsyncMock(return_value=table_mock)

    async def fake_to_thread(fn, *args, **kwargs):
        return fn(*args, **kwargs)

    with patch("deephaven_mcp.queries.asyncio.to_thread", new=fake_to_thread):
        result_table, is_complete = await get_table(session_mock, "foo", max_rows=None)
        assert result_table is arrow_mock
        assert is_complete is True
        session_mock.open_table.assert_awaited_once_with("foo")


@pytest.mark.asyncio
async def test_get_table_open_table_error():
    session_mock = MagicMock()
    session_mock.open_table = AsyncMock(side_effect=RuntimeError("fail open"))
    with pytest.raises(RuntimeError, match="fail open"):
        await get_table(session_mock, "foo", max_rows=None)


@pytest.mark.asyncio
async def test_get_table_to_arrow_error():
    table_mock = MagicMock()
    table_mock.to_arrow = MagicMock(side_effect=RuntimeError("fail arrow"))
    session_mock = MagicMock()
    session_mock.open_table = AsyncMock(return_value=table_mock)

    async def fake_to_thread(fn, *args, **kwargs):
        return fn(*args, **kwargs)

    with patch("deephaven_mcp.queries.asyncio.to_thread", new=fake_to_thread):
        with pytest.raises(RuntimeError, match="fail arrow"):
            await get_table(session_mock, "foo", max_rows=None)


@pytest.mark.asyncio
async def test_get_table_head_complete_table():
    """Test get_table with head=True when table is smaller than max_rows"""
    original_table_mock = MagicMock()
    original_table_mock.size = 500  # Table has 500 rows
    head_table_mock = MagicMock()
    arrow_mock = MagicMock(spec=pyarrow.Table)
    arrow_mock.__len__ = lambda: 500  # Arrow table also has 500 rows
    head_table_mock.to_arrow = lambda: arrow_mock
    original_table_mock.head = lambda n: head_table_mock

    session_mock = MagicMock()
    session_mock.open_table = AsyncMock(return_value=original_table_mock)

    async def fake_to_thread(fn, *args, **kwargs):
        return fn(*args, **kwargs)

    with patch("deephaven_mcp.queries.asyncio.to_thread", new=fake_to_thread):
        result_table, is_complete = await get_table(
            session_mock, "foo", max_rows=1000, head=True
        )
        assert result_table is arrow_mock
        assert is_complete is True  # 500 <= 1000, so complete


@pytest.mark.asyncio
async def test_get_table_head_incomplete_table():
    """Test get_table with head=True when table is larger than max_rows"""
    original_table_mock = MagicMock()
    original_table_mock.size = 2000  # Table has 2000 rows
    head_table_mock = MagicMock()
    arrow_mock = MagicMock(spec=pyarrow.Table)
    arrow_mock.__len__ = lambda: 1000  # Arrow table has 1000 rows (limited)
    head_table_mock.to_arrow = lambda: arrow_mock
    original_table_mock.head = lambda n: head_table_mock

    session_mock = MagicMock()
    session_mock.open_table = AsyncMock(return_value=original_table_mock)

    async def fake_to_thread(fn, *args, **kwargs):
        return fn(*args, **kwargs)

    with patch("deephaven_mcp.queries.asyncio.to_thread", new=fake_to_thread):
        result_table, is_complete = await get_table(
            session_mock, "foo", max_rows=1000, head=True
        )
        assert result_table is arrow_mock
        assert is_complete is False  # 2000 > 1000, so incomplete


@pytest.mark.asyncio
async def test_get_table_tail_complete_table():
    """Test get_table with head=False (tail) when table is smaller than max_rows"""
    original_table_mock = MagicMock()
    original_table_mock.size = 300  # Table has 300 rows
    tail_table_mock = MagicMock()
    arrow_mock = MagicMock(spec=pyarrow.Table)
    arrow_mock.__len__ = lambda: 300  # Arrow table has 300 rows
    tail_table_mock.to_arrow = lambda: arrow_mock
    original_table_mock.tail = lambda n: tail_table_mock

    session_mock = MagicMock()
    session_mock.open_table = AsyncMock(return_value=original_table_mock)

    async def fake_to_thread(fn, *args, **kwargs):
        return fn(*args, **kwargs)

    with patch("deephaven_mcp.queries.asyncio.to_thread", new=fake_to_thread):
        result_table, is_complete = await get_table(
            session_mock, "foo", max_rows=500, head=False
        )
        assert result_table is arrow_mock
        assert is_complete is True  # 300 <= 500, so complete


@pytest.mark.asyncio
async def test_get_table_tail_incomplete_table():
    """Test get_table with head=False (tail) when table is larger than max_rows"""
    original_table_mock = MagicMock()
    original_table_mock.size = 1500  # Table has 1500 rows
    tail_table_mock = MagicMock()
    arrow_mock = MagicMock(spec=pyarrow.Table)
    arrow_mock.__len__ = lambda: 800  # Arrow table has 800 rows (limited)
    tail_table_mock.to_arrow = lambda: arrow_mock
    original_table_mock.tail = lambda n: tail_table_mock

    session_mock = MagicMock()
    session_mock.open_table = AsyncMock(return_value=original_table_mock)

    async def fake_to_thread(fn, *args, **kwargs):
        return fn(*args, **kwargs)

    with patch("deephaven_mcp.queries.asyncio.to_thread", new=fake_to_thread):
        result_table, is_complete = await get_table(
            session_mock, "foo", max_rows=800, head=False
        )
        assert result_table is arrow_mock
        assert is_complete is False  # 1500 > 800, so incomplete


@pytest.mark.asyncio
async def test_get_table_exact_size_match():
    """Test get_table when original table size exactly matches max_rows"""
    original_table_mock = MagicMock()
    original_table_mock.size = 1000  # Table has exactly 1000 rows
    head_table_mock = MagicMock()
    arrow_mock = MagicMock(spec=pyarrow.Table)
    arrow_mock.__len__ = lambda: 1000  # Arrow table has 1000 rows
    head_table_mock.to_arrow = lambda: arrow_mock
    original_table_mock.head = lambda n: head_table_mock

    session_mock = MagicMock()
    session_mock.open_table = AsyncMock(return_value=original_table_mock)

    async def fake_to_thread(fn, *args, **kwargs):
        return fn(*args, **kwargs)

    with patch("deephaven_mcp.queries.asyncio.to_thread", new=fake_to_thread):
        result_table, is_complete = await get_table(
            session_mock, "foo", max_rows=1000, head=True
        )
        assert result_table is arrow_mock
        assert is_complete is True  # 1000 <= 1000, so complete


@pytest.mark.asyncio
async def test_get_table_keyword_only_max_rows():
    """Test that max_rows must be specified as keyword argument"""
    session_mock = MagicMock()

    # This should raise TypeError because max_rows is keyword-only
    with pytest.raises(TypeError):
        await get_table(session_mock, "foo", 1000)  # Positional argument should fail


@pytest.mark.asyncio
async def test_get_table_head_parameter_ignored_with_full_table():
    """Test that head parameter is ignored when max_rows=None"""
    table_mock = MagicMock()
    arrow_mock = MagicMock(spec=pyarrow.Table)
    table_mock.to_arrow = lambda: arrow_mock
    session_mock = MagicMock()
    session_mock.open_table = AsyncMock(return_value=table_mock)

    async def fake_to_thread(fn, *args, **kwargs):
        return fn(*args, **kwargs)

    with patch("deephaven_mcp.queries.asyncio.to_thread", new=fake_to_thread):
        # Both head=True and head=False should behave identically with max_rows=None
        result1, complete1 = await get_table(
            session_mock, "foo", max_rows=None, head=True
        )
        result2, complete2 = await get_table(
            session_mock, "foo", max_rows=None, head=False
        )

        assert result1 is arrow_mock
        assert result2 is arrow_mock
        assert complete1 is True
        assert complete2 is True


@pytest.mark.asyncio
async def test_get_session_meta_table_success():
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
        result = await get_session_meta_table(session_mock, "foo")
        assert result is arrow_mock
        session_mock.open_table.assert_awaited_once_with("foo")


@pytest.mark.asyncio
async def test_get_session_meta_table_open_table_error():
    session_mock = MagicMock()
    session_mock.open_table = AsyncMock(side_effect=RuntimeError("fail-open"))
    with pytest.raises(RuntimeError) as excinfo:
        await get_session_meta_table(session_mock, "foo")
    assert "fail-open" in str(excinfo.value)
    session_mock.open_table.assert_awaited_once_with("foo")


@pytest.mark.asyncio
async def test_get_session_meta_table_to_arrow_error():
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
            await get_session_meta_table(session_mock, "foo")
        assert "fail-arrow" in str(excinfo.value)
        session_mock.open_table.assert_awaited_once_with("foo")


# ===== _extract_meta_table tests =====


@pytest.mark.asyncio
async def test_extract_meta_table_success():
    """Test _extract_meta_table successfully extracts meta table from a table"""
    table_mock = MagicMock()
    meta_table_mock = MagicMock()
    arrow_mock = object()
    type(table_mock).meta_table = property(lambda self: meta_table_mock)

    def to_arrow():
        return arrow_mock

    meta_table_mock.to_arrow = to_arrow

    async def fake_to_thread(fn, *args, **kwargs):
        return fn(*args, **kwargs)

    with patch("deephaven_mcp.queries.asyncio.to_thread", new=fake_to_thread):
        result = await _extract_meta_table(table_mock, "test_table")
        assert result is arrow_mock


@pytest.mark.asyncio
async def test_extract_meta_table_error():
    """Test _extract_meta_table handles errors properly"""
    table_mock = MagicMock()
    meta_table_mock = MagicMock()
    type(table_mock).meta_table = property(lambda self: meta_table_mock)

    def to_arrow():
        raise RuntimeError("Meta table extraction failed")

    meta_table_mock.to_arrow = to_arrow

    async def fake_to_thread(fn, *args, **kwargs):
        return fn(*args, **kwargs)

    with patch("deephaven_mcp.queries.asyncio.to_thread", new=fake_to_thread):
        with pytest.raises(RuntimeError, match="Meta table extraction failed"):
            await _extract_meta_table(table_mock, "test_table")


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
            AsyncMock(return_value=(arrow_mock, True)),
        ) as mock_get_table:
            with caplog.at_level("DEBUG"):
                result = await get_pip_packages_table(session_mock)
            assert result is arrow_mock
            assert "Running pip packages script in session..." in caplog.text
            assert "Script executed successfully." in caplog.text
            assert "Table '_pip_packages_table' retrieved successfully." in caplog.text
            session_mock.run_script.assert_awaited_once()
            mock_get_table.assert_awaited_once_with(
                session_mock, "_pip_packages_table", max_rows=None
            )


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
            AsyncMock(return_value=(arrow_mock, True)),
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
                session_mock, "_python_version_table", max_rows=None
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


# ===== get_catalog_table tests =====


@pytest.mark.asyncio
async def test_get_catalog_table_success_no_filters():
    """Test get_catalog_table with no filters and row limit"""
    from deephaven_mcp.client import CorePlusSession

    # Create mock catalog table
    catalog_table_mock = MagicMock()
    catalog_table_mock.size = 5000
    limited_table_mock = MagicMock()
    arrow_mock = MagicMock(spec=pyarrow.Table)
    limited_table_mock.to_arrow = lambda: arrow_mock
    catalog_table_mock.head = lambda n: limited_table_mock

    # Create mock session
    session_mock = MagicMock(spec=CorePlusSession)
    session_mock.catalog_table = AsyncMock(return_value=catalog_table_mock)

    async def fake_to_thread(fn, *args, **kwargs):
        return fn(*args, **kwargs)

    with patch("deephaven_mcp.queries.asyncio.to_thread", new=fake_to_thread):
        result_table, is_complete = await get_catalog_table(
            session_mock, max_rows=1000, filters=None, distinct_namespaces=False
        )
        assert result_table is arrow_mock
        assert is_complete is False  # 5000 > 1000, so incomplete
        session_mock.catalog_table.assert_awaited_once()


@pytest.mark.asyncio
async def test_get_catalog_table_success_with_filters():
    """Test get_catalog_table with filters applied"""
    from deephaven_mcp.client import CorePlusSession

    # Create mock filtered table
    filtered_table_mock = MagicMock()
    filtered_table_mock.size = 50
    filtered_table_mock.head = lambda n: filtered_table_mock
    arrow_mock = MagicMock(spec=pyarrow.Table)
    filtered_table_mock.to_arrow = lambda: arrow_mock

    # Create mock catalog table with where method
    catalog_table_mock = MagicMock()
    catalog_table_mock.where = lambda filters: filtered_table_mock

    # Create mock session
    session_mock = MagicMock(spec=CorePlusSession)
    session_mock.catalog_table = AsyncMock(return_value=catalog_table_mock)

    async def fake_to_thread(fn, *args, **kwargs):
        return fn(*args, **kwargs)

    with patch("deephaven_mcp.queries.asyncio.to_thread", new=fake_to_thread):
        result_table, is_complete = await get_catalog_table(
            session_mock,
            max_rows=1000,
            filters=["Namespace = `market_data`", "TableName.contains(`price`)"],
            distinct_namespaces=False,
        )
        assert result_table is arrow_mock
        assert is_complete is True  # 50 <= 1000, so complete
        session_mock.catalog_table.assert_awaited_once()


@pytest.mark.asyncio
async def test_get_catalog_table_success_full_catalog():
    """Test get_catalog_table with max_rows=None (full catalog)"""
    from deephaven_mcp.client import CorePlusSession

    # Create mock catalog table
    catalog_table_mock = MagicMock()
    arrow_mock = MagicMock(spec=pyarrow.Table)
    catalog_table_mock.to_arrow = lambda: arrow_mock

    # Create mock session
    session_mock = MagicMock(spec=CorePlusSession)
    session_mock.catalog_table = AsyncMock(return_value=catalog_table_mock)

    async def fake_to_thread(fn, *args, **kwargs):
        return fn(*args, **kwargs)

    with patch("deephaven_mcp.queries.asyncio.to_thread", new=fake_to_thread):
        result_table, is_complete = await get_catalog_table(
            session_mock, max_rows=None, filters=None, distinct_namespaces=False
        )
        assert result_table is arrow_mock
        assert is_complete is True
        session_mock.catalog_table.assert_awaited_once()


@pytest.mark.asyncio
async def test_get_catalog_table_not_enterprise_session():
    """Test get_catalog_table raises error for non-enterprise session"""
    from deephaven_mcp.client import BaseSession

    # Create mock community session (not CorePlusSession)
    session_mock = MagicMock(spec=BaseSession)

    with pytest.raises(
        UnsupportedOperationError,
        match="get_catalog_table only supports enterprise.*sessions",
    ):
        await get_catalog_table(session_mock, max_rows=1000, distinct_namespaces=False)


@pytest.mark.asyncio
async def test_get_catalog_table_catalog_retrieval_error():
    """Test get_catalog_table handles catalog_table() errors"""
    from deephaven_mcp.client import CorePlusSession

    # Create mock session that fails to get catalog
    session_mock = MagicMock(spec=CorePlusSession)
    session_mock.catalog_table = AsyncMock(
        side_effect=RuntimeError("Catalog not available")
    )

    with pytest.raises(RuntimeError, match="Catalog not available"):
        await get_catalog_table(session_mock, max_rows=1000, distinct_namespaces=False)


@pytest.mark.asyncio
async def test_get_catalog_table_filter_error():
    """Test get_catalog_table handles invalid filter syntax"""
    from deephaven_mcp.client import CorePlusSession

    # Create mock catalog table with where method that raises error
    catalog_table_mock = MagicMock()
    catalog_table_mock.where = MagicMock(
        side_effect=RuntimeError("Invalid filter syntax")
    )

    # Create mock session
    session_mock = MagicMock(spec=CorePlusSession)
    session_mock.catalog_table = AsyncMock(return_value=catalog_table_mock)

    async def fake_to_thread(fn, *args, **kwargs):
        return fn(*args, **kwargs)

    with patch("deephaven_mcp.queries.asyncio.to_thread", new=fake_to_thread):
        with pytest.raises(RuntimeError, match="Invalid filter syntax"):
            await get_catalog_table(
                session_mock,
                max_rows=1000,
                filters=["InvalidFilter!!!"],
                distinct_namespaces=False,
            )


# ===== get_catalog_table with distinct_namespaces tests =====


@pytest.mark.asyncio
async def test_get_catalog_table_distinct_namespaces_success_no_filters():
    """Test get_catalog_table with distinct_namespaces=True and no filters"""
    from deephaven_mcp.client import CorePlusSession
    from deephaven_mcp.queries import get_catalog_table

    # Create mock namespace table (after sort)
    sorted_namespace_table_mock = MagicMock()
    sorted_namespace_table_mock.size = 50
    sorted_namespace_table_mock.head = lambda n: sorted_namespace_table_mock
    arrow_mock = MagicMock(spec=pyarrow.Table)
    sorted_namespace_table_mock.to_arrow = lambda: arrow_mock

    # Create mock namespace table (after select_distinct)
    namespace_table_mock = MagicMock()
    namespace_table_mock.sort = lambda col: sorted_namespace_table_mock

    # Create mock catalog table with select_distinct
    catalog_table_mock = MagicMock()
    catalog_table_mock.select_distinct = lambda col: namespace_table_mock

    # Create mock session
    session_mock = MagicMock(spec=CorePlusSession)
    session_mock.catalog_table = AsyncMock(return_value=catalog_table_mock)

    async def fake_to_thread(fn, *args, **kwargs):
        return fn(*args, **kwargs)

    with patch("deephaven_mcp.queries.asyncio.to_thread", new=fake_to_thread):
        result_table, is_complete = await get_catalog_table(
            session_mock, max_rows=1000, distinct_namespaces=True
        )
        assert result_table is arrow_mock
        assert is_complete is True  # 50 <= 1000, so complete


@pytest.mark.asyncio
async def test_get_catalog_namespaces_success_with_filters():
    """Test get_catalog_namespaces with filters applied"""
    from deephaven_mcp.client import CorePlusSession
    from deephaven_mcp.queries import get_catalog_table

    # Create mock filtered namespace table (after where)
    filtered_namespace_table_mock = MagicMock()
    filtered_namespace_table_mock.size = 10
    filtered_namespace_table_mock.head = lambda n: filtered_namespace_table_mock
    arrow_mock = MagicMock(spec=pyarrow.Table)
    filtered_namespace_table_mock.to_arrow = lambda: arrow_mock

    # Create mock sorted namespace table (after sort)
    sorted_namespace_table_mock = MagicMock()
    sorted_namespace_table_mock.where = lambda filters: filtered_namespace_table_mock

    # Create mock namespace table (after select_distinct)
    namespace_table_mock = MagicMock()
    namespace_table_mock.sort = lambda col: sorted_namespace_table_mock

    # Create mock catalog table with select_distinct
    catalog_table_mock = MagicMock()
    catalog_table_mock.select_distinct = lambda col: namespace_table_mock

    # Create mock session
    session_mock = MagicMock(spec=CorePlusSession)
    session_mock.catalog_table = AsyncMock(return_value=catalog_table_mock)

    async def fake_to_thread(fn, *args, **kwargs):
        return fn(*args, **kwargs)

    with patch("deephaven_mcp.queries.asyncio.to_thread", new=fake_to_thread):
        result_table, is_complete = await get_catalog_table(
            session_mock,
            max_rows=1000,
            filters=["TableName.contains(`daily`)"],
            distinct_namespaces=True,
        )
        assert result_table is arrow_mock
        assert is_complete is True


@pytest.mark.asyncio
async def test_get_catalog_namespaces_success_full():
    """Test get_catalog_namespaces with max_rows=None (full namespaces)"""
    from deephaven_mcp.client import CorePlusSession
    from deephaven_mcp.queries import get_catalog_table

    # Create mock sorted namespace table
    sorted_namespace_table_mock = MagicMock()
    arrow_mock = MagicMock(spec=pyarrow.Table)
    sorted_namespace_table_mock.to_arrow = lambda: arrow_mock

    # Create mock namespace table (after select_distinct)
    namespace_table_mock = MagicMock()
    namespace_table_mock.sort = lambda col: sorted_namespace_table_mock

    # Create mock catalog table
    catalog_table_mock = MagicMock()
    catalog_table_mock.select_distinct = lambda col: namespace_table_mock

    # Create mock session
    session_mock = MagicMock(spec=CorePlusSession)
    session_mock.catalog_table = AsyncMock(return_value=catalog_table_mock)

    async def fake_to_thread(fn, *args, **kwargs):
        return fn(*args, **kwargs)

    with patch("deephaven_mcp.queries.asyncio.to_thread", new=fake_to_thread):
        result_table, is_complete = await get_catalog_table(
            session_mock, max_rows=None, distinct_namespaces=True
        )
        assert result_table is arrow_mock
        assert is_complete is True


@pytest.mark.asyncio
async def test_get_catalog_namespaces_incomplete():
    """Test get_catalog_namespaces when results are incomplete"""
    from deephaven_mcp.client import CorePlusSession
    from deephaven_mcp.queries import get_catalog_table

    # Create mock limited table (after head)
    limited_table_mock = MagicMock()
    arrow_mock = MagicMock(spec=pyarrow.Table)
    limited_table_mock.to_arrow = lambda: arrow_mock

    # Create mock sorted namespace table with more rows than max_rows
    sorted_namespace_table_mock = MagicMock()
    sorted_namespace_table_mock.size = 2000
    sorted_namespace_table_mock.head = lambda n: limited_table_mock

    # Create mock namespace table (after select_distinct)
    namespace_table_mock = MagicMock()
    namespace_table_mock.sort = lambda col: sorted_namespace_table_mock

    # Create mock catalog table
    catalog_table_mock = MagicMock()
    catalog_table_mock.select_distinct = lambda col: namespace_table_mock

    # Create mock session
    session_mock = MagicMock(spec=CorePlusSession)
    session_mock.catalog_table = AsyncMock(return_value=catalog_table_mock)

    async def fake_to_thread(fn, *args, **kwargs):
        return fn(*args, **kwargs)

    with patch("deephaven_mcp.queries.asyncio.to_thread", new=fake_to_thread):
        result_table, is_complete = await get_catalog_table(
            session_mock, max_rows=1000, distinct_namespaces=True
        )
        assert result_table is arrow_mock
        assert is_complete is False  # 2000 > 1000, so incomplete


@pytest.mark.asyncio
async def test_get_catalog_namespaces_not_enterprise_session():
    """Test get_catalog_namespaces with non-enterprise session (should fail)"""
    from deephaven_mcp._exceptions import UnsupportedOperationError
    from deephaven_mcp.queries import get_catalog_table

    # Create mock non-enterprise session
    session_mock = MagicMock()
    session_mock.catalog_table = AsyncMock()

    with pytest.raises(
        UnsupportedOperationError,
        match="get_catalog_table only supports enterprise.*sessions",
    ):
        await get_catalog_table(session_mock, max_rows=1000, distinct_namespaces=True)


@pytest.mark.asyncio
async def test_get_catalog_namespaces_catalog_retrieval_error():
    """Test get_catalog_namespaces handles catalog retrieval errors"""
    from deephaven_mcp.client import CorePlusSession
    from deephaven_mcp.queries import get_catalog_table

    # Create mock session that fails to retrieve catalog
    session_mock = MagicMock(spec=CorePlusSession)
    session_mock.catalog_table = AsyncMock(
        side_effect=RuntimeError("Catalog not available")
    )

    with pytest.raises(RuntimeError, match="Catalog not available"):
        await get_catalog_table(session_mock, max_rows=1000, distinct_namespaces=True)


# ===== get_catalog_meta_table tests =====


@pytest.mark.asyncio
async def test_get_catalog_meta_table_success_historical():
    """Test get_catalog_meta_table successfully retrieves meta table via historical_table"""
    from deephaven_mcp.client import CorePlusSession

    # Create mock session
    session_mock = MagicMock(spec=CorePlusSession)

    # Mock table with meta_table
    mock_table = MagicMock()
    mock_meta_table = MagicMock()
    mock_arrow_table = MagicMock(spec=pyarrow.Table)

    mock_meta_table.to_arrow = MagicMock(return_value=mock_arrow_table)
    mock_table.meta_table = mock_meta_table

    # historical_table succeeds
    session_mock.historical_table = AsyncMock(return_value=mock_table)

    # Mock asyncio.to_thread to run synchronously
    async def fake_to_thread(fn, *args, **kwargs):
        return fn(*args, **kwargs)

    with patch("deephaven_mcp.queries.asyncio.to_thread", new=fake_to_thread):
        result = await get_catalog_meta_table(
            session_mock, "market_data", "daily_prices"
        )

    assert result is mock_arrow_table
    session_mock.historical_table.assert_called_once_with("market_data", "daily_prices")


@pytest.mark.asyncio
async def test_get_catalog_meta_table_fallback_to_live():
    """Test get_catalog_meta_table falls back to live_table when historical_table fails"""
    from deephaven_mcp.client import CorePlusSession

    # Create mock session
    session_mock = MagicMock(spec=CorePlusSession)

    # Mock table with meta_table
    mock_table = MagicMock()
    mock_meta_table = MagicMock()
    mock_arrow_table = MagicMock(spec=pyarrow.Table)

    mock_meta_table.to_arrow = MagicMock(return_value=mock_arrow_table)
    mock_table.meta_table = mock_meta_table

    # historical_table fails, live_table succeeds
    session_mock.historical_table = AsyncMock(
        side_effect=Exception("Historical table not found")
    )
    session_mock.live_table = AsyncMock(return_value=mock_table)

    # Mock asyncio.to_thread to run synchronously
    async def fake_to_thread(fn, *args, **kwargs):
        return fn(*args, **kwargs)

    with patch("deephaven_mcp.queries.asyncio.to_thread", new=fake_to_thread):
        result = await get_catalog_meta_table(
            session_mock, "market_data", "live_trades"
        )

    assert result is mock_arrow_table
    session_mock.historical_table.assert_called_once_with("market_data", "live_trades")
    session_mock.live_table.assert_called_once_with("market_data", "live_trades")


@pytest.mark.asyncio
async def test_get_catalog_meta_table_both_fail():
    """Test get_catalog_meta_table raises exception when both historical and live fail"""
    from deephaven_mcp.client import CorePlusSession

    # Create mock session
    session_mock = MagicMock(spec=CorePlusSession)

    # Both historical_table and live_table fail
    session_mock.historical_table = AsyncMock(
        side_effect=Exception("Historical table not found")
    )
    session_mock.live_table = AsyncMock(side_effect=Exception("Live table not found"))

    with pytest.raises(
        Exception,
        match="Failed to load catalog table 'market_data.missing_table'",
    ):
        await get_catalog_meta_table(session_mock, "market_data", "missing_table")


# ===== _load_catalog_table tests =====


@pytest.mark.asyncio
async def test_load_catalog_table_success_historical():
    """Test _load_catalog_table successfully loads via historical_table"""
    from deephaven_mcp.client import CorePlusSession

    # Create mock session
    session_mock = MagicMock(spec=CorePlusSession)
    mock_table = MagicMock()

    # historical_table succeeds
    session_mock.historical_table = AsyncMock(return_value=mock_table)

    result = await _load_catalog_table(session_mock, "market_data", "daily_prices")

    assert result is mock_table
    session_mock.historical_table.assert_called_once_with("market_data", "daily_prices")


@pytest.mark.asyncio
async def test_load_catalog_table_fallback_to_live():
    """Test _load_catalog_table falls back to live_table when historical_table fails"""
    from deephaven_mcp.client import CorePlusSession

    # Create mock session
    session_mock = MagicMock(spec=CorePlusSession)
    mock_table = MagicMock()

    # historical_table fails, live_table succeeds
    session_mock.historical_table = AsyncMock(
        side_effect=Exception("Historical table not found")
    )
    session_mock.live_table = AsyncMock(return_value=mock_table)

    result = await _load_catalog_table(session_mock, "market_data", "live_trades")

    assert result is mock_table
    session_mock.historical_table.assert_called_once_with("market_data", "live_trades")
    session_mock.live_table.assert_called_once_with("market_data", "live_trades")


@pytest.mark.asyncio
async def test_load_catalog_table_both_fail():
    """Test _load_catalog_table raises exception when both historical and live fail"""
    from deephaven_mcp.client import CorePlusSession

    # Create mock session
    session_mock = MagicMock(spec=CorePlusSession)

    # Both historical_table and live_table fail
    session_mock.historical_table = AsyncMock(
        side_effect=Exception("Historical table not found")
    )
    session_mock.live_table = AsyncMock(side_effect=Exception("Live table not found"))

    with pytest.raises(
        Exception,
        match="Failed to load catalog table 'market_data.missing_table'",
    ):
        await _load_catalog_table(session_mock, "market_data", "missing_table")


# ===== get_catalog_table_data tests =====


@pytest.mark.asyncio
async def test_get_catalog_table_data_success_with_limit():
    """Test get_catalog_table_data successfully retrieves data with row limit"""
    from deephaven_mcp.client import CorePlusSession

    # Create mock session
    session_mock = MagicMock(spec=CorePlusSession)
    mock_table = MagicMock()
    mock_limited_table = MagicMock()
    mock_arrow_table = MagicMock(spec=pyarrow.Table)
    mock_arrow_table.num_rows = 100

    # Mock table size
    mock_table.size = 1000

    # Mock head() for row limiting
    mock_table.head = MagicMock(return_value=mock_limited_table)
    mock_limited_table.to_arrow = MagicMock(return_value=mock_arrow_table)

    # historical_table succeeds
    session_mock.historical_table = AsyncMock(return_value=mock_table)

    # Mock asyncio.to_thread to run synchronously
    async def fake_to_thread(fn, *args, **kwargs):
        return fn(*args, **kwargs)

    with patch("deephaven_mcp.queries.asyncio.to_thread", new=fake_to_thread):
        result, is_complete = await get_catalog_table_data(
            session_mock, "market_data", "daily_prices", max_rows=100, head=True
        )

    assert result is mock_arrow_table
    assert is_complete is False  # 100 rows retrieved from 1000 total
    session_mock.historical_table.assert_called_once_with("market_data", "daily_prices")
    mock_table.head.assert_called_once_with(100)


@pytest.mark.asyncio
async def test_get_catalog_table_data_success_full_table():
    """Test get_catalog_table_data retrieves full table when max_rows=None"""
    from deephaven_mcp.client import CorePlusSession

    # Create mock session
    session_mock = MagicMock(spec=CorePlusSession)
    mock_table = MagicMock()
    mock_arrow_table = MagicMock(spec=pyarrow.Table)
    mock_arrow_table.num_rows = 500

    mock_table.to_arrow = MagicMock(return_value=mock_arrow_table)

    # historical_table succeeds
    session_mock.historical_table = AsyncMock(return_value=mock_table)

    # Mock asyncio.to_thread to run synchronously
    async def fake_to_thread(fn, *args, **kwargs):
        return fn(*args, **kwargs)

    with patch("deephaven_mcp.queries.asyncio.to_thread", new=fake_to_thread):
        result, is_complete = await get_catalog_table_data(
            session_mock, "market_data", "small_table", max_rows=None
        )

    assert result is mock_arrow_table
    assert is_complete is True  # Full table retrieved
    session_mock.historical_table.assert_called_once_with("market_data", "small_table")


@pytest.mark.asyncio
async def test_get_catalog_table_data_with_tail():
    """Test get_catalog_table_data retrieves last rows when head=False"""
    from deephaven_mcp.client import CorePlusSession

    # Create mock session
    session_mock = MagicMock(spec=CorePlusSession)
    mock_table = MagicMock()
    mock_limited_table = MagicMock()
    mock_arrow_table = MagicMock(spec=pyarrow.Table)
    mock_arrow_table.num_rows = 50

    # Mock table size
    mock_table.size = 1000

    # Mock tail() for row limiting
    mock_table.tail = MagicMock(return_value=mock_limited_table)
    mock_limited_table.to_arrow = MagicMock(return_value=mock_arrow_table)

    # historical_table succeeds
    session_mock.historical_table = AsyncMock(return_value=mock_table)

    # Mock asyncio.to_thread to run synchronously
    async def fake_to_thread(fn, *args, **kwargs):
        return fn(*args, **kwargs)

    with patch("deephaven_mcp.queries.asyncio.to_thread", new=fake_to_thread):
        result, is_complete = await get_catalog_table_data(
            session_mock, "market_data", "trades", max_rows=50, head=False
        )

    assert result is mock_arrow_table
    assert is_complete is False  # 50 rows retrieved from 1000 total
    session_mock.historical_table.assert_called_once_with("market_data", "trades")
    mock_table.tail.assert_called_once_with(50)


@pytest.mark.asyncio
async def test_get_catalog_table_data_fallback_to_live():
    """Test get_catalog_table_data falls back to live_table when historical fails"""
    from deephaven_mcp.client import CorePlusSession

    # Create mock session
    session_mock = MagicMock(spec=CorePlusSession)
    mock_table = MagicMock()
    mock_arrow_table = MagicMock(spec=pyarrow.Table)
    mock_arrow_table.num_rows = 100

    mock_table.to_arrow = MagicMock(return_value=mock_arrow_table)

    # historical_table fails, live_table succeeds
    session_mock.historical_table = AsyncMock(
        side_effect=Exception("Historical table not found")
    )
    session_mock.live_table = AsyncMock(return_value=mock_table)

    # Mock asyncio.to_thread to run synchronously
    async def fake_to_thread(fn, *args, **kwargs):
        return fn(*args, **kwargs)

    with patch("deephaven_mcp.queries.asyncio.to_thread", new=fake_to_thread):
        result, is_complete = await get_catalog_table_data(
            session_mock, "market_data", "live_trades", max_rows=None
        )

    assert result is mock_arrow_table
    assert is_complete is True
    session_mock.historical_table.assert_called_once_with("market_data", "live_trades")
    session_mock.live_table.assert_called_once_with("market_data", "live_trades")


@pytest.mark.asyncio
async def test_get_catalog_table_data_load_failure():
    """Test get_catalog_table_data raises exception when table cannot be loaded"""
    from deephaven_mcp.client import CorePlusSession

    # Create mock session
    session_mock = MagicMock(spec=CorePlusSession)

    # Both historical_table and live_table fail
    session_mock.historical_table = AsyncMock(
        side_effect=Exception("Historical table not found")
    )
    session_mock.live_table = AsyncMock(side_effect=Exception("Live table not found"))

    with pytest.raises(
        Exception,
        match="Failed to load catalog table 'market_data.missing_table'",
    ):
        await get_catalog_table_data(
            session_mock, "market_data", "missing_table", max_rows=100
        )
