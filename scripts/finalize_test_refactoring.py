#!/usr/bin/env python3
"""
Finalize test refactoring by:
1. Creating test__mcp.py validation stub with export assertions
2. Moving single-use helpers from conftest.py to their test files
3. Validating the final structure
"""

from pathlib import Path
import re


def create_validation_stub(repo_root):
    """Create test__mcp.py to test the _mcp.py module."""
    test_mcp_file = repo_root / "tests/mcp_systems_server/test__mcp.py"
    
    content = '''"""Tests for deephaven_mcp.mcp_systems_server._mcp module."""

import pytest


def test_mcp_module_exports_all_match():
    """Validate that _mcp.py __all__ matches actual exports."""
    import deephaven_mcp.mcp_systems_server._mcp as mcp_mod
    
    # Get __all__ list from _mcp.py
    expected_exports = set(mcp_mod.__all__)
    
    # Everything in __all__ should be importable
    for name in expected_exports:
        assert hasattr(mcp_mod, name), f"{name} in __all__ but not found in module"
    
    # Everything public should be in __all__
    public_attrs = set(name for name in dir(mcp_mod) if not name.startswith('_'))
    dunder_attrs = {'__all__', '__doc__', '__file__', '__name__', '__package__', '__spec__', '__cached__', '__loader__'}
    unlisted_public = public_attrs - expected_exports - dunder_attrs
    
    assert not unlisted_public, f"Public attributes not in __all__: {sorted(unlisted_public)}"


def test_mcp_module_exports_count():
    """Validate _mcp.py exports the expected number of items."""
    import deephaven_mcp.mcp_systems_server._mcp as mcp_mod
    
    assert len(mcp_mod.__all__) == 96, f"Expected 96 exports, found {len(mcp_mod.__all__)}"
'''
    
    with open(test_mcp_file, 'w') as f:
        f.write(content)
    
    print(f"✅ Created {test_mcp_file}")


def move_single_use_helpers(repo_root):
    """Move single-use helpers from conftest.py to their respective test files."""
    conftest_file = repo_root / "tests/mcp_systems_server/_tools/conftest.py"
    
    # Read current conftest
    with open(conftest_file) as f:
        conftest_content = f.read()
    
    # Extract helpers to move
    helpers_to_move = {}
    
    # Find create_mock_arrow_meta_table (used only in catalog)
    arrow_match = re.search(
        r'(def create_mock_arrow_meta_table.*?(?=\n(?:def |class |\Z)))',
        conftest_content,
        re.DOTALL
    )
    if arrow_match:
        helpers_to_move['catalog'] = helpers_to_move.get('catalog', [])
        helpers_to_move['catalog'].append(arrow_match.group(1))
    
    # Find create_mock_catalog_schema_function (used only in catalog)
    catalog_match = re.search(
        r'(def create_mock_catalog_schema_function.*?(?=\n(?:def |class |\Z)))',
        conftest_content,
        re.DOTALL
    )
    if catalog_match:
        helpers_to_move['catalog'] = helpers_to_move.get('catalog', [])
        helpers_to_move['catalog'].append(catalog_match.group(1))
    
    # Find create_mock_pq_info (used only in pq)
    pq_match = re.search(
        r'(def create_mock_pq_info.*?(?=\n(?:def |class |\Z)))',
        conftest_content,
        re.DOTALL
    )
    if pq_match:
        helpers_to_move['pq'] = [pq_match.group(1)]
    
    # Remove these helpers from conftest and keep only shared ones
    new_conftest = '''"""Shared test fixtures and helpers for mcp_systems_server tests."""

from unittest.mock import AsyncMock, MagicMock


class MockRequestContext:
    """Mock MCP request context for testing."""
    def __init__(self, lifespan_context):
        self.lifespan_context = lifespan_context


class MockContext:
    """Mock MCP context for testing."""
    def __init__(self, lifespan_context):
        self.request_context = MockRequestContext(lifespan_context)


def create_mock_instance_tracker():
    """Create a mock InstanceTracker for tests."""
    mock_tracker = MagicMock()
    mock_tracker.instance_id = "test-instance-id"
    mock_tracker.track_python_process = AsyncMock()
    mock_tracker.untrack_python_process = AsyncMock()
    return mock_tracker
'''
    
    with open(conftest_file, 'w') as f:
        f.write(new_conftest)
    
    print(f"✅ Updated {conftest_file} - kept only shared utilities")
    
    # Add helpers to their respective test files
    for module_name, helpers in helpers_to_move.items():
        test_file = repo_root / f"tests/mcp_systems_server/_tools/test_{module_name}.py"
        
        with open(test_file) as f:
            content = f.read()
        
        # Find insertion point (after imports, before first test/class)
        import_end = content.rfind('\nimport')
        if import_end != -1:
            # Find end of that import line
            next_newline = content.find('\n\n', import_end + 1)
            if next_newline != -1:
                # Insert helpers after imports
                helper_block = '\n\n# Test-specific helper functions (only used in this file)\n' + '\n\n'.join(helpers) + '\n\n'
                new_content = content[:next_newline] + helper_block + content[next_newline:]
                
                with open(test_file, 'w') as f:
                    f.write(new_content)
                
                print(f"✅ Added {len(helpers)} helper(s) to {test_file.name}")


def main():
    """Execute finalization steps."""
    print("=" * 80)
    print("FINALIZING TEST REFACTORING")
    print("=" * 80)
    
    repo_root = Path(__file__).parent.parent
    
    print("\n1. Creating test__mcp.py validation stub...")
    create_validation_stub(repo_root)
    
    print("\n2. Moving single-use helpers to their test files...")
    move_single_use_helpers(repo_root)
    
    print("\n" + "=" * 80)
    print("✅ FINALIZATION COMPLETE")
    print("=" * 80)
    print("\nNext: Run test suite")
    print("  uv run pytest tests/mcp_systems_server/ -v")


if __name__ == "__main__":
    main()
