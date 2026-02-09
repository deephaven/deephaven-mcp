#!/usr/bin/env python3
"""
Automated refactoring program to split _mcp.py into logical modules.

This program performs byte-for-byte extraction of functions and constants.
"""

import re
from pathlib import Path


MODULE_IMPORTS = """import asyncio
import logging
import os
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from datetime import datetime
from typing import TYPE_CHECKING, Any, Literal, TypeVar, cast

import aiofiles
import pyarrow
from mcp.server.fastmcp import Context, FastMCP

from deephaven_mcp import queries
from deephaven_mcp._exceptions import (
    CommunitySessionConfigurationError,
    MissingEnterprisePackageError,
    UnsupportedOperationError,
)
from deephaven_mcp.client import BaseSession, CorePlusSession
from deephaven_mcp.client._controller_client import CorePlusControllerClient
from deephaven_mcp.client._protobuf import (
    CorePlusQueryConfig,
    CorePlusQuerySerial,
    CorePlusQueryState,
)

if TYPE_CHECKING:
    from deephaven.proto.table_pb2 import (
        ColumnDefinitionMessage,
        ExportedObjectInfoMessage,
        TableDefinitionMessage,
    )
    from deephaven_enterprise.proto.controller_common_pb2 import (
        NamedStringList,
    )
    from deephaven_enterprise.proto.persistent_query_pb2 import (
        ExceptionDetailsMessage,
        PersistentQueryConfigMessage,
        ProcessorConnectionDetailsMessage,
        WorkerProtocolMessage,
    )

try:
    from deephaven_enterprise.proto.persistent_query_pb2 import (
        ExportedObjectTypeEnum,
        RestartUsersEnum,
    )
except ImportError:
    ExportedObjectTypeEnum = None
    RestartUsersEnum = None

from deephaven_mcp.config import (
    ConfigManager,
    get_config_section,
    redact_enterprise_system_config,
)
from deephaven_mcp.formatters import format_table_data
from deephaven_mcp.resource_manager import (
    BaseItemManager,
    CombinedSessionRegistry,
    CommunitySessionManager,
    DockerLaunchedSession,
    DynamicCommunitySessionManager,
    EnterpriseSessionManager,
    LaunchedSession,
    PythonLaunchedSession,
    SystemType,
    find_available_port,
    generate_auth_token,
    launch_session,
)
from deephaven_mcp.resource_manager._instance_tracker import (
    InstanceTracker,
    cleanup_orphaned_resources,
)

T = TypeVar("T")

_LOGGER = logging.getLogger(__name__)
"""

# Module groupings based on approved REFACTORING_PLAN.md
MODULE_STRUCTURE = {
    "shared.py": [
        "_get_session_from_context",
        "_get_enterprise_session",
        "_check_response_size",
        "_format_meta_table_result",
        "_get_system_config",
    ],
    "mcp_server.py": [
        "app_lifespan",
        "mcp_reload",
    ],
    "session_enterprise.py": [
        "enterprise_systems_status",
        "_check_session_limits",
        "_generate_session_name_if_none",
        "_check_session_id_available",
        "_resolve_session_parameters",
        "session_enterprise_create",
        "session_enterprise_delete",
    ],
    "session.py": [
        "sessions_list",
        "session_details",
        "_get_session_liveness_info",
        "_get_session_property",
        "_get_session_programming_language",
        "_get_session_versions",
    ],
    "table.py": [
        "_build_table_data_response",
        "session_tables_schema",
        "session_tables_list",
        "session_table_data",
    ],
    "script.py": [
        "session_script_run",
        "session_pip_list",
    ],
    "catalog.py": [
        "_get_catalog_data",
        "catalog_tables_list",
        "catalog_namespaces_list",
        "catalog_tables_schema",
        "catalog_table_sample",
    ],
    "pq.py": [
        "_parse_pq_id",
        "_make_pq_id",
        "_validate_timeout",
        "_validate_max_concurrent",
        "_format_pq_config",
        "_format_named_string_list",
        "_format_column_definition",
        "_format_table_definition",
        "_format_exported_object_info",
        "_format_worker_protocol",
        "_format_connection_details",
        "_format_exception_details",
        "_format_pq_state",
        "_format_pq_replicas",
        "_format_pq_spares",
        "_normalize_programming_language",
        "_setup_batch_pq_operation",
        "_validate_and_parse_pq_ids",
        "_convert_restart_users_to_enum",
        "_add_session_id_if_running",
        "pq_name_to_id",
        "pq_list",
        "pq_details",
        "pq_create",
        "_apply_pq_config_simple_fields",
        "_apply_pq_config_list_fields",
        "_apply_pq_config_modifications",
        "pq_modify",
        "pq_delete",
        "pq_start",
        "pq_stop",
        "pq_restart",
    ],
    "session_community.py": [
        "_get_session_creation_config",
        "_check_session_limit",
        "_validate_launch_method_params",
        "_resolve_docker_image",
        "_resolve_community_session_parameters",
        "_normalize_auth_type",
        "_resolve_auth_token",
        "_register_session_manager",
        "_launch_process_and_wait_for_ready",
        "_build_success_response",
        "_log_auto_generated_credentials",
        "session_community_create",
        "session_community_delete",
        "session_community_credentials",
    ],
}

# Constants distribution (will look for these in original file)
CONSTANT_DISTRIBUTION = {
    "shared.py": ["MAX_RESPONSE_SIZE", "WARNING_SIZE"],
    "session.py": ["DEFAULT_PROGRAMMING_LANGUAGE"],
    "table.py": ["ESTIMATED_BYTES_PER_CELL"],
    "session_enterprise.py": [
        "DEFAULT_MAX_CONCURRENT_SESSIONS",
        "DEFAULT_ENGINE",
        "DEFAULT_TIMEOUT_SECONDS",
    ],
    "pq.py": ["DEFAULT_PQ_TIMEOUT", "DEFAULT_MAX_CONCURRENT"],
    "session_community.py": [
        "DEFAULT_LAUNCH_METHOD",
        "DEFAULT_AUTH_TYPE",
        "DEFAULT_DOCKER_IMAGE_PYTHON",
        "DEFAULT_DOCKER_IMAGE_GROOVY",
        "DEFAULT_HEAP_SIZE_GB",
        "DEFAULT_STARTUP_TIMEOUT_SECONDS",
        "DEFAULT_STARTUP_CHECK_INTERVAL_SECONDS",
        "DEFAULT_STARTUP_RETRIES",
    ],
}


def find_function_boundaries(lines):
    """Find start and end line numbers for all functions, INCLUDING decorators."""
    boundaries = {}
    i = 0
    
    while i < len(lines):
        line = lines[i]
        
        # Look for function definition at column 0
        match = re.match(r'^(async )?def ([a-zA-Z_][a-zA-Z0-9_]*)\(', line)
        if match:
            func_name = match.group(2)
            
            # CRITICAL FIX: Scan backwards to find decorators
            # Decorators are lines starting with @ at column 0
            start = i
            while start > 0:
                prev_line = lines[start - 1].rstrip()
                # Check if previous line is a decorator or blank
                if not prev_line:  # blank line
                    start -= 1
                    continue
                elif prev_line.startswith('@'):  # decorator
                    start -= 1
                    continue
                else:
                    # Hit something that's not a decorator or blank - stop
                    break
            
            # Find end of function signature
            i += 1
            while i < len(lines):
                if ')' in lines[i] and ':' in lines[i]:
                    i += 1
                    break
                i += 1
            
            # Skip blank lines after signature
            while i < len(lines) and not lines[i].strip():
                i += 1
            
            if i < len(lines):
                # Get the base indentation from the first line of function body
                base_indent = len(lines[i]) - len(lines[i].lstrip())
                
                # Scan forward to find end of function
                while i < len(lines):
                    curr_line = lines[i]
                    
                    # Empty lines are part of the function
                    if not curr_line.strip():
                        i += 1
                        continue
                    
                    # If we hit column 0, function has ended
                    curr_indent = len(curr_line) - len(curr_line.lstrip())
                    if curr_indent == 0:
                        break
                    
                    i += 1
                
                # Include trailing blank lines
                end = i
                while end < len(lines) and not lines[end].strip():
                    end += 1
                
                boundaries[func_name] = (start, end)
                continue
        
        i += 1
    
    return boundaries



def find_constant_boundaries(lines):
    """Find start and end line numbers for module-level constants."""
    boundaries = {}
    i = 0
    
    while i < len(lines):
        line = lines[i]
        
        # Look for constant (ALLCAPS = value)
        match = re.match(r'^([A-Z][A-Z0-9_]*)\s*=', line)
        if match:
            const_name = match.group(1)
            start = i
            end = i + 1
            
            # Handle multi-line assignments (e.g., with parentheses)
            # Keep scanning if line ends with open paren/bracket and no closing yet
            if '(' in line and ')' not in line:
                # Multi-line constant - scan until we find closing paren
                while end < len(lines):
                    if ')' in lines[end]:
                        end += 1
                        break
                    end += 1
            
            # Check for docstring immediately after the value
            if end < len(lines) and lines[end].strip().startswith('"""'):
                # Skip through docstring
                if lines[end].count('"""') == 1:  # Multi-line docstring
                    end += 1
                    while end < len(lines) and '"""' not in lines[end]:
                        end += 1
                    if end < len(lines):
                        end += 1
                else:  # Single-line docstring
                    end += 1
            
            # Include trailing blank line if present
            if end < len(lines) and not lines[end].strip():
                end += 1
            
            boundaries[const_name] = (start, end)
            i = end
            continue
        
        i += 1
    
    return boundaries


def extract_header_and_imports(lines):
    """Extract imports and setup, skipping the original module docstring."""
    # Skip the original module docstring (lines 1-58 based on structure)
    start = 0
    
    # Find end of module docstring
    if lines[0].strip().startswith('"""'):
        # Multi-line docstring
        for i in range(1, len(lines)):
            if '"""' in lines[i]:
                start = i + 1
                break
    
    # Skip blank lines after docstring
    while start < len(lines) and not lines[start].strip():
        start += 1
    
    # Find where functions/constants start
    first_item = len(lines)
    for i in range(start, len(lines)):
        line = lines[i]
        if re.match(r'^(async )?def |^class |^[A-Z][A-Z0-9_]*\s*=', line):
            first_item = i
            break
    
    return '\n'.join(lines[start:first_item])


def main():
    """Main refactoring execution."""
    print("=" * 80)
    print("REFACTORING _mcp.py INTO MODULES")
    print("=" * 80)
    
    # Paths
    repo_root = Path(__file__).parent.parent
    source_file = repo_root / "src/deephaven_mcp/mcp_systems_server/_mcp.py.original"
    output_dir = repo_root / "src/deephaven_mcp/mcp_systems_server/_tools"
    
    # Read source
    print(f"\n1. Reading source file: {source_file}")
    with open(source_file) as f:
        content = f.read()
    
    lines = content.splitlines()
    print(f"   - Source file: {len(content)} bytes, {len(lines)} lines")
    
    # Find boundaries
    print("\n2. Finding function and constant boundaries...")
    func_boundaries = find_function_boundaries(lines)
    const_boundaries = find_constant_boundaries(lines)
    
    print(f"   - Found {len(func_boundaries)} functions")
    print(f"   - Found {len(const_boundaries)} constants")
    
    # Extract header/imports
    header = extract_header_and_imports(lines)
    
    # Create output directory
    print(f"\n3. Creating output directory: {output_dir}")
    output_dir.mkdir(exist_ok=True)
    
    # Create __init__.py
    (output_dir / "__init__.py").write_text('"""MCP Systems Server Tools."""\n')
    
    # Write each module
    print("\n4. Writing module files...")
    for module_name, func_names in MODULE_STRUCTURE.items():
        print(f"   - Writing {module_name}...")
        
        with open(output_dir / module_name, 'w') as f:
            # Placeholder docstring
            f.write(f'"""TODO: Add comprehensive module docstring for {module_name}"""\n\n')
            
            # Write standardized imports
            f.write(MODULE_IMPORTS)
            f.write('\n\n')
            
            # Write constants for this module
            const_count = 0
            if module_name in CONSTANT_DISTRIBUTION:
                for const_name in CONSTANT_DISTRIBUTION[module_name]:
                    if const_name in const_boundaries:
                        start, end = const_boundaries[const_name]
                        const_text = '\n'.join(lines[start:end])
                        f.write(const_text)
                        f.write('\n\n')
                        const_count += 1
            
            # Write functions for this module
            func_count = 0
            for func_name in func_names:
                if func_name in func_boundaries:
                    start, end = func_boundaries[func_name]
                    func_text = '\n'.join(lines[start:end])
                    f.write(func_text)
                    f.write('\n\n')
                    func_count += 1
                else:
                    print(f"     WARNING: Function {func_name} not found!")
        
        print(f"     → {func_count} functions, {const_count} constants")
    
    # Create stub
    print("\n5. Creating backward compatibility stub...")
    stub_path = repo_root / "src/deephaven_mcp/mcp_systems_server/_mcp.py.new"
    
    with open(stub_path, 'w') as f:
        f.write('"""\nBackward compatibility stub for _mcp.py.\n\n')
        f.write('All functions have been moved to _tools/* modules.\n')
        f.write('This stub imports and re-exports them for backward compatibility.\n')
        f.write('"""\n\n')
        
        # Import from each module
        for module_name, func_names in MODULE_STRUCTURE.items():
            f.write(f'from deephaven_mcp.mcp_systems_server._tools.{module_name[:-3]} import (\n')
            for func_name in func_names:
                if func_name in func_boundaries:
                    f.write(f'    {func_name},\n')
            f.write(')\n\n')
        
        # Export all
        f.write('__all__ = [\n')
        for func_names in MODULE_STRUCTURE.values():
            for func_name in func_names:
                if func_name in func_boundaries:
                    f.write(f'    "{func_name}",\n')
        f.write(']\n')
    
    print(f"     → Created {stub_path}")
    print(f"     → Review and rename to _mcp.py after validation")
    
    print("\n" + "=" * 80)
    print("REFACTORING COMPLETE")
    print("=" * 80)
    print(f"\nGenerated files in: {output_dir}")
    print("\nNext steps:")
    print("1. Add comprehensive file-level pydocs to each module")
    print("2. Run validation program to verify byte-for-byte identity")
    print("3. Run unit tests to ensure everything works")


if __name__ == "__main__":
    main()
