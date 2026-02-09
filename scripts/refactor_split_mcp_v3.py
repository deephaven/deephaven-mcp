#!/usr/bin/env python3
"""
Refactoring script v3 - Order-preserving extraction with validation.

Critical fix: Preserves original line number order within each module.
This ensures that if app_lifespan is defined before mcp_server in the original,
it stays that way in the extracted module.
"""

import re
from pathlib import Path
from typing import Dict, List, Tuple


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

CONSTANT_DISTRIBUTION = {
    "shared.py": ["MAX_RESPONSE_SIZE", "WARNING_SIZE"],
    "session.py": ["DEFAULT_PROGRAMMING_LANGUAGE"],
    "table.py": ["ESTIMATED_BYTES_PER_CELL"],
    "session_enterprise.py": [
        "DEFAULT_MAX_CONCURRENT_SESSIONS",
        "DEFAULT_ENGINE",
        "DEFAULT_TIMEOUT_SECONDS",
    ],
    "pq.py": ["DEFAULT_PQ_TIMEOUT", "DEFAULT_MAX_CONCURRENT", "MAX_MCP_SAFE_TIMEOUT"],
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

OBJECT_DISTRIBUTION = {
    "mcp_server.py": ["mcp_server"],
}


def find_function_boundaries(lines):
    """Find functions WITH decorators."""
    boundaries = {}
    i = 0
    
    while i < len(lines):
        line = lines[i]
        match = re.match(r'^(async )?def ([a-zA-Z_][a-zA-Z0-9_]*)\(', line)
        if match:
            func_name = match.group(2)
            start = i
            
            # Scan backwards for decorators/comments
            while start > 0:
                prev_line = lines[start - 1].rstrip()
                if not prev_line or prev_line.startswith('@') or prev_line.startswith('#'):
                    start -= 1
                else:
                    break
            
            # Find end of signature
            i += 1
            while i < len(lines):
                if ')' in lines[i] and ':' in lines[i]:
                    i += 1
                    break
                i += 1
            
            # Skip blanks after signature
            while i < len(lines) and not lines[i].strip():
                i += 1
            
            if i < len(lines):
                # Scan to end of body
                while i < len(lines):
                    curr_line = lines[i]
                    if not curr_line.strip():
                        i += 1
                        continue
                    curr_indent = len(curr_line) - len(curr_line.lstrip())
                    if curr_indent == 0:
                        break
                    i += 1
                
                # Include up to 2 trailing blanks
                end = i
                blank_count = 0
                while end < len(lines) and not lines[end].strip() and blank_count < 2:
                    end += 1
                    blank_count += 1
                
                boundaries[func_name] = (start, end)
                continue
        
        i += 1
    
    return boundaries


def find_constant_boundaries(lines):
    """Find ALLCAPS constants WITH preceding comments and following docstrings."""
    boundaries = {}
    i = 0
    
    while i < len(lines):
        line = lines[i]
        match = re.match(r'^([A-Z][A-Z0-9_]*)\s*=', line)
        if match:
            const_name = match.group(1)
            
            # Skip T = TypeVar (already in imports)
            if const_name == 'T':
                i += 1
                continue
            
            # Scan backwards for comments
            start = i
            while start > 0:
                prev_line = lines[start - 1].rstrip()
                if not prev_line or prev_line.startswith('#'):
                    start -= 1
                else:
                    break
            
            # Find end of assignment
            end = i + 1
            if '(' in line and ')' not in line:
                while end < len(lines):
                    if ')' in lines[end]:
                        end += 1
                        break
                    end += 1
            
            # Check for docstring after
            if end < len(lines) and lines[end].strip().startswith('"""'):
                if lines[end].count('"""') == 1:
                    end += 1
                    while end < len(lines) and '"""' not in lines[end]:
                        end += 1
                    if end < len(lines):
                        end += 1
                else:
                    end += 1
            
            # Include ONE trailing blank
            if end < len(lines) and not lines[end].strip():
                end += 1
            
            boundaries[const_name] = (start, end)
            i = end
            continue
        
        i += 1
    
    return boundaries


def find_object_boundaries(lines):
    """Find lowercase module-level objects."""
    boundaries = {}
    i = 0
    
    while i < len(lines):
        line = lines[i]
        match = re.match(r'^([a-z_][a-z0-9_]*)\s*=', line)
        if match:
            obj_name = match.group(1)
            start = i
            end = i + 1
            
            # Check for docstring
            if end < len(lines) and lines[end].strip().startswith('"""'):
                if lines[end].count('"""') == 1:
                    end += 1
                    while end < len(lines) and '"""' not in lines[end]:
                        end += 1
                    if end < len(lines):
                        end += 1
                else:
                    end += 1
            
            # Include ONE trailing blank
            if end < len(lines) and not lines[end].strip():
                end += 1
            
            boundaries[obj_name] = (start, end)
            i = end
            continue
        
        i += 1
    
    return boundaries


def main():
    """Main refactoring with order preservation."""
    print("=" * 80)
    print("REFACTORING _mcp.py (v3 - order preserving)")
    print("=" * 80)
    
    repo_root = Path(__file__).parent.parent
    source_file = repo_root / "src/deephaven_mcp/mcp_systems_server/_mcp.py.original"
    output_dir = repo_root / "src/deephaven_mcp/mcp_systems_server/_tools"
    
    print(f"\n1. Reading source file: {source_file}")
    with open(source_file) as f:
        content = f.read()
    
    lines = content.splitlines()
    print(f"   - Source file: {len(content)} bytes, {len(lines)} lines")
    
    print("\n2. Finding all code items...")
    func_boundaries = find_function_boundaries(lines)
    const_boundaries = find_constant_boundaries(lines)
    obj_boundaries = find_object_boundaries(lines)
    
    print(f"   - Found {len(func_boundaries)} functions")
    print(f"   - Found {len(const_boundaries)} constants (excluding T)")
    print(f"   - Found {len(obj_boundaries)} objects")
    
    # Validation
    print("\n3. Validating...")
    expected_funcs = set()
    for func_list in MODULE_STRUCTURE.values():
        expected_funcs.update(func_list)
    
    found_funcs = set(func_boundaries.keys())
    missing_funcs = expected_funcs - found_funcs
    
    expected_consts = set()
    for const_list in CONSTANT_DISTRIBUTION.values():
        expected_consts.update(const_list)
    
    found_consts = set(const_boundaries.keys())
    missing_consts = expected_consts - found_consts
    
    expected_objs = set()
    for obj_list in OBJECT_DISTRIBUTION.values():
        expected_objs.update(obj_list)
    
    found_objs = set(obj_boundaries.keys())
    missing_objs = expected_objs - found_objs
    
    validation_passed = True
    
    if missing_funcs:
        print(f"   ❌ MISSING FUNCTIONS: {sorted(missing_funcs)}")
        validation_passed = False
    
    if missing_consts:
        print(f"   ❌ MISSING CONSTANTS: {sorted(missing_consts)}")
        validation_passed = False
    
    if missing_objs:
        print(f"   ❌ MISSING OBJECTS: {sorted(missing_objs)}")
        validation_passed = False
    
    if not validation_passed:
        return 1
    
    print("   ✅ All expected items found")
    
    # Build module -> items map with line numbers for sorting
    print("\n4. Organizing items by module (preserving order)...")
    module_items = {}
    
    for module_name in MODULE_STRUCTURE.keys():
        items = []
        
        # Add functions
        for func_name in MODULE_STRUCTURE[module_name]:
            start, end = func_boundaries[func_name]
            items.append(('function', func_name, start, end))
        
        # Add constants
        if module_name in CONSTANT_DISTRIBUTION:
            for const_name in CONSTANT_DISTRIBUTION[module_name]:
                start, end = const_boundaries[const_name]
                items.append(('constant', const_name, start, end))
        
        # Add objects
        if module_name in OBJECT_DISTRIBUTION:
            for obj_name in OBJECT_DISTRIBUTION[module_name]:
                start, end = obj_boundaries[obj_name]
                items.append(('object', obj_name, start, end))
        
        # CRITICAL: Sort by original line number to preserve order
        items.sort(key=lambda x: x[2])
        module_items[module_name] = items
    
    # Write modules
    print("\n5. Writing modules...")
    output_dir.mkdir(exist_ok=True)
    (output_dir / "__init__.py").write_text('"""MCP Systems Server Tools."""\n')
    
    for module_name, items in module_items.items():
        print(f"   - Writing {module_name}...")
        
        with open(output_dir / module_name, 'w') as f:
            f.write(f'"""TODO: Add comprehensive module docstring for {module_name}"""\n\n')
            f.write(MODULE_IMPORTS)
            f.write('\n\n')
            
            # Write items in original order
            for item_type, item_name, start, end in items:
                item_text = '\n'.join(lines[start:end])
                f.write(item_text)
                f.write('\n')  # Single blank between items
        
        print(f"     → {len(items)} items in original order")
    
    # Create stub
    print("\n6. Creating stub...")
    stub_path = repo_root / "src/deephaven_mcp/mcp_systems_server/_mcp.py.new"
    
    with open(stub_path, 'w') as f:
        f.write('"""\nBackward compatibility stub.\n"""\n\n')
        
        for module_name, items in module_items.items():
            import_names = [item[1] for item in items]
            f.write(f'from deephaven_mcp.mcp_systems_server._tools.{module_name[:-3]} import (\n')
            for name in import_names:
                f.write(f'    {name},\n')
            f.write(')\n\n')
        
        f.write('__all__ = [\n')
        for items in module_items.values():
            for _, item_name, _, _ in items:
                f.write(f'    "{item_name}",\n')
        f.write(']\n')
    
    print(f"     → Created {stub_path}")
    
    print("\n" + "=" * 80)
    print("✅ REFACTORING COMPLETE - ORDER PRESERVED")
    print("=" * 80)
    print(f"Extracted {len(expected_funcs)} functions, {len(expected_consts)} constants, {len(expected_objs)} objects")
    
    return 0


if __name__ == "__main__":
    exit(main())
