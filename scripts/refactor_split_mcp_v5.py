#!/usr/bin/env python3
"""
Refactoring script v5 - Hard-coded correct dependencies.

Based on analysis of ruff F821 errors, this version uses a hard-coded
MODULE_DEPENDENCIES dict to specify exactly which modules need what from
where, avoiding circular imports and naive pattern matching.
"""

import re
import subprocess
from pathlib import Path
from typing import Dict, List, Set, Tuple


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
    "shared.py": [
        "MAX_RESPONSE_SIZE",
        "WARNING_SIZE",
    ],
    "session.py": [
        "DEFAULT_PROGRAMMING_LANGUAGE",
        "DEFAULT_MAX_CONCURRENT_SESSIONS",
    ],
    "table.py": ["ESTIMATED_BYTES_PER_CELL"],
    "session_enterprise.py": [
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

# Hard-coded correct cross-module dependencies
# Based on analysis of original ruff F821 errors
MODULE_DEPENDENCIES = {
    "shared.py": {},  # No cross-module dependencies
    "mcp_server.py": {},  # Only imports from external packages, not from other _tools modules
    "session_enterprise.py": {
        "mcp_server.py": ["mcp_server"],
        "shared.py": ["_get_system_config"],
    },
    "session.py": {
        "mcp_server.py": ["mcp_server"],
        "shared.py": ["_get_session_from_context"],
    },
    "table.py": {
        "mcp_server.py": ["mcp_server"],
        "shared.py": ["_get_session_from_context", "_check_response_size"],
    },
    "script.py": {
        "mcp_server.py": ["mcp_server"],
        "shared.py": ["_get_session_from_context"],
    },
    "catalog.py": {
        "mcp_server.py": ["mcp_server"],
        "shared.py": [
            "_get_session_from_context",
            "_get_enterprise_session",
            "_check_response_size",
            "_format_meta_table_result",
        ],
        "table.py": ["ESTIMATED_BYTES_PER_CELL", "_build_table_data_response"],
    },
    "pq.py": {
        "mcp_server.py": ["mcp_server"],
        "shared.py": ["_get_system_config"],
    },
    "session_community.py": {
        "mcp_server.py": ["mcp_server"],
        "session.py": ["DEFAULT_PROGRAMMING_LANGUAGE", "DEFAULT_MAX_CONCURRENT_SESSIONS"],
    },
    "session_enterprise.py": {
        "mcp_server.py": ["mcp_server"],
        "shared.py": ["_get_system_config"],
        "session.py": ["DEFAULT_PROGRAMMING_LANGUAGE", "DEFAULT_MAX_CONCURRENT_SESSIONS"],
    },
    "table.py": {
        "mcp_server.py": ["mcp_server"],
        "shared.py": ["_get_session_from_context", "_check_response_size", "_format_meta_table_result"],
    },
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
            
            while start > 0:
                prev_line = lines[start - 1].rstrip()
                if not prev_line or prev_line.startswith('@') or prev_line.startswith('#'):
                    start -= 1
                else:
                    break
            
            i += 1
            while i < len(lines):
                if ')' in lines[i] and ':' in lines[i]:
                    i += 1
                    break
                i += 1
            
            while i < len(lines) and not lines[i].strip():
                i += 1
            
            if i < len(lines):
                while i < len(lines):
                    curr_line = lines[i]
                    if not curr_line.strip():
                        i += 1
                        continue
                    curr_indent = len(curr_line) - len(curr_line.lstrip())
                    if curr_indent == 0:
                        break
                    i += 1
                
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
    """Find ALLCAPS constants (excluding T)."""
    boundaries = {}
    i = 0
    
    while i < len(lines):
        line = lines[i]
        match = re.match(r'^([A-Z][A-Z0-9_]*)\s*=', line)
        if match:
            const_name = match.group(1)
            
            if const_name == 'T':
                i += 1
                continue
            
            start = i
            while start > 0:
                prev_line = lines[start - 1].rstrip()
                if not prev_line or prev_line.startswith('#'):
                    start -= 1
                else:
                    break
            
            end = i + 1
            if '(' in line and ')' not in line:
                while end < len(lines):
                    if ')' in lines[end]:
                        end += 1
                        break
                    end += 1
            
            if end < len(lines) and lines[end].strip().startswith('"""'):
                if lines[end].count('"""') == 1:
                    end += 1
                    while end < len(lines) and '"""' not in lines[end]:
                        end += 1
                    if end < len(lines):
                        end += 1
                else:
                    end += 1
            
            if end < len(lines) and not lines[end].strip():
                end += 1
            
            boundaries[const_name] = (start, end)
            i = end
            continue
        
        i += 1
    
    return boundaries


def find_object_boundaries(lines):
    """Find lowercase module objects (skip _LOGGER - already in MODULE_IMPORTS)."""
    boundaries = {}
    i = 0
    
    while i < len(lines):
        line = lines[i]
        match = re.match(r'^([a-z_][a-z0-9_]*)\s*=', line)
        if match:
            obj_name = match.group(1)
            
            # Skip _LOGGER - it's already in MODULE_IMPORTS
            if obj_name == '_LOGGER':
                i += 1
                continue
            
            start = i
            end = i + 1
            
            if end < len(lines) and lines[end].strip().startswith('"""'):
                if lines[end].count('"""') == 1:
                    end += 1
                    while end < len(lines) and '"""' not in lines[end]:
                        end += 1
                    if end < len(lines):
                        end += 1
                else:
                    end += 1
            
            if end < len(lines) and not lines[end].strip():
                end += 1
            
            boundaries[obj_name] = (start, end)
            i = end
            continue
        
        i += 1
    
    return boundaries


def build_import_section(module_name):
    """Build import statements from hard-coded MODULE_DEPENDENCIES."""
    if module_name not in MODULE_DEPENDENCIES or not MODULE_DEPENDENCIES[module_name]:
        return ""
    
    import_lines = []
    for source_module in sorted(MODULE_DEPENDENCIES[module_name].keys()):
        items = sorted(MODULE_DEPENDENCIES[module_name][source_module])
        module_path = source_module[:-3]  # Remove .py
        import_lines.append(f"from deephaven_mcp.mcp_systems_server._tools.{module_path} import (")
        for item in items:
            import_lines.append(f"    {item},")
        import_lines.append(")")
    
    return '\n'.join(import_lines)


def extract_module_docstring(lines):
    """Extract the original module docstring for use in stub."""
    if not lines[0].strip().startswith('"""'):
        return '"""Module docstring not found."""'
    
    end_line = 0
    for i in range(1, len(lines)):
        if '"""' in lines[i]:
            end_line = i
            break
    
    if end_line == 0:
        return '"""Module docstring not found."""'
    
    return '\n'.join(lines[0:end_line + 1])


def main():
    """Main refactoring with hard-coded dependencies."""
    print("=" * 80)
    print("REFACTORING _mcp.py (v5 - hard-coded dependencies)")
    print("=" * 80)
    
    repo_root = Path(__file__).parent.parent
    source_file = repo_root / "src/deephaven_mcp/mcp_systems_server/_mcp.py.original"
    output_dir = repo_root / "src/deephaven_mcp/mcp_systems_server/_tools"
    
    print(f"\n1. Reading source: {source_file}")
    with open(source_file) as f:
        content = f.read()
    
    lines = content.splitlines()
    print(f"   - {len(lines)} lines")
    
    original_docstring = extract_module_docstring(lines)
    print(f"   - Extracted {len(original_docstring.splitlines())} line module docstring")
    
    print("\n2. Finding code items...")
    func_boundaries = find_function_boundaries(lines)
    const_boundaries = find_constant_boundaries(lines)
    obj_boundaries = find_object_boundaries(lines)
    print(f"   - {len(func_boundaries)} functions")
    print(f"   - {len(const_boundaries)} constants")
    print(f"   - {len(obj_boundaries)} objects")
    
    print("\n3. Validating...")
    expected_funcs = set()
    for func_list in MODULE_STRUCTURE.values():
        expected_funcs.update(func_list)
    
    expected_consts = set()
    for const_list in CONSTANT_DISTRIBUTION.values():
        expected_consts.update(const_list)
    
    expected_objs = set()
    for obj_list in OBJECT_DISTRIBUTION.values():
        expected_objs.update(obj_list)
    
    missing = []
    if expected_funcs - set(func_boundaries.keys()):
        missing.extend(expected_funcs - set(func_boundaries.keys()))
    if expected_consts - set(const_boundaries.keys()):
        missing.extend(expected_consts - set(const_boundaries.keys()))
    if expected_objs - set(obj_boundaries.keys()):
        missing.extend(expected_objs - set(obj_boundaries.keys()))
    
    if missing:
        print(f"   ❌ MISSING: {missing}")
        return 1
    
    print("   ✅ All items found")
    
    print("\n4. Organizing by module (preserving order)...")
    module_items = {}
    for module_name in MODULE_STRUCTURE.keys():
        items = []
        for func_name in MODULE_STRUCTURE[module_name]:
            start, end = func_boundaries[func_name]
            items.append(('function', func_name, start, end))
        
        if module_name in CONSTANT_DISTRIBUTION:
            for const_name in CONSTANT_DISTRIBUTION[module_name]:
                start, end = const_boundaries[const_name]
                items.append(('constant', const_name, start, end))
        
        if module_name in OBJECT_DISTRIBUTION:
            for obj_name in OBJECT_DISTRIBUTION[module_name]:
                start, end = obj_boundaries[obj_name]
                items.append(('object', obj_name, start, end))
        
        items.sort(key=lambda x: x[2])
        module_items[module_name] = items
    
    print("\n5. Writing modules with hard-coded dependencies...")
    output_dir.mkdir(exist_ok=True)
    (output_dir / "__init__.py").write_text('"""MCP Systems Server Tools."""\n')
    
    for module_name, items in module_items.items():
        with open(output_dir / module_name, 'w') as f:
            f.write(f'"""TODO: Add comprehensive module docstring for {module_name}"""\n\n')
            f.write(MODULE_IMPORTS)
            
            # Add hard-coded cross-module imports
            cross_imports = build_import_section(module_name)
            if cross_imports:
                f.write('\n')
                f.write(cross_imports)
            
            f.write('\n\n')
            
            # Add T and _LOGGER after all imports
            f.write('T = TypeVar("T")\n\n')
            f.write('_LOGGER = logging.getLogger(__name__)\n\n')
            
            for item_type, item_name, start, end in items:
                item_text = '\n'.join(lines[start:end])
                f.write(item_text)
                f.write('\n')
        
        deps_count = len(MODULE_DEPENDENCIES.get(module_name, {}))
        print(f"   - {module_name}: {len(items)} items, {deps_count} import groups")
    
    print("\n6. Creating stub with original docstring...")
    stub_path = repo_root / "src/deephaven_mcp/mcp_systems_server/_mcp.py.new"
    with open(stub_path, 'w') as f:
        f.write(original_docstring)
        f.write('\n\n')
        
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
    
    print(f"   - Created {stub_path}")
    
    print("\n7. Validating with ruff...")
    result = subprocess.run(
        ["uv", "run", "ruff", "check", str(output_dir / "*.py"), "--select", "F821"],
        capture_output=True,
        text=True,
        cwd=repo_root
    )
    
    if result.returncode == 0:
        print("   ✅ VALIDATION PASSED - 0 undefined names")
    else:
        print(f"   ❌ VALIDATION FAILED:")
        print(result.stdout)
        return 1
    
    print("\n8. Testing imports...")
    result = subprocess.run(
        ["uv", "run", "python", "-c", 
         "from deephaven_mcp.mcp_systems_server._mcp import mcp_server, sessions_list; print('✓ Imports work')"],
        capture_output=True,
        text=True,
        cwd=repo_root
    )
    
    if result.returncode == 0:
        print(f"   ✅ {result.stdout.strip()}")
    else:
        print(f"   ❌ Import test failed:")
        print(result.stderr)
        return 1
    
    print("\n" + "=" * 80)
    print("✅ REFACTORING COMPLETE AND VALIDATED")
    print("=" * 80)
    return 0


if __name__ == "__main__":
    exit(main())
