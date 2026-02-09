#!/usr/bin/env python3
"""
Automated test refactoring for modularized _mcp.py.

Updates test__mcp.py to import from the correct tool modules and fixes all patch targets.
"""

import re
from pathlib import Path

# Map functions/constants to their new module locations
FUNCTION_MODULE_MAP = {
    # shared.py functions
    "_get_session_from_context": "shared",
    "_get_enterprise_session": "shared",
    "_check_response_size": "shared",
    "_format_meta_table_result": "shared",
    "_get_system_config": "shared",
    
    # mcp_server.py
    "app_lifespan": "mcp_server",
    "mcp_reload": "mcp_server",
    "mcp_server": "mcp_server",
    
    # session_enterprise.py
    "enterprise_systems_status": "session_enterprise",
    "_check_session_limits": "session_enterprise",
    "_generate_session_name_if_none": "session_enterprise",
    "_check_session_id_available": "session_enterprise",
    "_resolve_session_parameters": "session_enterprise",
    "session_enterprise_create": "session_enterprise",
    "session_enterprise_delete": "session_enterprise",
    "DEFAULT_MAX_CONCURRENT_SESSIONS": "session_enterprise",
    "DEFAULT_ENGINE": "session_enterprise",
    "DEFAULT_TIMEOUT_SECONDS": "session_enterprise",
    
    # session.py
    "session_details": "session",
    "session_list": "session",
    "refresh": "session",
    "_get_version_info": "session",
    "_format_session_info": "session",
    "_normalize_programming_language": "session",
    "DEFAULT_PROGRAMMING_LANGUAGE": "session",
    
    # table.py
    "session_tables_schema": "table",
    "session_tables_list": "table",
    "session_table_data": "table",
    "_build_table_data_response": "table",
    "ESTIMATED_BYTES_PER_CELL": "table",
    
    # script.py
    "session_script_run": "script",
    "session_pip_list": "script",
    
    # catalog.py
    "catalog_tables": "catalog",
    "catalog_namespaces": "catalog",
    "catalog_tables_schema": "catalog",
    "catalog_table_sample": "catalog",
    "_get_catalog_data": "catalog",
    
    # pq.py - all PQ functions
    "pq_name_to_id": "pq",
    "pq_list": "pq",
    "pq_details": "pq",
    "pq_create": "pq",
    "pq_modify": "pq",
    "pq_delete": "pq",
    "pq_start": "pq",
    "pq_stop": "pq",
    "pq_restart": "pq",
    "pq_cancel": "pq",
    "_parse_pq_id": "pq",
    "_validate_timeout": "pq",
    "_format_pq_config": "pq",
    "_format_pq_state": "pq",
    "_format_exported_object_info": "pq",
    "DEFAULT_PQ_TIMEOUT": "pq",
    "DEFAULT_MAX_CONCURRENT": "pq",
    "MAX_MCP_SAFE_TIMEOUT": "pq",
    
    # session_community.py
    "session_community_create": "session_community",
    "session_community_delete": "session_community",
    "_normalize_auth_type": "session_community",
    "_resolve_community_session_parameters": "session_community",
    "DEFAULT_LAUNCH_METHOD": "session_community",
    "DEFAULT_AUTH_TYPE": "session_community",
    "DEFAULT_DOCKER_IMAGE_PYTHON": "session_community",
    "DEFAULT_DOCKER_IMAGE_GROOVY": "session_community",
    "DEFAULT_HEAP_SIZE_GB": "session_community",
    "DEFAULT_STARTUP_TIMEOUT_SECONDS": "session_community",
    "DEFAULT_STARTUP_CHECK_INTERVAL_SECONDS": "session_community",
    "DEFAULT_STARTUP_RETRIES": "session_community",
}


def build_import_groups(functions_to_import):
    """Group functions by their target module."""
    module_imports = {}
    for func in functions_to_import:
        module = FUNCTION_MODULE_MAP.get(func)
        if module:
            if module not in module_imports:
                module_imports[module] = []
            module_imports[module].append(func)
        else:
            print(f"Warning: No module mapping for {func}")
    return module_imports


def generate_new_imports(module_imports):
    """Generate new import statements."""
    import_lines = []
    
    # Sort modules for consistency
    for module in sorted(module_imports.keys()):
        funcs = sorted(module_imports[module])
        import_path = f"deephaven_mcp.mcp_systems_server._tools.{module}"
        
        if len(funcs) == 1:
            import_lines.append(f"from {import_path} import {funcs[0]}")
        else:
            import_lines.append(f"from {import_path} import (")
            for i, func in enumerate(funcs):
                comma = "," if i < len(funcs) - 1 else ""
                import_lines.append(f"    {func}{comma}")
            import_lines.append(")")
    
    return "\n".join(import_lines)


def detect_test_module(test_name):
    """Detect which module a test is testing based on its name."""
    # Map test name patterns to modules
    if 'catalog' in test_name:
        return 'catalog'
    elif 'pq_' in test_name or '_pq_' in test_name or 'format_pq' in test_name or 'format_exported_object' in test_name:
        return 'pq'
    elif 'session_enterprise' in test_name or 'enterprise' in test_name:
        return 'session_enterprise'
    elif 'session_community' in test_name or 'community' in test_name:
        return 'session_community'
    elif 'session_table' in test_name or 'table_data' in test_name or 'build_table_data' in test_name:
        return 'table'
    elif 'session_script' in test_name or 'pip_list' in test_name:
        return 'script'
    elif 'check_response_size' in test_name or 'get_session_from_context' in test_name or 'get_enterprise_session' in test_name or 'format_meta_table' in test_name or 'get_system_config' in test_name:
        return 'shared'
    elif 'app_lifespan' in test_name or 'mcp_reload' in test_name:
        return 'mcp_server'
    elif 'session' in test_name:
        return 'session'
    else:
        # Default to session for ambiguous cases
        return 'session'


def update_context_aware_logger_patches(content):
    """Update logger patches based on which test/function they're in."""
    lines = content.split('\n')
    current_test = None
    result_lines = []
    
    for line in lines:
        # Track which test we're in
        if line.strip().startswith('def test_') or line.strip().startswith('async def test_'):
            # Extract test name
            match = re.search(r'def (test_[a-zA-Z0-9_]+)', line)
            if match:
                current_test = match.group(1)
        
        # Update logger patches based on current test context
        if current_test and '"deephaven_mcp.mcp_systems_server._tools.session._LOGGER' in line:
            # Detect which module this test is actually testing
            test_module = detect_test_module(current_test)
            if test_module != 'session':
                # Replace with correct module's logger
                line = line.replace(
                    '"deephaven_mcp.mcp_systems_server._tools.session._LOGGER',
                    f'"deephaven_mcp.mcp_systems_server._tools.{test_module}._LOGGER'
                )
        
        result_lines.append(line)
    
    return '\n'.join(result_lines)


def update_patch_targets(content):
    """Update all patch target strings in the test file."""
    
    # Pattern to match patch decorators and calls
    # @patch("deephaven_mcp.mcp_systems_server._mcp.SOMETHING")
    # patch("deephaven_mcp.mcp_systems_server._mcp.SOMETHING", ...)
    
    for func_name, module in FUNCTION_MODULE_MAP.items():
        # Update patch decorators
        old_target = f'"deephaven_mcp.mcp_systems_server._mcp.{func_name}"'
        new_target = f'"deephaven_mcp.mcp_systems_server._tools.{module}.{func_name}"'
        content = content.replace(old_target, new_target)
        
        # Also handle single quotes
        old_target_single = f"'deephaven_mcp.mcp_systems_server._mcp.{func_name}'"
        new_target_single = f"'deephaven_mcp.mcp_systems_server._tools.{module}.{func_name}'"
        content = content.replace(old_target_single, new_target_single)
    
    # Also update special module-level patches (queries, _LOGGER, etc.)
    # Map commonly patched imports to their correct modules
    import_patches = {
        # Resource manager classes - patch at actual location
        '"deephaven_mcp.mcp_systems_server._mcp.ConfigManager"': '"deephaven_mcp.config.ConfigManager"',
        '"deephaven_mcp.mcp_systems_server._mcp.CombinedSessionRegistry"': '"deephaven_mcp.resource_manager.CombinedSessionRegistry"',
        '"deephaven_mcp.mcp_systems_server._mcp.DynamicCommunitySessionManager"': '"deephaven_mcp.resource_manager.DynamicCommunitySessionManager"',
        
        # Utility functions - patch at actual location
        '"deephaven_mcp.mcp_systems_server._mcp.find_available_port"': '"deephaven_mcp.resource_manager.find_available_port"',
        '"deephaven_mcp.mcp_systems_server._mcp.generate_auth_token"': '"deephaven_mcp.resource_manager.generate_auth_token"',
        '"deephaven_mcp.mcp_systems_server._mcp.launch_session"': '"deephaven_mcp.resource_manager.launch_session"',
        '"deephaven_mcp.mcp_systems_server._mcp.get_config_section"': '"deephaven_mcp.config.get_config_section"',
        '"deephaven_mcp.mcp_systems_server._mcp.format_table_data"': '"deephaven_mcp.formatters.format_table_data"',
        
        # Proto enums - patch at actual location
        '"deephaven_mcp.mcp_systems_server._mcp.ExportedObjectTypeEnum"': '"deephaven_mcp.client._protobuf.ExportedObjectTypeEnum"',
        '"deephaven_mcp.mcp_systems_server._mcp.RestartUsersEnum"': '"deephaven_mcp.client._protobuf.RestartUsersEnum"',
        
        # Common module patches - queries is used in session.py, others vary
        '"deephaven_mcp.mcp_systems_server._mcp.queries"': '"deephaven_mcp.queries"',
        '"deephaven_mcp.mcp_systems_server._mcp.queries.get_catalog_meta_table"': '"deephaven_mcp.queries.get_catalog_meta_table"',
        '"deephaven_mcp.mcp_systems_server._mcp.queries.get_catalog_table"': '"deephaven_mcp.queries.get_catalog_table"',
        '"deephaven_mcp.mcp_systems_server._mcp.queries.get_catalog_table_data"': '"deephaven_mcp.queries.get_catalog_table_data"',
        '"deephaven_mcp.mcp_systems_server._mcp.queries.get_table"': '"deephaven_mcp.queries.get_table"',
        
        # Loggers - assume session._LOGGER for now (most common)
        '"deephaven_mcp.mcp_systems_server._mcp._LOGGER"': '"deephaven_mcp.mcp_systems_server._tools.session._LOGGER"',
        '"deephaven_mcp.mcp_systems_server._mcp._LOGGER.info"': '"deephaven_mcp.mcp_systems_server._tools.session._LOGGER.info"',
        
        # Other utilities
        '"deephaven_mcp.mcp_systems_server._mcp.datetime"': '"datetime"',
        '"deephaven_mcp.mcp_systems_server._mcp.asyncio.Lock"': '"asyncio.Lock"',
    }
    
    for old_patch, new_patch in import_patches.items():
        content = content.replace(old_patch, new_patch)
    
    return content


def update_inline_imports(content):
    """Update inline imports inside test functions."""
    # Replace inline imports from _mcp with the correct module
    # Pattern: from deephaven_mcp.mcp_systems_server._mcp import function_name
    
    for func_name, module in FUNCTION_MODULE_MAP.items():
        # Match indented imports (inside functions)
        old_import = f'from deephaven_mcp.mcp_systems_server._mcp import {func_name}'
        new_import = f'from deephaven_mcp.mcp_systems_server._tools.{module} import {func_name}'
        content = content.replace(old_import, new_import)
    
    return content


def main():
    """Main refactoring execution."""
    print("=" * 80)
    print("REFACTORING TEST FILE")
    print("=" * 80)
    
    test_file = Path("tests/mcp_systems_server/test__mcp.py")
    
    if not test_file.exists():
        print(f"Error: {test_file} not found")
        return
    
    # Read the test file
    with open(test_file) as f:
        content = f.read()
    
    print("\n1. Extracting current imports from _mcp...")
    
    # Find all imports from _mcp
    import_pattern = r'from deephaven_mcp\.mcp_systems_server\._mcp import \((.*?)\)'
    matches = re.findall(import_pattern, content, re.DOTALL)
    
    all_imports = set()
    for match in matches:
        # Split by comma and clean up
        items = [item.strip() for item in match.split(',')]
        all_imports.update([item for item in items if item])
    
    # Also find single-line imports
    single_import_pattern = r'from deephaven_mcp\.mcp_systems_server\._mcp import ([a-zA-Z_][a-zA-Z0-9_]*)'
    single_matches = re.findall(single_import_pattern, content)
    all_imports.update(single_matches)
    
    print(f"   Found {len(all_imports)} unique imports")
    
    print("\n2. Building new import structure...")
    module_imports = build_import_groups(all_imports)
    new_imports = generate_new_imports(module_imports)
    
    print(f"   Grouped into {len(module_imports)} modules")
    
    print("\n3. Replacing old imports with new structure...")
    
    # Only remove module-level imports (not indented ones inside functions)
    # Split into lines to process line by line
    lines = content.split('\n')
    new_lines = []
    in_import_block = False
    
    for i, line in enumerate(lines):
        # Check if this is a module-level import from _mcp (no leading whitespace)
        if line.startswith('from deephaven_mcp.mcp_systems_server._mcp import'):
            # Check if it's a multi-line import
            if '(' in line and ')' not in line:
                in_import_block = True
                continue  # Skip this line
            elif '(' in line and ')' in line:
                # Single line with parens - skip it
                continue
            else:
                # Single line import - skip it
                continue
        elif in_import_block:
            # We're inside a multi-line import block
            if ')' in line:
                in_import_block = False
            continue  # Skip all lines in the import block
        else:
            # Keep all other lines (including indented imports inside functions)
            new_lines.append(line)
    
    content = '\n'.join(new_lines)
    
    # Insert new imports after the other imports (after "import deephaven_mcp.mcp_systems_server._mcp as mcp_mod")
    insertion_point = 'import deephaven_mcp.mcp_systems_server._mcp as mcp_mod'
    if insertion_point in content:
        content = content.replace(
            insertion_point,
            insertion_point + '\n' + new_imports
        )
    else:
        print("   Warning: Could not find insertion point for imports")
    
    print("\n4. Updating inline imports in test functions...")
    content = update_inline_imports(content)
    
    print("\n5. Updating patch targets...")
    content = update_patch_targets(content)
    
    print("\n6. Fixing context-aware logger patches...")
    content = update_context_aware_logger_patches(content)
    
    print("\n7. Writing updated test file...")
    backup_file = test_file.with_suffix('.py.phase1')
    
    # Backup original
    with open(backup_file, 'w') as f:
        with open(test_file) as orig:
            f.write(orig.read())
    print(f"   Backed up original to {backup_file}")
    
    # Write updated version
    with open(test_file, 'w') as f:
        f.write(content)
    
    print("\n" + "=" * 80)
    print("TEST REFACTORING COMPLETE")
    print("=" * 80)
    print(f"\nUpdated: {test_file}")
    print(f"Backup: {backup_file}")
    print("\nNext step: Run tests to verify")


if __name__ == "__main__":
    main()
