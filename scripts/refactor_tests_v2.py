#!/usr/bin/env python3
"""
Comprehensive automated test refactoring v2.

Splits test__mcp.py into module-specific test files and updates all imports/patches.
ZERO manual edits - fully automated.
"""

import re
import subprocess
from pathlib import Path
from collections import defaultdict
from typing import Dict, List, Set, Tuple


# Hard-coded function-to-module mapping (matches v5 refactoring)
FUNCTION_MODULE_MAP = {
    # shared.py
    "_get_session_from_context": "shared",
    "_get_enterprise_session": "shared",
    "_check_response_size": "shared",
    "_format_meta_table_result": "shared",
    "_get_system_config": "shared",
    "MAX_RESPONSE_SIZE": "shared",
    "WARNING_SIZE": "shared",
    
    # mcp_server.py
    "app_lifespan": "mcp_server",
    "mcp_server": "mcp_server",
    "mcp_reload": "mcp_server",
    
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
    "sessions_list": "session",
    "session_details": "session",
    "_get_session_liveness_info": "session",
    "_get_session_property": "session",
    "_get_session_programming_language": "session",
    "_get_session_versions": "session",
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
    "catalog_tables_list": "catalog",
    "catalog_namespaces_list": "catalog",
    "catalog_tables_schema": "catalog",
    "catalog_table_sample": "catalog",
    "_get_catalog_data": "catalog",
    
    # pq.py
    "pq_name_to_id": "pq",
    "pq_list": "pq",
    "pq_details": "pq",
    "pq_create": "pq",
    "pq_modify": "pq",
    "pq_delete": "pq",
    "pq_start": "pq",
    "pq_stop": "pq",
    "pq_restart": "pq",
    "_parse_pq_id": "pq",
    "_make_pq_id": "pq",
    "_validate_timeout": "pq",
    "_validate_max_concurrent": "pq",
    "_format_pq_config": "pq",
    "_format_named_string_list": "pq",
    "_format_column_definition": "pq",
    "_format_table_definition": "pq",
    "_format_exported_object_info": "pq",
    "_format_worker_protocol": "pq",
    "_format_connection_details": "pq",
    "_format_exception_details": "pq",
    "_format_pq_state": "pq",
    "_format_pq_replicas": "pq",
    "_format_pq_spares": "pq",
    "_normalize_programming_language": "pq",
    "_setup_batch_pq_operation": "pq",
    "_validate_and_parse_pq_ids": "pq",
    "_convert_restart_users_to_enum": "pq",
    "_add_session_id_if_running": "pq",
    "_apply_pq_config_simple_fields": "pq",
    "_apply_pq_config_list_fields": "pq",
    "_apply_pq_config_modifications": "pq",
    "DEFAULT_PQ_TIMEOUT": "pq",
    "DEFAULT_MAX_CONCURRENT": "pq",
    "MAX_MCP_SAFE_TIMEOUT": "pq",
    
    # session_community.py
    "session_community_create": "session_community",
    "session_community_delete": "session_community",
    "session_community_credentials": "session_community",
    "_get_session_creation_config": "session_community",
    "_check_session_limit": "session_community",
    "_validate_launch_method_params": "session_community",
    "_resolve_docker_image": "session_community",
    "_resolve_community_session_parameters": "session_community",
    "_normalize_auth_type": "session_community",
    "_resolve_auth_token": "session_community",
    "_register_session_manager": "session_community",
    "_launch_process_and_wait_for_ready": "session_community",
    "_build_success_response": "session_community",
    "_log_auto_generated_credentials": "session_community",
    "DEFAULT_LAUNCH_METHOD": "session_community",
    "DEFAULT_AUTH_TYPE": "session_community",
    "DEFAULT_DOCKER_IMAGE_PYTHON": "session_community",
    "DEFAULT_DOCKER_IMAGE_GROOVY": "session_community",
    "DEFAULT_HEAP_SIZE_GB": "session_community",
    "DEFAULT_STARTUP_TIMEOUT_SECONDS": "session_community",
    "DEFAULT_STARTUP_CHECK_INTERVAL_SECONDS": "session_community",
    "DEFAULT_STARTUP_RETRIES": "session_community",
}


def categorize_test(test_name):
    """Categorize test by which module it belongs to."""
    name_lower = test_name.lower()
    
    # Priority order matching
    if 'catalog' in name_lower:
        return 'catalog'
    elif any(x in name_lower for x in ['pq_', '_pq_', 'parse_pq', 'make_pq', 'validate_timeout', 
                                         'validate_max_concurrent', 'format_pq', 'format_named_string',
                                         'format_column_definition', 'format_table_definition',
                                         'format_exported_object', 'format_worker_protocol',
                                         'format_connection_details', 'format_exception_details',
                                         'format_pq_replicas', 'format_pq_spares', 
                                         'normalize_programming_language_pq', 'setup_batch_pq',
                                         'validate_and_parse_pq', 'convert_restart_users',
                                         'add_session_id_if_running', 'apply_pq_config']):
        return 'pq'
    elif 'session_enterprise' in name_lower or 'enterprise_create' in name_lower or 'enterprise_delete' in name_lower or 'enterprise_systems_status' in name_lower or 'check_session_limits' in name_lower or 'check_session_id_available' in name_lower or 'generate_session_name' in name_lower or 'resolve_session_parameters' in name_lower:
        return 'session_enterprise'
    elif 'session_community' in name_lower or 'community_create' in name_lower or 'community_delete' in name_lower or 'community_credentials' in name_lower or 'normalize_auth_type' in name_lower or 'resolve_community_session' in name_lower or 'validate_launch_method' in name_lower or 'resolve_docker_image' in name_lower or 'resolve_auth_token' in name_lower or 'register_session_manager' in name_lower or 'launch_process_and_wait' in name_lower or 'build_success_response' in name_lower or 'log_auto_generated' in name_lower or 'get_session_creation_config' in name_lower or 'check_session_limit' in name_lower:
        return 'session_community'
    elif 'session_table' in name_lower or 'table_data' in name_lower or 'tables_schema' in name_lower or 'tables_list' in name_lower or 'build_table_data' in name_lower:
        return 'table'
    elif 'session_script' in name_lower or 'script_run' in name_lower or 'pip_list' in name_lower or 'run_script_reads' in name_lower:
        return 'script'
    elif any(x in name_lower for x in ['check_response_size', 'get_session_from_context', 
                                        'get_enterprise_session', 'format_meta_table', 'get_system_config']):
        return 'shared'
    elif 'app_lifespan' in name_lower or 'mcp_reload' in name_lower:
        return 'mcp_server'
    elif 'sessions_list' in name_lower or 'session_details' in name_lower or 'get_session_liveness' in name_lower or 'get_session_property' in name_lower or 'get_session_programming_language' in name_lower or 'get_session_versions' in name_lower:
        return 'session'
    elif 'session' in name_lower:
        return 'session'
    else:
        return 'script'  # Default for unknown (test_run_script_reads_script_from_file)


def extract_sections(content):
    """Extract different sections from test file."""
    lines = content.splitlines()
    
    sections = {
        'header': [],
        'helpers': [],
        'tests': defaultdict(list),
        'classes': defaultdict(list),
    }
    
    i = 0
    
    # Extract header (up to first class or function)
    while i < len(lines):
        line = lines[i]
        if re.match(r'^(class |def |async def )', line):
            break
        sections['header'].append(line)
        i += 1
    
    # Extract classes, helpers, and tests
    while i < len(lines):
        line = lines[i]
        
        # Test class
        if re.match(r'^class Test', line):
            class_match = re.search(r'class (Test[A-Za-z0-9_]+)', line)
            if class_match:
                class_name = class_match.group(1)
                class_lines = [line]
                i += 1
                
                # Find end of class - check for column 0 non-indented line
                while i < len(lines):
                    curr_line = lines[i]
                    # Empty lines are part of the class
                    if not curr_line.strip():
                        class_lines.append(curr_line)
                        i += 1
                        continue
                    # Non-empty line at column 0 means class ended
                    if curr_line and not curr_line[0].isspace():
                        break
                    class_lines.append(curr_line)
                    i += 1
                
                # Categorize by class name
                module = categorize_test(class_name.lower())
                sections['classes'][module].extend(class_lines)
                sections['classes'][module].append('')
                continue
        
        # Mock/helper class
        elif re.match(r'^class Mock|^class [A-Z]', line):
            class_match = re.search(r'class ([A-Za-z0-9_]+)', line)
            if class_match:
                class_lines = [line]
                i += 1
                
                # Find end of class - check for column 0 non-indented line
                while i < len(lines):
                    curr_line = lines[i]
                    # Empty lines are part of the class
                    if not curr_line.strip():
                        class_lines.append(curr_line)
                        i += 1
                        continue
                    # Non-empty line at column 0 means class ended
                    if curr_line and not curr_line[0].isspace():
                        break
                    class_lines.append(curr_line)
                    i += 1
                
                sections['helpers'].extend(class_lines)
                sections['helpers'].append('')
                continue
        
        # Decorator (e.g., @pytest.mark.asyncio)
        elif re.match(r'^@', line):
            decorator_lines = [line]
            i += 1
            
            # Collect all consecutive decorators
            while i < len(lines) and re.match(r'^@', lines[i]):
                decorator_lines.append(lines[i])
                i += 1
            
            # Next line should be the function definition
            if i < len(lines):
                next_line = lines[i]
                
                # Test function with decorators
                if re.match(r'^(async )?def test_', next_line):
                    test_match = re.search(r'def (test_[a-zA-Z0-9_]+)', next_line)
                    if test_match:
                        test_name = test_match.group(1)
                        # Include decorators + function definition
                        test_lines = decorator_lines + [next_line]
                        i += 1
                        
                        # Find end of test
                        while i < len(lines):
                            curr_line = lines[i]
                            if not curr_line.strip():
                                test_lines.append(curr_line)
                                i += 1
                                continue
                            if curr_line and not curr_line[0].isspace():
                                break
                            test_lines.append(curr_line)
                            i += 1
                        
                        module = categorize_test(test_name)
                        sections['tests'][module].extend(test_lines)
                        sections['tests'][module].append('')
                        continue
                
                # Test class with decorators
                elif re.match(r'^class Test', next_line):
                    class_match = re.search(r'class (Test[A-Za-z0-9_]+)', next_line)
                    if class_match:
                        class_name = class_match.group(1)
                        class_lines = decorator_lines + [next_line]
                        i += 1
                        
                        while i < len(lines):
                            curr_line = lines[i]
                            if not curr_line.strip():
                                class_lines.append(curr_line)
                                i += 1
                                continue
                            if curr_line and not curr_line[0].isspace():
                                break
                            class_lines.append(curr_line)
                            i += 1
                        
                        module = categorize_test(class_name.lower())
                        sections['classes'][module].extend(class_lines)
                        sections['classes'][module].append('')
                        continue
            
            # Orphaned decorator - shouldn't happen, but include in helpers
            sections['helpers'].extend(decorator_lines)
            continue
        
        # Test function (no decorator)
        elif re.match(r'^(async )?def test_', line):
            test_match = re.search(r'def (test_[a-zA-Z0-9_]+)', line)
            if test_match:
                test_name = test_match.group(1)
                test_lines = [line]
                i += 1
                
                # Find end of test - check for column 0 non-indented line
                while i < len(lines):
                    curr_line = lines[i]
                    # Empty lines are part of the function
                    if not curr_line.strip():
                        test_lines.append(curr_line)
                        i += 1
                        continue
                    # Non-empty line at column 0 means function ended
                    if curr_line and not curr_line[0].isspace():
                        break
                    test_lines.append(curr_line)
                    i += 1
                
                module = categorize_test(test_name)
                sections['tests'][module].extend(test_lines)
                sections['tests'][module].append('')
                continue
        
        # Helper function
        elif re.match(r'^def [a-z_]', line) and not re.match(r'^def test_', line):
            func_match = re.search(r'def ([a-z_][a-zA-Z0-9_]*)', line)
            if func_match:
                func_lines = [line]
                i += 1
                
                # Handle multi-line function signature - find the ):
                in_signature = ')' not in line or ':' not in line
                while i < len(lines) and in_signature:
                    func_lines.append(lines[i])
                    if ')' in lines[i] and ':' in lines[i]:
                        in_signature = False
                    i += 1
                
                # Now extract function body - check for column 0 non-indented line
                while i < len(lines):
                    curr_line = lines[i]
                    # Empty lines are part of the function
                    if not curr_line.strip():
                        func_lines.append(curr_line)
                        i += 1
                        continue
                    # Non-empty line at column 0 means function ended
                    if curr_line and not curr_line[0].isspace():
                        break
                    func_lines.append(curr_line)
                    i += 1
                
                sections['helpers'].extend(func_lines)
                sections['helpers'].append('')
                continue
        
        i += 1
    
    return sections


def update_imports_and_patches(content, module_name):
    """Update imports and patch targets for a specific module."""
    
    # Update imports from _mcp to _tools modules
    for func_name, func_module in FUNCTION_MODULE_MAP.items():
        old_import = f'from deephaven_mcp.mcp_systems_server._mcp import {func_name}'
        new_import = f'from deephaven_mcp.mcp_systems_server._tools.{func_module} import {func_name}'
        content = content.replace(old_import, new_import)
        
        # Update patch targets
        old_patch = f'"deephaven_mcp.mcp_systems_server._mcp.{func_name}"'
        new_patch = f'"deephaven_mcp.mcp_systems_server._tools.{func_module}.{func_name}"'
        content = content.replace(old_patch, new_patch)
        
        # Single quotes
        old_patch_single = f"'deephaven_mcp.mcp_systems_server._mcp.{func_name}'"
        new_patch_single = f"'deephaven_mcp.mcp_systems_server._tools.{func_module}.{func_name}'"
        content = content.replace(old_patch_single, new_patch_single)
    
    # Update logger patches to point to correct module
    # Handle both base _LOGGER and _LOGGER.method patterns
    import re
    
    # Replace _LOGGER.method patterns (e.g., _LOGGER.info, _LOGGER.warning)
    content = re.sub(
        r'"deephaven_mcp\.mcp_systems_server\._mcp\._LOGGER\.(info|warning|error|debug|exception)"',
        rf'"deephaven_mcp.mcp_systems_server._tools.{module_name}._LOGGER.\1"',
        content
    )
    
    # Replace base _LOGGER pattern (without method)
    content = content.replace(
        '"deephaven_mcp.mcp_systems_server._mcp._LOGGER"',
        f'"deephaven_mcp.mcp_systems_server._tools.{module_name}._LOGGER"'
    )
    
    # Fix any already-updated session._LOGGER references
    content = content.replace(
        '"deephaven_mcp.mcp_systems_server._tools.session._LOGGER"',
        f'"deephaven_mcp.mcp_systems_server._tools.{module_name}._LOGGER"'
    )
    
    # Update module-specific patches (queries, formatters, types imported in _tools modules)
    # These need to point to where they're imported in the _tools module, not globally
    module_specific_patches = {
        'session': {
            'deephaven_mcp.mcp_systems_server._mcp.queries': 'deephaven_mcp.mcp_systems_server._tools.session.queries',
        },
        'catalog': {
            'deephaven_mcp.mcp_systems_server._mcp.queries.get_catalog_meta_table': 'deephaven_mcp.mcp_systems_server._tools.catalog.queries.get_catalog_meta_table',
            'deephaven_mcp.mcp_systems_server._mcp.queries.get_catalog_table': 'deephaven_mcp.mcp_systems_server._tools.catalog.queries.get_catalog_table',
            'deephaven_mcp.mcp_systems_server._mcp.queries.get_catalog_table_data': 'deephaven_mcp.mcp_systems_server._tools.catalog.queries.get_catalog_table_data',
            'deephaven_mcp.mcp_systems_server._mcp.format_table_data': 'deephaven_mcp.mcp_systems_server._tools.catalog.format_table_data',
            'deephaven_mcp.mcp_systems_server._mcp.RestartUsersEnum': 'deephaven_mcp.mcp_systems_server._tools.catalog.RestartUsersEnum',
            'deephaven_mcp.mcp_systems_server._mcp.ExportedObjectTypeEnum': 'deephaven_mcp.mcp_systems_server._tools.catalog.ExportedObjectTypeEnum',
        },
        'table': {
            'deephaven_mcp.mcp_systems_server._mcp.queries.get_table': 'deephaven_mcp.mcp_systems_server._tools.table.queries.get_table',
            'deephaven_mcp.mcp_systems_server._mcp.queries.get_table_data': 'deephaven_mcp.mcp_systems_server._tools.table.queries.get_table_data',
            'deephaven_mcp.mcp_systems_server._mcp.format_table_data': 'deephaven_mcp.mcp_systems_server._tools.table.format_table_data',
            'deephaven_mcp.mcp_systems_server._mcp.RestartUsersEnum': 'deephaven_mcp.mcp_systems_server._tools.table.RestartUsersEnum',
            'deephaven_mcp.mcp_systems_server._mcp.ExportedObjectTypeEnum': 'deephaven_mcp.mcp_systems_server._tools.table.ExportedObjectTypeEnum',
        },
        'pq': {
            'deephaven_mcp.mcp_systems_server._mcp.queries.get_pq_snapshot': 'deephaven_mcp.mcp_systems_server._tools.pq.queries.get_pq_snapshot',
            'deephaven_mcp.mcp_systems_server._mcp.format_table_data': 'deephaven_mcp.mcp_systems_server._tools.pq.format_table_data',
            'deephaven_mcp.mcp_systems_server._mcp.RestartUsersEnum': 'deephaven_mcp.mcp_systems_server._tools.pq.RestartUsersEnum',
            'deephaven_mcp.mcp_systems_server._mcp.ExportedObjectTypeEnum': 'deephaven_mcp.mcp_systems_server._tools.pq.ExportedObjectTypeEnum',
        },
        'session_enterprise': {
            'deephaven_mcp.mcp_systems_server._mcp.RestartUsersEnum': 'deephaven_mcp.mcp_systems_server._tools.session_enterprise.RestartUsersEnum',
            'deephaven_mcp.mcp_systems_server._mcp.ExportedObjectTypeEnum': 'deephaven_mcp.mcp_systems_server._tools.session_enterprise.ExportedObjectTypeEnum',
        },
        'shared': {
            'deephaven_mcp.mcp_systems_server._mcp.RestartUsersEnum': 'deephaven_mcp.mcp_systems_server._tools.shared.RestartUsersEnum',
            'deephaven_mcp.mcp_systems_server._mcp.ExportedObjectTypeEnum': 'deephaven_mcp.mcp_systems_server._tools.shared.ExportedObjectTypeEnum',
        },
    }
    
    # Apply module-specific patches (handle both double and single quotes)
    if module_name in module_specific_patches:
        for old, new in module_specific_patches[module_name].items():
            # Double quotes
            content = content.replace(f'"{old}"', f'"{new}"')
            # Single quotes
            content = content.replace(f"'{old}'", f"'{new}'")
    
    # Update global patches (things that don't depend on module)
    # For mcp_server and session_community module tests, patches need to point to where imports are USED, not defined
    # script and session tests also call session_community_create, so they need session_community patches
    if module_name == 'mcp_server':
        global_patches = {
            '"deephaven_mcp.mcp_systems_server._mcp.ConfigManager"': f'"deephaven_mcp.mcp_systems_server._tools.{module_name}.ConfigManager"',
            '"deephaven_mcp.mcp_systems_server._mcp.CombinedSessionRegistry"': f'"deephaven_mcp.mcp_systems_server._tools.{module_name}.CombinedSessionRegistry"',
            '"deephaven_mcp.mcp_systems_server._mcp.asyncio.Lock"': f'"deephaven_mcp.mcp_systems_server._tools.{module_name}.asyncio.Lock"',
            '"deephaven_mcp.mcp_systems_server._mcp.InstanceTracker"': f'"deephaven_mcp.mcp_systems_server._tools.{module_name}.InstanceTracker"',
            '"deephaven_mcp.mcp_systems_server._mcp.cleanup_orphaned_resources"': f'"deephaven_mcp.mcp_systems_server._tools.{module_name}.cleanup_orphaned_resources"',
            '"deephaven_mcp.mcp_systems_server._mcp.datetime"': f'"deephaven_mcp.mcp_systems_server._tools.{module_name}.datetime"',
        }
    elif module_name in ['session_community', 'script', 'session']:
        # These modules test session_community_create, so patch where resource_manager imports are USED in session_community
        global_patches = {
            '"deephaven_mcp.mcp_systems_server._mcp.ConfigManager"': '"deephaven_mcp.config.ConfigManager"',
            '"deephaven_mcp.mcp_systems_server._mcp.CombinedSessionRegistry"': '"deephaven_mcp.resource_manager.CombinedSessionRegistry"',
            '"deephaven_mcp.mcp_systems_server._mcp.DynamicCommunitySessionManager"': '"deephaven_mcp.mcp_systems_server._tools.session_community.DynamicCommunitySessionManager"',
            '"deephaven_mcp.mcp_systems_server._mcp.EnterpriseSessionManager"': '"deephaven_mcp.resource_manager.EnterpriseSessionManager"',
            '"deephaven_mcp.mcp_systems_server._mcp.find_available_port"': '"deephaven_mcp.mcp_systems_server._tools.session_community.find_available_port"',
            '"deephaven_mcp.mcp_systems_server._mcp.generate_auth_token"': '"deephaven_mcp.mcp_systems_server._tools.session_community.generate_auth_token"',
            '"deephaven_mcp.mcp_systems_server._mcp.launch_session"': '"deephaven_mcp.mcp_systems_server._tools.session_community.launch_session"',
            '"deephaven_mcp.mcp_systems_server._mcp.get_config_section"': '"deephaven_mcp.config.get_config_section"',
            '"deephaven_mcp.mcp_systems_server._mcp.datetime"': f'"deephaven_mcp.mcp_systems_server._tools.{module_name}.datetime"',
            '"deephaven_mcp.mcp_systems_server._mcp.asyncio.Lock"': '"asyncio.Lock"',
        }
    else:
        global_patches = {
            '"deephaven_mcp.mcp_systems_server._mcp.ConfigManager"': '"deephaven_mcp.config.ConfigManager"',
            '"deephaven_mcp.mcp_systems_server._mcp.CombinedSessionRegistry"': '"deephaven_mcp.resource_manager.CombinedSessionRegistry"',
            '"deephaven_mcp.mcp_systems_server._mcp.DynamicCommunitySessionManager"': '"deephaven_mcp.resource_manager.DynamicCommunitySessionManager"',
            '"deephaven_mcp.mcp_systems_server._mcp.EnterpriseSessionManager"': '"deephaven_mcp.resource_manager.EnterpriseSessionManager"',
            '"deephaven_mcp.mcp_systems_server._mcp.find_available_port"': '"deephaven_mcp.resource_manager.find_available_port"',
            '"deephaven_mcp.mcp_systems_server._mcp.generate_auth_token"': '"deephaven_mcp.resource_manager.generate_auth_token"',
            '"deephaven_mcp.mcp_systems_server._mcp.launch_session"': '"deephaven_mcp.resource_manager.launch_session"',
            '"deephaven_mcp.mcp_systems_server._mcp.get_config_section"': '"deephaven_mcp.config.get_config_section"',
            '"deephaven_mcp.mcp_systems_server._mcp.datetime"': f'"deephaven_mcp.mcp_systems_server._tools.{module_name}.datetime"',
            '"deephaven_mcp.mcp_systems_server._mcp.asyncio.Lock"': '"asyncio.Lock"',
        }
    
    # Apply global patches
    for old, new in global_patches.items():
        content = content.replace(old, new)
    
    # Update spec= references (for MagicMock specs)
    spec_replacements = {
        'spec=mcp_mod.EnterpriseSessionManager': 'spec=EnterpriseSessionManager',
        'spec=mcp_mod.DynamicCommunitySessionManager': 'spec=DynamicCommunitySessionManager',
        'spec=mcp_mod.CombinedSessionRegistry': 'spec=CombinedSessionRegistry',
    }
    
    for old, new in spec_replacements.items():
        content = content.replace(old, new)
    
    return content


def build_test_file_header(module_name):
    """Build header for a test file."""
    return f'''"""
Tests for deephaven_mcp.mcp_systems_server._tools.{module_name}.
"""

import asyncio
import os
import warnings
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, PropertyMock, patch

import pytest

import deephaven_mcp.mcp_systems_server._mcp as mcp_mod
from conftest import MockContext, create_mock_instance_tracker
'''


def count_items_in_original(content):
    """Count tests, helpers, and classes in original file."""
    lines = content.splitlines()
    
    test_count = 0
    helper_count = 0
    class_count = 0
    
    for line in lines:
        if re.match(r'^(async )?def test_', line):
            test_count += 1
        elif re.match(r'^def [a-z_]', line) and not re.match(r'^def test_', line):
            helper_count += 1
        elif re.match(r'^class ', line):
            class_count += 1
    
    return {
        'tests': test_count,
        'helpers': helper_count,
        'classes': class_count,
    }


def validate_extraction(original_counts, sections):
    """Validate that all items were extracted exactly once."""
    
    # Count extracted tests - must match column 0 like original counting
    extracted_test_count = sum(
        len([l for l in test_lines if re.match(r'^(async )?def test_', l)])
        for test_lines in sections['tests'].values()
    )
    
    # Count extracted helpers
    extracted_helper_count = len([
        l for l in sections['helpers'] if re.match(r'^def [a-z_]', l)
    ])
    
    # Count extracted classes
    extracted_class_count = len([
        l for l in sections['helpers'] if re.match(r'^class ', l)
    ])
    
    # Add classes from test sections
    for class_lines in sections['classes'].values():
        extracted_class_count += len([
            l for l in class_lines if re.match(r'^class ', l)
        ])
    
    errors = []
    
    if extracted_test_count != original_counts['tests']:
        errors.append(f"Test count mismatch: original={original_counts['tests']}, extracted={extracted_test_count}")
    
    if extracted_helper_count != original_counts['helpers']:
        errors.append(f"Helper count mismatch: original={original_counts['helpers']}, extracted={extracted_helper_count}")
    
    if extracted_class_count != original_counts['classes']:
        errors.append(f"Class count mismatch: original={original_counts['classes']}, extracted={extracted_class_count}")
    
    return errors


def main():
    """Main test refactoring execution."""
    print("=" * 80)
    print("COMPREHENSIVE TEST REFACTORING v2 - WITH VALIDATION")
    print("=" * 80)
    
    repo_root = Path(__file__).parent.parent
    test_file = repo_root / "tests/mcp_systems_server/test__mcp.py"
    test_dir = repo_root / "tests/mcp_systems_server/_tools"
    
    print(f"\n1. Reading test file: {test_file}")
    with open(test_file) as f:
        content = f.read()
    
    print(f"   - {len(content.splitlines())} lines")
    
    print("\n2. Counting original items...")
    original_counts = count_items_in_original(content)
    print(f"   - Tests: {original_counts['tests']}")
    print(f"   - Helpers: {original_counts['helpers']}")
    print(f"   - Classes: {original_counts['classes']}")
    
    print("\n3. Extracting sections...")
    sections = extract_sections(content)
    
    print(f"   - Header: {len(sections['header'])} lines")
    print(f"   - Helpers: {len(sections['helpers'])} lines")
    print(f"   - Tests by module:")
    for module in sorted(sections['tests'].keys()):
        print(f"     - {module}: {len([l for l in sections['tests'][module] if l.strip().startswith('def test_') or l.strip().startswith('async def test_')])} tests")
    
    if sections['classes']:
        print(f"   - Test classes by module:")
        for module in sorted(sections['classes'].keys()):
            print(f"     - {module}: {len([l for l in sections['classes'][module] if l.strip().startswith('class Test')])} classes")
    
    print("\n4. Validating extraction...")
    validation_errors = validate_extraction(original_counts, sections)
    if validation_errors:
        print("   ❌ VALIDATION FAILED:")
        for error in validation_errors:
            print(f"      - {error}")
        return 1
    print("   ✅ All items extracted exactly once")
    
    print("\n5. Creating output directory and conftest.py...")
    test_dir.mkdir(parents=True, exist_ok=True)
    print(f"   - Created directory: {test_dir}")
    
    conftest_path = test_dir / "conftest.py"
    with open(conftest_path, 'w') as f:
        f.write('"""Shared test fixtures and helpers for mcp_systems_server tests."""\n\n')
        f.write('from unittest.mock import AsyncMock, MagicMock\n\n')
        f.write('\n'.join(sections['helpers']))
    print(f"   - Created {conftest_path}")
    
    print("\n6. Creating module-specific test files...")
    modules = set(sections['tests'].keys()) | set(sections['classes'].keys())
    created_files = []
    
    for module_name in sorted(modules):
        test_file_path = test_dir / f"test_{module_name}.py"
        
        with open(test_file_path, 'w') as f:
            # Header
            f.write(build_test_file_header(module_name))
            f.write('\n')
            
            # Module imports - analyze which functions are used
            module_content = '\n'.join(sections['tests'][module_name] + sections['classes'].get(module_name, []))
            
            # Find all _mcp imports in this content using more precise matching
            imports_needed = set()
            for func_name in FUNCTION_MODULE_MAP.keys():
                # Use word boundary matching to avoid false positives in comments/strings
                import_pattern = rf'\b{re.escape(func_name)}\b'
                if re.search(import_pattern, module_content):
                    imports_needed.add(func_name)
            
            # Build imports
            import_by_module = defaultdict(set)
            for func_name in imports_needed:
                func_module = FUNCTION_MODULE_MAP[func_name]
                import_by_module[func_module].add(func_name)
            
            # Write imports
            if import_by_module:
                for import_module in sorted(import_by_module.keys()):
                    funcs = sorted(import_by_module[import_module])
                    if len(funcs) == 1:
                        f.write(f'from deephaven_mcp.mcp_systems_server._tools.{import_module} import {funcs[0]}\n')
                    else:
                        f.write(f'from deephaven_mcp.mcp_systems_server._tools.{import_module} import (\n')
                        for func in funcs:
                            f.write(f'    {func},\n')
                        f.write(')\n')
            
            # Add other common imports
            f.write('from deephaven_mcp import config\n')
            f.write('from deephaven_mcp.resource_manager import (\n')
            f.write('    DockerLaunchedSession,\n')
            f.write('    DynamicCommunitySessionManager,\n')
            f.write('    EnterpriseSessionManager,\n')
            f.write('    PythonLaunchedSession,\n')
            f.write('    ResourceLivenessStatus,\n')
            f.write('    SystemType,\n')
            f.write(')\n\n\n')
            
            # Write classes for this module
            if module_name in sections['classes']:
                class_content = '\n'.join(sections['classes'][module_name])
                class_content = update_imports_and_patches(class_content, module_name)
                f.write(class_content)
                f.write('\n\n')
            
            # Write tests for this module
            test_content = '\n'.join(sections['tests'][module_name])
            test_content = update_imports_and_patches(test_content, module_name)
            f.write(test_content)
        
        created_files.append(test_file_path)
        print(f"   - Created {test_file_path}")
    
    print(f"\n   → Created {len(created_files)} test files")
    
    print("\n7. Backing up original test file...")
    backup_path = test_file.with_suffix('.py.original')
    with open(test_file) as f_in, open(backup_path, 'w') as f_out:
        f_out.write(f_in.read())
    print(f"   - Backed up to {backup_path}")
    
    print("\n8. Removing original test__mcp.py...")
    test_file.unlink()
    print(f"   - Removed {test_file}")
    
    print("\n9. Validating with pytest collection...")
    result = subprocess.run(
        ["uv", "run", "pytest", "--collect-only", "-q", str(test_dir)],
        capture_output=True,
        text=True,
        cwd=repo_root
    )
    
    if result.returncode == 0:
        # Count collected tests
        output_lines = result.stdout.splitlines()
        test_count = 0
        for line in output_lines:
            if ' test' in line.lower():
                test_count += 1
        print(f"   ✅ pytest collection successful")
        print(f"   - Found test files and can collect tests")
    else:
        print(f"   ⚠️  pytest collection had issues:")
        print(result.stdout)
        print(result.stderr)
    
    print("\n" + "=" * 80)
    print("✅ TEST REFACTORING COMPLETE")
    print("=" * 80)
    print(f"\nCreated {len(modules)} test files")
    print(f"Backup: {backup_path}")
    print("\nNext: Run full test suite to verify")
    print("  uv run pytest tests/mcp_systems_server/ -v")


if __name__ == "__main__":
    main()
