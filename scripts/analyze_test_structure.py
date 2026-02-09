#!/usr/bin/env python3
"""
Analyze test__mcp.py structure to create comprehensive splitting plan.
"""

import re
from pathlib import Path
from collections import defaultdict


def extract_test_functions(content):
    """Extract all test functions with their line ranges."""
    tests = []
    lines = content.splitlines()
    
    for i, line in enumerate(lines):
        if re.match(r'^(async )?def test_', line):
            match = re.search(r'def (test_[a-zA-Z0-9_]+)', line)
            if match:
                test_name = match.group(1)
                
                # Find end of this test (next test or end of file)
                start_line = i
                end_line = len(lines)
                
                for j in range(i + 1, len(lines)):
                    if re.match(r'^(async )?def test_|^class Test', lines[j]):
                        end_line = j
                        break
                
                tests.append({
                    'name': test_name,
                    'start': start_line,
                    'end': end_line,
                    'is_async': line.strip().startswith('async'),
                })
    
    return tests


def categorize_test(test_name):
    """Categorize test by which module it belongs to."""
    name_lower = test_name.lower()
    
    # Check patterns in priority order
    if 'catalog' in name_lower:
        return 'catalog'
    elif any(x in name_lower for x in ['pq_', '_pq_', 'parse_pq', 'make_pq', 'validate_timeout', 
                                         'validate_max_concurrent', 'format_pq', 'format_named_string',
                                         'format_column_definition', 'format_table_definition',
                                         'format_exported_object', 'format_worker_protocol',
                                         'format_connection_details', 'format_exception_details',
                                         'format_pq_replicas', 'format_pq_spares', 
                                         'normalize_programming_language', 'setup_batch_pq',
                                         'validate_and_parse_pq', 'convert_restart_users',
                                         'add_session_id_if_running', 'apply_pq_config']):
        return 'pq'
    elif 'session_enterprise' in name_lower or 'enterprise_create' in name_lower or 'enterprise_delete' in name_lower or 'enterprise_systems_status' in name_lower or 'check_session_limits' in name_lower or 'check_session_id_available' in name_lower or 'generate_session_name' in name_lower or 'resolve_session_parameters' in name_lower:
        return 'session_enterprise'
    elif 'session_community' in name_lower or 'community_create' in name_lower or 'community_delete' in name_lower or 'community_credentials' in name_lower or 'normalize_auth_type' in name_lower or 'resolve_community_session' in name_lower or 'validate_launch_method' in name_lower or 'resolve_docker_image' in name_lower or 'resolve_auth_token' in name_lower or 'register_session_manager' in name_lower or 'launch_process_and_wait' in name_lower or 'build_success_response' in name_lower or 'log_auto_generated' in name_lower or 'get_session_creation_config' in name_lower or 'check_session_limit' in name_lower:
        return 'session_community'
    elif 'session_table' in name_lower or 'table_data' in name_lower or 'tables_schema' in name_lower or 'tables_list' in name_lower or 'build_table_data' in name_lower:
        return 'table'
    elif 'session_script' in name_lower or 'script_run' in name_lower or 'pip_list' in name_lower:
        return 'script'
    elif any(x in name_lower for x in ['check_response_size', 'get_session_from_context', 
                                        'get_enterprise_session', 'format_meta_table', 'get_system_config']):
        return 'shared'
    elif 'app_lifespan' in name_lower or 'mcp_reload' in name_lower:
        return 'mcp_server'
    elif 'sessions_list' in name_lower or 'session_details' in name_lower or 'get_session_liveness' in name_lower or 'get_session_property' in name_lower or 'get_session_programming_language' in name_lower or 'get_session_versions' in name_lower:
        return 'session'
    elif 'session' in name_lower:
        # Generic session tests - need to analyze content
        return 'session'
    else:
        return 'unknown'


def main():
    test_file = Path("tests/mcp_systems_server/test__mcp.py")
    
    with open(test_file) as f:
        content = f.read()
    
    print("=" * 80)
    print("TEST STRUCTURE ANALYSIS")
    print("=" * 80)
    
    tests = extract_test_functions(content)
    print(f"\nTotal tests found: {len(tests)}")
    
    # Categorize
    by_module = defaultdict(list)
    for test in tests:
        category = categorize_test(test['name'])
        by_module[category].append(test)
    
    print(f"\nTests by module:")
    for module in sorted(by_module.keys()):
        print(f"  {module}: {len(by_module[module])} tests")
    
    print("\n" + "=" * 80)
    print("DETAILED BREAKDOWN")
    print("=" * 80)
    
    for module in sorted(by_module.keys()):
        print(f"\n{module.upper()} ({len(by_module[module])} tests):")
        for test in sorted(by_module[module], key=lambda t: t['name']):
            async_marker = '[async]' if test['is_async'] else '[sync] '
            lines = test['end'] - test['start']
            print(f"  {async_marker} {test['name']} ({lines} lines)")
    
    # Find helper functions (non-test functions at module level)
    print("\n" + "=" * 80)
    print("HELPER FUNCTIONS")
    print("=" * 80)
    
    lines = content.splitlines()
    helpers = []
    for i, line in enumerate(lines):
        if re.match(r'^(async )?def [a-z_]', line) and not re.match(r'^(async )?def test_', line):
            match = re.search(r'def ([a-z_][a-zA-Z0-9_]*)', line)
            if match:
                helpers.append(match.group(1))
    
    print(f"\nFound {len(helpers)} helper functions:")
    for helper in helpers:
        print(f"  - {helper}")
    
    # Find classes
    print("\n" + "=" * 80)
    print("CLASSES")
    print("=" * 80)
    
    classes = []
    for i, line in enumerate(lines):
        if re.match(r'^class ', line):
            match = re.search(r'class ([A-Za-z][A-Za-z0-9_]*)', line)
            if match:
                classes.append(match.group(1))
    
    print(f"\nFound {len(classes)} classes:")
    for cls in classes:
        print(f"  - {cls}")


if __name__ == "__main__":
    main()
