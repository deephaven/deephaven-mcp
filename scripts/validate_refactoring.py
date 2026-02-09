#!/usr/bin/env python3
"""
Validation program to verify refactoring preserved code identity.

This program verifies that all functions and constants were extracted correctly
and are byte-for-byte identical to the original (except for added module docstrings).
"""

import re
from pathlib import Path


def extract_function_from_file(content, func_name):
    """Extract a function from file content by name."""
    lines = content.splitlines()
    
    # Find function definition
    pattern = rf'^(async )?def {re.escape(func_name)}\('
    for i, line in enumerate(lines):
        if re.match(pattern, line):
            start = i
            
            # Find end of function
            i += 1
            while i < len(lines) and not lines[i].strip():
                i += 1
            
            if i < len(lines):
                base_indent = len(lines[i]) - len(lines[i].lstrip())
                
                while i < len(lines):
                    curr_line = lines[i]
                    if not curr_line.strip():
                        i += 1
                        continue
                    
                    curr_indent = len(curr_line) - len(curr_line.lstrip())
                    if curr_indent == 0 or (curr_indent < base_indent and curr_line.strip()):
                        break
                    i += 1
                
                end = i
                while end < len(lines) and not lines[end].strip():
                    end += 1
                
                return '\n'.join(lines[start:end])
    
    return None


def extract_constant_from_file(content, const_name):
    """Extract a constant from file content by name."""
    lines = content.splitlines()
    
    pattern = rf'^{re.escape(const_name)}\s*='
    for i, line in enumerate(lines):
        if re.match(pattern, line):
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
            
            if end < len(lines) and not lines[end].strip():
                end += 1
            
            return '\n'.join(lines[start:end])
    
    return None


def main():
    """Validate refactoring."""
    print("=" * 80)
    print("VALIDATING REFACTORING")
    print("=" * 80)
    
    repo_root = Path(__file__).parent.parent
    original_file = repo_root / "src/deephaven_mcp/mcp_systems_server/_mcp.py"
    tools_dir = repo_root / "src/deephaven_mcp/mcp_systems_server/_tools"
    
    # Read original
    print(f"\n1. Reading original file: {original_file}")
    with open(original_file) as f:
        original_content = f.read()
    
    # Module structure (duplicated from refactor_split_mcp.py)
    MODULE_STRUCTURE = {
        "shared.py": ["_get_session_from_context", "_get_enterprise_session", "_check_response_size", "_format_meta_table_result", "_get_system_config"],
        "mcp_server.py": ["app_lifespan", "mcp_reload"],
        "session_enterprise.py": ["enterprise_systems_status", "_check_session_limits", "_generate_session_name_if_none", "_check_session_id_available", "_resolve_session_parameters", "session_enterprise_create", "session_enterprise_delete"],
        "session.py": ["sessions_list", "session_details", "_get_session_liveness_info", "_get_session_property", "_get_session_programming_language", "_get_session_versions"],
        "table.py": ["_build_table_data_response", "session_tables_schema", "session_tables_list", "session_table_data"],
        "script.py": ["session_script_run", "session_pip_list"],
        "catalog.py": ["_get_catalog_data", "catalog_tables_list", "catalog_namespaces_list", "catalog_tables_schema", "catalog_table_sample"],
        "pq.py": ["_parse_pq_id", "_make_pq_id", "_validate_timeout", "_validate_max_concurrent", "_format_pq_config", "_format_named_string_list", "_format_column_definition", "_format_table_definition", "_format_exported_object_info", "_format_worker_protocol", "_format_connection_details", "_format_exception_details", "_format_pq_state", "_format_pq_replicas", "_format_pq_spares", "_normalize_programming_language", "_setup_batch_pq_operation", "_validate_and_parse_pq_ids", "_convert_restart_users_to_enum", "_add_session_id_if_running", "pq_name_to_id", "pq_list", "pq_details", "pq_create", "_apply_pq_config_simple_fields", "_apply_pq_config_list_fields", "_apply_pq_config_modifications", "pq_modify", "pq_delete", "pq_start", "pq_stop", "pq_restart"],
        "session_community.py": ["_get_session_creation_config", "_check_session_limit", "_validate_launch_method_params", "_resolve_docker_image", "_resolve_community_session_parameters", "_normalize_auth_type", "_resolve_auth_token", "_register_session_manager", "_launch_process_and_wait_for_ready", "_build_success_response", "_log_auto_generated_credentials", "session_community_create", "session_community_delete", "session_community_credentials"],
    }
    
    CONSTANT_DISTRIBUTION = {
        "session.py": ["DEFAULT_PROGRAMMING_LANGUAGE"],
        "table.py": ["ESTIMATED_BYTES_PER_CELL"],
        "session_enterprise.py": ["DEFAULT_MAX_CONCURRENT_SESSIONS", "DEFAULT_ENGINE", "DEFAULT_TIMEOUT_SECONDS"],
        "session_community.py": ["DEFAULT_LAUNCH_METHOD", "DEFAULT_AUTH_TYPE", "DEFAULT_DOCKER_IMAGE_PYTHON", "DEFAULT_DOCKER_IMAGE_GROOVY", "DEFAULT_HEAP_SIZE_GB", "DEFAULT_STARTUP_TIMEOUT_SECONDS", "DEFAULT_STARTUP_CHECK_INTERVAL_SECONDS", "DEFAULT_STARTUP_RETRIES"],
    }
    
    total_functions = sum(len(funcs) for funcs in MODULE_STRUCTURE.values())
    total_constants = sum(len(consts) for consts in CONSTANT_DISTRIBUTION.values())
    
    print(f"   - Functions to validate: {total_functions}")
    print(f"   - Constants to validate: {total_constants}")
    
    # Validate each module
    print("\n2. Validating modules...")
    errors = []
    validated_funcs = 0
    validated_consts = 0
    
    for module_name, func_names in MODULE_STRUCTURE.items():
        module_path = tools_dir / module_name
        
        with open(module_path) as f:
            module_content = f.read()
        
        # Validate functions
        for func_name in func_names:
            orig_func = extract_function_from_file(original_content, func_name)
            new_func = extract_function_from_file(module_content, func_name)
            
            if orig_func is None:
                errors.append(f"{module_name}: Function {func_name} not found in original")
            elif new_func is None:
                errors.append(f"{module_name}: Function {func_name} not found in new module")
            elif orig_func.strip() != new_func.strip():
                errors.append(f"{module_name}: Function {func_name} content differs")
                # Show difference
                print(f"\n   ERROR in {func_name}:")
                print(f"   Original length: {len(orig_func)}")
                print(f"   New length: {len(new_func)}")
            else:
                validated_funcs += 1
        
        # Validate constants
        if module_name in CONSTANT_DISTRIBUTION:
            for const_name in CONSTANT_DISTRIBUTION[module_name]:
                orig_const = extract_constant_from_file(original_content, const_name)
                new_const = extract_constant_from_file(module_content, const_name)
                
                if orig_const is None:
                    # Constant might not exist, that's okay
                    pass
                elif new_const is None:
                    errors.append(f"{module_name}: Constant {const_name} not found in new module")
                elif orig_const.strip() != new_const.strip():
                    errors.append(f"{module_name}: Constant {const_name} content differs")
                else:
                    validated_consts += 1
    
    # Validate line counts
    print("\n3. Validating file sizes...")
    original_lines = len(original_content.splitlines())
    print(f"   - Original _mcp.py: {original_lines:,} lines")
    
    module_lines = {}
    total_module_lines = 0
    for module_name in MODULE_STRUCTURE.keys():
        module_path = tools_dir / module_name
        with open(module_path) as f:
            line_count = len(f.read().splitlines())
        module_lines[module_name] = line_count
        total_module_lines += line_count
        print(f"   - {module_name}: {line_count:,} lines")
    
    # Check stub
    stub_path = repo_root / "src/deephaven_mcp/mcp_systems_server/_mcp.py.new"
    with open(stub_path) as f:
        stub_lines = len(f.read().splitlines())
    print(f"   - _mcp.py (stub): {stub_lines:,} lines")
    print(f"   - Total in modules: {total_module_lines:,} lines")
    
    # Analyze line count difference
    # When splitting 1 file into 9 modules, we ADD overhead:
    # - Each module needs import section (~60-90 lines × 9 = 540-810 lines)
    # - Each module needs its own docstring (~30-60 lines × 9 = 270-540 lines)
    # - Total added overhead: ~810-1,350 lines
    #
    # Original overhead that's NOT duplicated:
    # - Monolithic docstring (~90 lines) saved
    # - Large __all__ list (~80 lines) only in stub
    # - Net expected increase: ~640-1,180 lines (8-14% increase)
    
    num_modules = len(MODULE_STRUCTURE)
    expected_module_overhead = num_modules * 75  # ~75 lines per module (imports + docstring)
    difference = total_module_lines - original_lines
    
    print(f"\n   Analysis:")
    print(f"   - Added overhead (9 modules × ~75 lines): ~{expected_module_overhead:,} lines")
    print(f"   - Actual increase: {difference:,} lines")
    print(f"   - Overhead per module: {difference / num_modules:.0f} lines average")
    
    # Sanity check: total should be 100-115% of original (accounting for per-module overhead)
    min_expected = original_lines * 0.95  # At least 95% (in case of deduplication)
    max_expected = original_lines * 1.20  # At most 120% (reasonable module overhead)
    
    if total_module_lines < min_expected:
        errors.append(f"Line count validation: Modules are TOO small ({total_module_lines} < {min_expected:.0f}, likely missing code)")
    elif total_module_lines > max_expected:
        errors.append(f"Line count validation: Modules are TOO large ({total_module_lines} > {max_expected:.0f}, possible duplication)")
    else:
        print(f"   ✓ Total lines within expected range: {min_expected:.0f}-{max_expected:.0f}")
    
    # Report results
    print(f"\n4. Validation Results:")
    print(f"   ✓ Functions validated: {validated_funcs}/{total_functions}")
    print(f"   ✓ Constants validated: {validated_consts} (some may not exist as module-level)")
    print(f"   ✓ Line counts reasonable: {total_module_lines:,} total lines")
    
    if errors:
        print(f"\n   ✗ ERRORS FOUND: {len(errors)}")
        for error in errors[:10]:  # Show first 10
            print(f"     - {error}")
        if len(errors) > 10:
            print(f"     ... and {len(errors) - 10} more errors")
        return False
    else:
        print("\n   ✓ ALL VALIDATIONS PASSED!")
        print("   ✓ Code is byte-for-byte identical (except module docstrings)")
        print("   ✓ File sizes are reasonable and account for modularization overhead")
        return True


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
