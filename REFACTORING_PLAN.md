# MCP Systems Server Refactoring Plan

## Overview
Refactor `src/deephaven_mcp/mcp_systems_server/_mcp.py` (8,208 lines, 77 functions) into logical modules using automated programs to ensure 100% code identity.

## Current Structure Analysis
- **Total Lines**: 8,208
- **Total Functions**: 77 (mix of MCP tools and helper functions)
- **Test File**: 13,290 lines (`tests/mcp_systems_server/test__mcp.py`)

## Proposed Module Structure

All new modules will be in `src/deephaven_mcp/mcp_systems_server/_tools/` subdirectory.
Helpers are colocated with the tools that use them to minimize scope.
Constants are distributed to relevant modules.

### 1. `_tools/shared.py` - Shared Helper Functions (Cross-Module Dependencies)
- Helper functions used by multiple modules:
  - `_get_session_from_context` (L636) - Used by table.py, script.py, catalog.py, session.py
  - `_get_enterprise_session` (L684) - Used by catalog.py, session.py
  - `_check_response_size` (L1637) - Used by table.py, catalog.py
  - `_format_meta_table_result` (L2900) - Used by table.py, catalog.py
  - `_get_system_config` (L3071) - Used by session_enterprise.py, pq.py
- Constants: (none needed)
- Rationale: These 5 helpers are each used by 2-4 different modules, preventing circular dependencies

### 2. `_tools/mcp_server.py` - MCP Server Setup and System Management
- Lines: ~1-310, plus system management tools
- Contains:
  - All imports for server setup
  - `mcp_server` variable creation
  - `app_lifespan` function (L206)
  - `mcp_reload` (L311) - System-wide reload of config and session clearing
- Constants: (none needed)

### 3. `_tools/session_enterprise.py` - Enterprise Session and System Management
- MCP Tools:
  - `enterprise_systems_status` (L388) - List enterprise systems and their status
  - `session_enterprise_create` (L3117)
  - `session_enterprise_delete` (L3495)
- Helpers (used only by these tools):
  - `_check_session_limits` (L2987)
  - `_generate_session_name_if_none` (L3018)
  - `_check_session_id_available` (L3046)
  - `_resolve_session_parameters` (L3436)
- Constants:
  - `DEFAULT_MAX_CONCURRENT_SESSIONS`
  - `DEFAULT_ENGINE`
  - `DEFAULT_TIMEOUT_SECONDS`
- Note: `_get_system_config` moved to shared.py (used by pq.py also)

### 4. `_tools/session.py` - Session Listing and Details
- MCP Tools:
  - `sessions_list` (L525)
  - `session_details` (L948)
- Helpers (used only by these tools):
  - `_get_session_liveness_info` (L738)
  - `_get_session_property` (L774)
  - `_get_session_programming_language` (L811)
  - `_get_session_versions` (L847)
- Constants:
  - `DEFAULT_PROGRAMMING_LANGUAGE`
- Note: `_get_session_from_context` and `_get_enterprise_session` moved to shared.py

### 5. `_tools/table.py` - Session Table Operations
- MCP Tools:
  - `session_tables_schema` (L1140)
  - `session_tables_list` (L1312)
  - `session_table_data` (L1675)
- Helpers (used only by these tools):
  - `_build_table_data_response` (L884)
- Constants:
  - `ESTIMATED_BYTES_PER_CELL`
- Note: `_check_response_size` and `_format_meta_table_result` moved to shared.py

### 6. `_tools/script.py` - Script and Package Management
- MCP Tools:
  - `session_script_run` (L1409)
  - `session_pip_list` (L1531)
- Helpers: (none)
- Constants: (none needed)

### 7. `_tools/catalog.py` - Catalog Operations
- MCP Tools:
  - `catalog_tables_list` (L2017)
  - `catalog_namespaces_list` (L2232)
  - `catalog_tables_schema` (L2375)
  - `catalog_table_sample` (L2699)
- Helpers (used only by these tools):
  - `_get_catalog_data` (L1897)
- Constants: (none needed)
- Note: Multiple helpers moved to shared.py (_get_session_from_context, _get_enterprise_session, _check_response_size, _format_meta_table_result)

### 8. `_tools/pq.py` - Persistent Query Management (ALL PQ functions colocated)
- MCP Tools:
  - `pq_name_to_id` (L4451)
  - `pq_list` (L4569)
  - `pq_details` (L4731)
  - `pq_create` (L5036)
  - `pq_delete` (L5276)
  - `pq_modify` (L5755)
  - `pq_start` (L5996)
  - `pq_stop` (L6255)
  - `pq_restart` (L6503)
- Helpers (used only by PQ tools - colocated):
  - `_parse_pq_id` (L3688)
  - `_make_pq_id` (L3716)
  - `_validate_timeout` (L3737)
  - `_validate_max_concurrent` (L3767)
  - `_format_pq_config` (L3788)
  - `_format_named_string_list` (L3880)
  - `_format_column_definition` (L3902)
  - `_format_table_definition` (L3938)
  - `_format_exported_object_info` (L3965)
  - `_format_worker_protocol` (L4005)
  - `_format_connection_details` (L4027)
  - `_format_exception_details` (L4062)
  - `_format_pq_state` (L4086)
  - `_format_pq_replicas` (L4189)
  - `_format_pq_spares` (L4211)
  - `_normalize_programming_language` (L4231)
  - `_setup_batch_pq_operation` (L4255)
  - `_validate_and_parse_pq_ids` (L4357)
  - `_convert_restart_users_to_enum` (L4398)
  - `_add_session_id_if_running` (L4426)
  - `_apply_pq_config_simple_fields` (L5511)
  - `_apply_pq_config_list_fields` (L5579)
  - `_apply_pq_config_modifications` (L5641)
- Constants: (none needed)

### 9. `_tools/session_community.py` - Community Session Management
- MCP Tools:
  - `session_community_create` (L7407)
  - `session_community_delete` (L7736)
  - `session_community_credentials` (L7938)
- Helpers (used only by these tools):
  - `_get_session_creation_config` (L6763)
  - `_check_session_limit` (L6789)
  - `_validate_launch_method_params` (L6812)
  - `_resolve_docker_image` (L6870)
  - `_resolve_community_session_parameters` (L6924)
  - `_normalize_auth_type` (L7081)
  - `_resolve_auth_token` (L7140)
  - `_register_session_manager` (L7192)
  - `_launch_process_and_wait_for_ready` (L7251)
  - `_build_success_response` (L7348)
  - `_log_auto_generated_credentials` (L7383)
- Constants:
  - `DEFAULT_LAUNCH_METHOD`
  - `DEFAULT_AUTH_TYPE`
  - `DEFAULT_DOCKER_IMAGE_PYTHON`
  - `DEFAULT_DOCKER_IMAGE_GROOVY`
  - `DEFAULT_HEAP_SIZE_GB`
  - `DEFAULT_STARTUP_TIMEOUT_SECONDS`
  - `DEFAULT_STARTUP_CHECK_INTERVAL_SECONDS`
  - `DEFAULT_STARTUP_RETRIES`

### 10. `_mcp.py` (stub) - Export all functions for backward compatibility
- Import all functions from `_tools/*` modules
- Export via `__all__` for backward compatibility with tests
- NO code duplication - pure import/export only

## Refactoring Execution Plan

### Phase 1: Source Code Refactoring (Steps 1-8)

1. **Analyze Structure** ✓
   - Completed: Function mapping and module groupings created

2. **Write Refactoring Program** (`scripts/refactor_split_mcp.py`)
   - Parse `_mcp.py` line-by-line to identify function and constant boundaries
   - Extract functions with exact text (byte-for-byte: formatting, pydocs, comments)
   - Extract constants with exact text (byte-for-byte: comments, docstrings)
   - Group functions and constants by module
   - Create `_tools/` subdirectory
   - Write new module files preserving exact order with aggressive imports
   - Generate stub `_mcp.py` that imports from `_tools/*` and exports via `__all__`
   - NO modifications to extracted code - pure copy operation

3. **Run Refactoring Program**
   - Execute: `uv run python scripts/refactor_split_mcp.py`
   - Output: `_tools/` directory with 9 module files + stub `_mcp.py`

4. **Add File-Level Pydocs**
   - Manually or via program: Add detailed module docstrings to each file
   - Quality level: Match existing documentation standards

5. **Write Validation Program** (`scripts/validate_refactoring.py`)
   - Compare function code between original and new files
   - Verify pydocs are identical (byte-for-byte)
   - Check function order preservation
   - Validate all functions are accounted for
   - Verify constants are properly distributed

6. **Run Validation**
   - Execute: `uv run python scripts/validate_refactoring.py`
   - Must show 100% match

7. **Fix Discrepancies** (if any)
   - Update refactoring program and re-run
   - Never manually fix - always regenerate

8. **Run Unit Tests**
   - Execute: `uv run pytest tests/mcp_systems_server/test__mcp.py -v`
   - Must achieve 100% pass rate
   - **CHECKPOINT**: Get user approval before proceeding

### Phase 2: Test Code Refactoring (Steps 9-13)

9. **Write Test Refactoring Program** (`scripts/refactor_split_tests.py`)
   - Parse test file structure (line-based)
   - Map tests to source modules in `_tools/`
   - Create `tests/mcp_systems_server/_tools/` subdirectory
   - Create test files: `test_<module>.py` matching source structure
   - Preserve test order within each file
   - Add aggressive imports

10. **Run Test Refactoring**
    - Execute: `uv run python scripts/refactor_split_tests.py`
    - Output: `tests/mcp_systems_server/_tools/` with 9 test files (matching source modules)

11. **Write Test Validation Program** (`scripts/validate_test_refactoring.py`)
    - Verify all tests preserved (byte-for-byte)
    - Check test order
    - Validate test code identity

12. **Run Test Validation**
    - Execute: `uv run python scripts/validate_test_refactoring.py`
    - Must show 100% match

13. **Run New Test Suite**
    - Execute: `uv run pytest tests/mcp_systems_server/_tools/ -v`
    - Must achieve 100% pass rate
    - Verify coverage matches original

### Phase 3: Public API Cleanup (Steps 14-16)

14. **Make Shared Functions Public**
    - After ALL validation passes, rename functions in `_tools/shared.py`
    - Remove leading underscore from functions that should be public:
      - `_get_session_from_context` → `get_session_from_context`
      - `_get_enterprise_session` → `get_enterprise_session`
      - `_check_response_size` → `check_response_size`
      - `_format_meta_table_result` → `format_meta_table_result`
      - `_get_system_config` → `get_system_config`
    - Update all imports in other `_tools/*` modules

15. **Clean Up `_mcp.py` Stub Exports**
    - Refactored tests import directly from `_tools/*`, not from stub
    - Remove exports of helper functions (prefixed with `_`)
    - Keep only MCP tools (decorated with `@mcp_server.tool()`) in exports
    - Significantly reduce `__all__` to only public MCP tool functions
    - Rationale: Stub is for external backward compatibility, not internal use

16. **Run Full Test Suite Again**
    - Execute: `uv run pytest tests/mcp_systems_server/ -v`
    - Verify all tests still pass with renamed public functions
    - This validates that the public API refactoring is correct

17. **Final Verification**
    - Confirm all tests passing
    - Confirm no manual edits were made
    - Document the new public API in shared.py module docstring

## Key Principles

1. **Automation First**: Use programs for all refactoring, never manual edits
2. **Identity Preservation**: Code must be byte-for-byte identical including constants (only imports can differ)
3. **Order Preservation**: Functions and constants stay in exact order within modules
4. **Scope Minimization**: Helpers colocated with tools that use them (no separate utility modules)
5. **Constant Distribution**: Move constants to the modules where they're used (preserving exact text)
6. **Aggressive Imports**: Better to over-import and clean up with linter later
7. **No Test Changes**: Tests unchanged until source is validated and passing
8. **Validation Gates**: Must pass validation + tests before proceeding to next phase
9. **Rollback Strategy**: If validation fails, fix program and regenerate (never manual fix)
10. **No Modifications**: Only extract and regroup - no code changes, no comment changes, no formatting changes
11. **Public API Last**: Rename shared.py functions to public ONLY after all validation passes

## Success Criteria

**Phase 1 & 2 (Source and Test Refactoring):**
- ✅ All functions extracted with identical code/pydocs/constants (byte-for-byte)
- ✅ Function and constant order preserved in new modules
- ✅ Constants distributed to relevant modules with exact text preservation
- ✅ All 9 modules have comprehensive file-level pydocs (including shared.py)
- ✅ Stub `_mcp.py` exports all functions correctly via `__all__`
- ✅ Validation program confirms 100% identity (every function, constant, pydoc)
- ✅ All original tests pass without modification (100% success rate)
- ✅ All refactored tests pass with identical coverage
- ✅ No manual edits required (programs handle everything)
- ✅ Shared helpers module only for cross-module dependencies (5 functions)
- ✅ Only imports differ between original and new files - everything else identical

**Phase 3 (Public API Cleanup):**
- ✅ Shared module functions made public (underscore removed)
- ✅ All imports updated to use new public names
- ✅ All tests still passing after public API changes
- ✅ Clean public API for cross-module utilities

## File Naming Convention

Source modules: `_tools/<category>.py` (e.g., `_tools/pq.py`, `_tools/session.py`, `_tools/shared.py`)
Test modules: `_tools/test_<category>.py` (e.g., `_tools/test_pq.py`, `_tools/test_session.py`, `_tools/test_shared.py`)

## Rollback Plan

If any step fails:
1. Identify issue in refactoring program
2. Fix program logic
3. Delete generated files
4. Re-run program
5. Re-validate
