#!/usr/bin/env python3
"""Add comprehensive module-level pydocs to all _tools/ modules."""

from pathlib import Path

# Module pydocs mapping
MODULE_PYDOCS = {
    "shared.py": '''"""
Shared Helper Functions for MCP Systems Server.

This module contains helper functions that are used across multiple MCP tool modules
to avoid code duplication and circular dependencies. These functions provide core
session access, validation, and formatting capabilities.

Functions:
    Session Access:
    - _get_session_from_context: Retrieve and validate a session from MCP context
    - _get_enterprise_session: Retrieve and validate an enterprise (Core+) session
    
    Validation and Safety:
    - _check_response_size: Estimate and validate response size to prevent memory issues
    
    Formatting:
    - _format_meta_table_result: Format table metadata for consistent API responses
    
    Configuration:
    - _get_system_config: Retrieve enterprise system configuration from config manager

Design Rationale:
    These functions are extracted to shared.py because they are used by 2-4 different
    modules each, preventing circular dependencies that would occur if they were
    colocated with any single user module.

Cross-Module Dependencies:
    - Used by: table.py, script.py, catalog.py, session.py, session_enterprise.py, pq.py
    - Core utilities for session management and data formatting

Note:
    In Phase 3 of the refactoring plan, these functions will be renamed to public
    (remove leading underscore) after all validation passes.
"""''',
    
    "mcp_server.py": '''"""
MCP Server Lifecycle and System Management.

This module contains the FastMCP server setup, lifecycle management, and system-wide
operations including configuration reloading.

Components:
    Server Lifecycle:
    - app_lifespan: Async context manager for server startup and shutdown
    - Manages ConfigManager and CombinedSessionRegistry initialization
    - Handles resource cleanup on shutdown
    
    System Management Tools:
    - mcp_reload: Atomically reload configuration and clear all sessions
    
MCP Tools (decorated with @mcp_server.tool()):
    - mcp_reload: System-wide configuration reload

Dependencies:
    - ConfigManager: Configuration management
    - CombinedSessionRegistry: Session registry management
    - cleanup_orphaned_resources: Resource cleanup on shutdown
"""''',

    "session_enterprise.py": '''"""
Enterprise Session and System Management Tools.

This module provides MCP tools and helpers for managing enterprise (Core+) sessions
and querying enterprise system status. Includes session creation, deletion, and
system status reporting.

MCP Tools (decorated with @mcp_server.tool()):
    - enterprise_systems_status: List all enterprise systems with status and configuration
    - session_enterprise_create: Create new enterprise session with resource allocation
    - session_enterprise_delete: Delete enterprise session and clean up resources

Helper Functions:
    - _check_session_limits: Validate session count against configured limits
    - _generate_session_name_if_none: Auto-generate session names from username
    - _check_session_id_available: Verify session ID is not already in use
    - _resolve_session_parameters: Resolve session parameters with defaults from config

Constants:
    - DEFAULT_MAX_CONCURRENT_SESSIONS: Default limit for concurrent sessions per system
    - DEFAULT_ENGINE: Default engine type for enterprise sessions
    - DEFAULT_TIMEOUT_SECONDS: Default timeout for session operations

Note:
    Requires deephaven_enterprise package for full functionality.
    Falls back gracefully when enterprise features unavailable.
"""''',

    "session.py": '''"""
Session Listing and Details Tools.

This module provides MCP tools for listing all sessions (community and enterprise)
and retrieving detailed information about specific sessions including status,
versions, and configuration.

MCP Tools (decorated with @mcp_server.tool()):
    - sessions_list: List all sessions with basic metadata
    - session_details: Get comprehensive details about a specific session

Helper Functions:
    - _get_session_liveness_info: Extract liveness/health status from session
    - _get_session_property: Safely retrieve session property with error handling
    - _get_session_programming_language: Determine session programming language
    - _get_session_versions: Extract version information from session

Constants:
    - DEFAULT_PROGRAMMING_LANGUAGE: Default when language cannot be determined

Dependencies:
    - Uses shared.py functions for session access and validation
    - Supports both community and enterprise sessions
"""''',

    "table.py": '''"""
Session Table Operation Tools.

This module provides MCP tools for table operations including schema retrieval,
table listing, and data extraction with flexible formatting options.

MCP Tools (decorated with @mcp_server.tool()):
    - session_tables_schema: Retrieve full metadata schemas for tables
    - session_tables_list: Get list of table names (lightweight)
    - session_table_data: Extract table data with formatting options

Helper Functions:
    - _build_table_data_response: Format table data for API responses

Constants:
    - ESTIMATED_BYTES_PER_CELL: Conservative estimate for response size calculation

Dependencies:
    - Uses shared.py for session access, response size validation, and formatting
    - deephaven_mcp.formatters for data formatting
    - deephaven_mcp.queries for table operations

Safety Features:
    - Response size estimation to prevent memory issues
    - Configurable row limits for large tables
    - Multiple output formats (JSON, CSV, etc.)
"""''',

    "script.py": '''"""
Script Execution and Package Management Tools.

This module provides MCP tools for executing scripts on Deephaven sessions and
querying installed Python packages.

MCP Tools (decorated with @mcp_server.tool()):
    - session_script_run: Execute script on specified session
    - session_pip_list: List all installed pip packages with versions

Features:
    - Script Execution:
      - Supports Python and Groovy scripts
      - Language-specific validation
      - Comprehensive error reporting
    
    - Package Management:
      - Uses importlib.metadata for accurate package information
      - Returns structured list of package names and versions
      - Compatible with all package sources

Dependencies:
    - Uses shared.py for session access
    - deephaven_mcp.queries for script execution

Note:
    Script execution permissions and language support depend on session configuration.
"""''',

    "catalog.py": '''"""
Catalog Operation Tools (Enterprise Core+ Only).

This module provides MCP tools for querying the Deephaven catalog in enterprise
sessions, including table listing, namespace discovery, schema retrieval, and
data sampling.

MCP Tools (decorated with @mcp_server.tool()):
    - catalog_tables_list: List catalog tables with optional filtering
    - catalog_namespaces_list: Retrieve distinct namespaces for data discovery
    - catalog_tables_schema: Get full schemas for catalog tables
    - catalog_table_sample: Sample data from catalog tables

Helper Functions:
    - _get_catalog_data: Unified catalog query handler with filtering

Features:
    - Namespace-based filtering
    - Table name pattern matching
    - Custom filter expressions
    - Safe data sampling with row limits
    - Multiple output formats

Dependencies:
    - Uses shared.py for session access, validation, and formatting
    - Requires CorePlusSession (enterprise) for all operations
    - deephaven_mcp.queries for catalog operations

Note:
    All catalog operations require enterprise (Core+) sessions.
    Returns appropriate errors for community sessions.
"""''',

    "pq.py": '''"""
Persistent Query (PQ) Management Tools (Enterprise Core+ Only).

This module provides comprehensive MCP tools and helpers for managing persistent
queries in enterprise Deephaven systems, including CRUD operations, lifecycle
management, and status monitoring.

MCP Tools (decorated with @mcp_server.tool()):
    Query Discovery and Information:
    - pq_name_to_id: Convert PQ name to canonical pq_id
    - pq_list: List all PQs on enterprise system
    - pq_details: Get detailed PQ information
    
    Lifecycle Management:
    - pq_create: Create new persistent query
    - pq_modify: Modify existing PQ configuration
    - pq_delete: Permanently delete PQ
    - pq_start: Start PQ and wait for RUNNING state
    - pq_stop: Stop one or more running PQs (bulk supported)
    - pq_restart: Restart one or more stopped PQs (bulk supported)

Helper Functions:
    ID and Validation:
    - _parse_pq_id: Parse pq_id into system name and serial number
    - _make_pq_id: Construct canonical pq_id from components
    - _validate_timeout: Validate timeout parameters
    - _validate_max_concurrent: Validate concurrency limits
    - _validate_and_parse_pq_ids: Batch PQ ID validation
    
    Formatting:
    - _format_pq_config: Format PQ configuration for API responses
    - _format_pq_state: Format PQ state information
    - _format_pq_replicas: Format replica state information
    - _format_pq_spares: Format spare worker information
    - _format_named_string_list: Format protobuf NamedStringList
    - _format_column_definition: Format table column definitions
    - _format_table_definition: Format table definitions
    - _format_exported_object_info: Format exported object metadata
    - _format_worker_protocol: Format worker protocol information
    - _format_connection_details: Format connection details
    - _format_exception_details: Format exception information
    
    Operations:
    - _setup_batch_pq_operation: Setup and validation for batch operations
    - _convert_restart_users_to_enum: Convert restart policy string to enum
    - _add_session_id_if_running: Add session ID to running PQ info
    - _normalize_programming_language: Normalize language strings
    
    Configuration:
    - _apply_pq_config_simple_fields: Apply simple field modifications
    - _apply_pq_config_list_fields: Apply list field modifications
    - _apply_pq_config_modifications: Apply all configuration changes

Features:
    - Batch operations (start, stop, restart multiple PQs)
    - Fire-and-forget mode (timeout=0)
    - Comprehensive status reporting
    - Resource allocation management
    - Script and configuration updates

Dependencies:
    - Requires CorePlusSession and CorePlusControllerClient
    - Uses shared.py for system configuration access
    - CorePlusQuerySerial, CorePlusQueryConfig, CorePlusQueryState types

Note:
    All PQ operations require enterprise (Core+) sessions and proper permissions.
    PQ IDs follow format: "system_name:serial_number" (e.g., "prod:42")
"""''',

    "session_community.py": '''"""
Community Session Management Tools.

This module provides MCP tools and helpers for creating and managing dynamically
launched Deephaven Community sessions via Docker or pip installation.

MCP Tools (decorated with @mcp_server.tool()):
    - session_community_create: Create new community session (Docker or pip)
    - session_community_delete: Delete community session and clean up resources
    - session_community_credentials: Retrieve connection credentials (security sensitive)

Helper Functions:
    Configuration:
    - _get_session_creation_config: Retrieve session creation config from ConfigManager
    - _check_session_limit: Validate against max dynamic sessions limit
    
    Validation:
    - _validate_launch_method_params: Validate launch method and parameters
    - _resolve_docker_image: Determine Docker image based on language
    - _resolve_community_session_parameters: Resolve all session parameters
    - _normalize_auth_type: Normalize and validate authentication type
    - _resolve_auth_token: Resolve authentication token (generate or use provided)
    
    Lifecycle:
    - _register_session_manager: Create and register session manager in registry
    - _launch_process_and_wait_for_ready: Launch process and wait for health check
    - _build_success_response: Build standardized success response
    - _log_auto_generated_credentials: Log credentials for user access

Constants:
    - DEFAULT_LAUNCH_METHOD: Default launch method (docker)
    - DEFAULT_AUTH_TYPE: Default authentication handler
    - DEFAULT_DOCKER_IMAGE_PYTHON: Docker image for Python sessions
    - DEFAULT_DOCKER_IMAGE_GROOVY: Docker image for Groovy sessions
    - DEFAULT_HEAP_SIZE_GB: Default JVM heap size
    - DEFAULT_STARTUP_TIMEOUT_SECONDS: Max time to wait for startup
    - DEFAULT_STARTUP_CHECK_INTERVAL_SECONDS: Time between health checks
    - DEFAULT_STARTUP_RETRIES: Number of connection attempts per health check

Features:
    - Docker container launches
    - Pip-based launches (venv isolation)
    - Auto-generated authentication tokens
    - Configurable resource allocation
    - Health check monitoring
    - Port management
    - Process cleanup

Dependencies:
    - DockerLaunchedSession, PythonLaunchedSession: Launch mechanisms
    - DynamicCommunitySessionManager: Session lifecycle management
    - InstanceTracker: Resource tracking
    - find_available_port, generate_auth_token: Utility functions

Security Note:
    session_community_credentials retrieval is disabled by default and requires
    explicit configuration: security.community.credential_retrieval_mode
"""''',
}


def main():
    """Add pydocs to all module files."""
    tools_dir = Path(__file__).parent.parent / "src/deephaven_mcp/mcp_systems_server/_tools"
    
    for module_name, pydoc in MODULE_PYDOCS.items():
        module_path = tools_dir / module_name
        
        if not module_path.exists():
            print(f"WARNING: {module_name} not found!")
            continue
        
        # Read current content
        with open(module_path) as f:
            content = f.read()
        
        # Replace TODO placeholder with actual pydoc
        if f'"""TODO: Add comprehensive module docstring for {module_name}"""' in content:
            new_content = content.replace(
                f'"""TODO: Add comprehensive module docstring for {module_name}"""',
                pydoc
            )
            
            # Write back
            with open(module_path, 'w') as f:
                f.write(new_content)
            
            print(f"✓ Added pydoc to {module_name}")
        else:
            print(f"✗ Could not find TODO placeholder in {module_name}")


if __name__ == "__main__":
    main()
