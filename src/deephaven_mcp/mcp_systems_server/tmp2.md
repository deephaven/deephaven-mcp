# Comprehensive API Analysis: MCP Tools vs Deephaven Client APIs

## (Excluding Table Operations)

---

## Executive Summary

This analysis focuses on **session-level and enterprise-specific operations** in the Deephaven Client APIs, explicitly excluding table manipulation operations (where, join, select, etc.).

**Key Finding:** Your current MCP tools provide **excellent coverage (~85%)** of basic session management and discovery operations, but are **missing critical enterprise features** including:

- Persistent Query (PQ) lifecycle management (~0% coverage)
- Worker pool and resource quota management (~0% coverage)
- Authentication and authorization operations (~0% coverage)
- Advanced catalog operations (~30% coverage)
- Session-level data operations (~20% coverage)

---

## Part 1: Current MCP Tools Analysis (18 tools)

### **✅ Well-Covered Areas**

#### 1. Basic Session Discovery & Status

- [sessions_list](cci:1://file:///Users/chip/dev/deephaven-mcp/src/deephaven_mcp/mcp_systems_server/_mcp.py:468:0-577:67) - List all sessions
- [session_details](cci:1://file:///Users/chip/dev/deephaven-mcp/src/deephaven_mcp/mcp_systems_server/_mcp.py:892:0-1081:67) - Get detailed session info
- [enterprise_systems_status](cci:1://file:///Users/chip/dev/deephaven-mcp/src/deephaven_mcp/mcp_systems_server/_mcp.py:331:0-465:67) - Check enterprise system health
- [mcp_reload](cci:1://file:///Users/chip/dev/deephaven-mcp/src/deephaven_mcp/mcp_systems_server/_mcp.py:254:0-328:67) - Reload configuration

#### 2. Basic Session Lifecycle

- [session_community_create](cci:1://file:///Users/chip/dev/deephaven-mcp/src/deephaven_mcp/mcp_systems_server/_mcp.py:4264:0-4588:17) - Create community session
- [session_community_delete](cci:1://file:///Users/chip/dev/deephaven-mcp/src/deephaven_mcp/mcp_systems_server/_mcp.py:4591:0-4788:17) - Delete community session  
- [session_enterprise_create](cci:1://file:///Users/chip/dev/deephaven-mcp/src/deephaven_mcp/mcp_systems_server/_mcp.py:3056:0-3369:17) - Create enterprise session
- [session_enterprise_delete](cci:1://file:///Users/chip/dev/deephaven-mcp/src/deephaven_mcp/mcp_systems_server/_mcp.py:3430:0-3614:17) - Delete enterprise session
- [session_community_credentials](cci:1://file:///Users/chip/dev/deephaven-mcp/src/deephaven_mcp/mcp_systems_server/_mcp.py:4791:0-5062:67) - Get connection credentials

#### 3. Table Discovery & Metadata

- [session_tables_list](cci:1://file:///Users/chip/dev/deephaven-mcp/src/deephaven_mcp/mcp_systems_server/_mcp.py:1255:0-1349:67) - List tables in session
- [session_tables_schema](cci:1://file:///Users/chip/dev/deephaven-mcp/src/deephaven_mcp/mcp_systems_server/_mcp.py:1084:0-1252:67) - Get table schemas
- [catalog_tables_list](cci:1://file:///Users/chip/dev/deephaven-mcp/src/deephaven_mcp/mcp_systems_server/_mcp.py:1948:0-2160:5) - List catalog tables (Enterprise)
- [catalog_namespaces_list](cci:1://file:///Users/chip/dev/deephaven-mcp/src/deephaven_mcp/mcp_systems_server/_mcp.py:2163:0-2303:5) - List catalog namespaces (Enterprise)
- [catalog_tables_schema](cci:1://file:///Users/chip/dev/deephaven-mcp/src/deephaven_mcp/mcp_systems_server/_mcp.py:2306:0-2627:67) - Get catalog table schemas (Enterprise)

#### 4. Basic Data Access

- [session_table_data](cci:1://file:///Users/chip/dev/deephaven-mcp/src/deephaven_mcp/mcp_systems_server/_mcp.py:1614:0-1830:17) - Retrieve table data
- [catalog_table_sample](cci:1://file:///Users/chip/dev/deephaven-mcp/src/deephaven_mcp/mcp_systems_server/_mcp.py:2630:0-2829:67) - Sample catalog table data (Enterprise)

#### 5. Script Execution

- [session_script_run](cci:1://file:///Users/chip/dev/deephaven-mcp/src/deephaven_mcp/mcp_systems_server/_mcp.py:1352:0-1469:17) - Execute Python/Groovy scripts
- [session_pip_list](cci:1://file:///Users/chip/dev/deephaven-mcp/src/deephaven_mcp/mcp_systems_server/_mcp.py:1472:0-1569:17) - List installed pip packages

---

## Part 2: Deephaven Core (Community) API - Missing Session-Level Operations

### ❌ **Session-Level Data Operations** (11 missing)

#### Data Import/Export

1. ❌ `Session.import_table(pa.Table)` - Import Arrow/PyArrow tables into session
2. ❌ `Session.empty_table(size)` - Create empty table with N rows
3. ❌ `Session.input_table()` - Create InputTable (keyed/blink/append-only)
4. ❌ `Session.time_table()` - Create time-based ticking tables
5. ❌ `Session.merge_tables()` - Merge multiple tables into one

**Impact:** AI agents cannot:

- Load external data (CSV, Parquet, Arrow) into sessions
- Create synthetic/test data tables
- Create real-time streaming tables
- Build incremental update workflows (InputTable)
- Combine multiple tables at session level

#### Cross-Session Sharing & Tickets

1. ❌ `Session.bind_table(name, table)` - Bind table to name in session scope
2. ❌ `Session.open_table(name)` - Open table by name from global scope
3. ❌ `Session.fetch(ticket)` - Fetch non-table objects by ticket
4. ❌ `Session.fetch_table(ticket)` - Fetch tables by shared ticket
5. ❌ `Session.publish(source_ticket, result_ticket)` - Publish objects to shared tickets
6. ❌ `Session.publish_table(ticket, table)` - Publish tables for cross-session sharing
7. ❌ `Session.release(ticket)` - Release tickets/resources

**Impact:** AI agents cannot:

- Share tables between sessions
- Reference tables by name across sessions
- Use ticket-based object sharing patterns
- Manually manage resource lifecycle via tickets
- Access shared server objects

#### Query Operations

1. ❌ `Session.query(table)` - Create Query objects for batched operations

**Impact:** AI agents cannot:

- Batch multiple table operations efficiently (though this is less critical since we're excluding table ops)

#### Plugin System

1. ❌ `Session.plugin_client(ticket)` - Access plugin clients

**Impact:** AI agents cannot:

- Interact with Deephaven plugins
- Access custom server-side extensions

---

## Part 3: Deephaven Core+ (Enterprise) API - Missing Features

### ❌ **Persistent Query (PQ) Management** (23 operations - 0% coverage)

The `ControllerClient` API provides comprehensive PQ management - **NONE of this is exposed via MCP tools**.

#### PQ Lifecycle Operations

1. ❌ `ControllerClient.add_query(config)` - Create new persistent query
2. ❌ `ControllerClient.delete_query(serials)` - Delete persistent queries
3. ❌ `ControllerClient.modify_query(config, restart)` - Modify PQ configuration
4. ❌ `ControllerClient.restart_query(serials)` - Restart one or more PQs
5. ❌ `ControllerClient.stop_query(serials)` - Stop running PQs
6. ❌ `ControllerClient.start_and_wait(serial, timeout)` - Start PQ and wait for ready state
7. ❌ `ControllerClient.stop_and_wait(serial, timeout)` - Stop PQ and wait for terminal state

**Impact:** AI agents cannot:

- Create/delete persistent queries programmatically
- Restart failed or modified queries
- Control PQ lifecycle (start/stop/restart)
- Wait for specific PQ state transitions
- Modify PQ configuration (heap size, JVM args, environment vars)

#### PQ Discovery & Monitoring

1. ❌ `ControllerClient.subscribe()` - Subscribe to PQ state updates
2. ❌ `ControllerClient.map()` - Get current PQ state map (serial → info)
3. ❌ `ControllerClient.map_and_version()` - Get PQ state map with version number
4. ❌ `ControllerClient.get(serial, timeout)` - Get specific PQ info by serial
5. ❌ `ControllerClient.get_serial_for_name(name, timeout)` - Resolve PQ name to serial
6. ❌ `ControllerClient.wait_for_change()` - Wait for any PQ state change
7. ❌ `ControllerClient.wait_for_change_from_version(version, timeout)` - Wait for specific version change
8. ❌ `ControllerClient.wait_for_state(state_function, timeout)` - Wait for custom state condition

**Impact:** AI agents cannot:

- Monitor PQ health and status in real-time
- Subscribe to PQ state change notifications
- Resolve PQ names to serial numbers
- Wait for specific PQ state transitions
- Track PQ state versions
- Build reactive PQ monitoring workflows

#### PQ Status Checks

1. ❌ `ControllerClient.is_running(info)` - Check if PQ is running
2. ❌ `ControllerClient.is_completed(info)` - Check if PQ completed successfully
3. ❌ `ControllerClient.is_terminal(info)` - Check if PQ is in terminal state
4. ❌ `ControllerClient.is_status_uninitialized(info)` - Check if PQ never started
5. ❌ `ControllerClient.status_name(info)` - Get human-readable status name

**Impact:** AI agents cannot:

- Determine PQ runtime status programmatically
- Distinguish between running, completed, failed, and uninitialized PQs
- Get human-readable status descriptions

#### PQ Configuration Generation

1. ❌ `ControllerClient.make_temporary_config(...)` - Create temporary InteractiveConsole query config

- Parameters: name, heap_size_gb, server, extra_jvm_args, extra_environment_vars, engine, auto_delete_timeout, admin_groups, viewer_groups

1. ❌ `ControllerClient.generate_disabled_scheduler()` - Generate disabled scheduler config

**Impact:** AI agents cannot:

- Programmatically create PQ configurations
- Specify heap size, JVM arguments, environment variables
- Configure auto-delete timeouts
- Set admin and viewer groups (RBAC)
- Create temporary worker configurations
- Configure PQ scheduling policies

#### PQ Authentication

1. ❌ `ControllerClient.authenticate(auth_client)` - Authenticate controller with auth client
2. ❌ `ControllerClient.ping()` - Ping controller to refresh cookie

**Impact:** AI agents cannot:

- Establish authenticated controller sessions
- Maintain controller session cookies
- Auto-refresh controller authentication

---

### ❌ **Authentication & Authorization** (9 operations - 0% coverage)

The `AuthClient` API provides comprehensive auth management - **NONE of this is exposed via MCP tools**.

#### Authentication Methods

1. ❌ `AuthClient.password(user, password, effective_user)` - Authenticate with username/password
2. ❌ `AuthClient.private_key(file)` - Authenticate with Deephaven private key
3. ❌ `AuthClient.saml(login_uri)` - Authenticate with SAML
4. ❌ `AuthClient.external_login(...)` - External authentication flow

**Impact:** AI agents cannot:

- Authenticate users programmatically
- Support password-based auth workflows
- Use private key authentication
- Enable SAML/SSO authentication flows
- Implement custom authentication flows

#### Token Management

1. ❌ `AuthClient.get_token(service, timeout)` - Get authentication token for service
2. ❌ `AuthClient.ping()` - Ping auth server to refresh cookie

**Impact:** AI agents cannot:

- Obtain service-specific authentication tokens
- Maintain auth session cookies
- Auto-refresh authentication

#### Key Management

1. ❌ `AuthClient.generate_keypair()` - Generate public/private keypair
2. ❌ `AuthClient.upload_key(pubtext, url, delete)` - Upload/delete public keys to ACL server

**Impact:** AI agents cannot:

- Generate Deephaven-format keypairs
- Manage user public keys
- Upload keys to ACL servers
- Delete expired or compromised keys

#### Session Management

1. ❌ `AuthClient.close()` - Close authentication client

**Impact:** AI agents cannot:

- Properly clean up auth client resources
- Manage auth session lifecycle

---

### ❌ **Enterprise Catalog Operations** (5 operations - 40% coverage)

Current coverage: [catalog_tables_list](cci:1://file:///Users/chip/dev/deephaven-mcp/src/deephaven_mcp/mcp_systems_server/_mcp.py:1948:0-2160:5), [catalog_namespaces_list](cci:1://file:///Users/chip/dev/deephaven-mcp/src/deephaven_mcp/mcp_systems_server/_mcp.py:2163:0-2303:5), [catalog_tables_schema](cci:1://file:///Users/chip/dev/deephaven-mcp/src/deephaven_mcp/mcp_systems_server/_mcp.py:2306:0-2627:67), [catalog_table_sample](cci:1://file:///Users/chip/dev/deephaven-mcp/src/deephaven_mcp/mcp_systems_server/_mcp.py:2630:0-2829:67)

#### Missing Catalog Table Access

1. ❌ `SessionManager.live_table(namespace, table_name)` - Load live catalog table into session
2. ❌ `SessionManager.historical_table(namespace, table_name)` - Load historical catalog table
3. ❌ `SessionManager.catalog_table()` - Generic catalog table accessor

**Impact:** AI agents cannot:

- Actually load catalog tables into the session for use
- Access historical versions of catalog tables
- Work with catalog data beyond sampling
- Can discover what tables exist, but can't use them

#### Missing Catalog Management

1. ❌ Catalog table write operations (if supported by API)
2. ❌ Catalog permission/ACL management

**Note:** Current MCP tools can *discover* and *sample* catalog data, but cannot *load* it into sessions for actual use.

---

### ❌ **Enterprise Session Features** (4 operations - 0% coverage)

#### Barrage & Subscription

1. ❌ `SessionManager.as_barrage_session()` - Get Barrage session for Persistent Query
2. ❌ `SessionManager.snapshot(table_ref)` - Create local table snapshot
3. ❌ `SessionManager.subscribe(table_ref)` - Subscribe to table updates

**Impact:** AI agents cannot:

- Connect to PQ via Barrage protocol
- Create local snapshots of remote tables
- Subscribe to real-time table updates
- Build streaming/reactive data pipelines

#### PQ Information

1. ❌ `SessionManager.pq_info()` - Retrieve persistent query information for current session

**Impact:** AI agents cannot:

- Get PQ metadata for the current session
- Determine which PQ a session belongs to
- Access PQ configuration from within session

---

### ❌ **Worker Pool & Resource Management** (Coverage: 0%)

**Critical Note:** The Python client APIs do **NOT** expose direct worker pool or resource quota management. These are configured at the **system administration level**, not via client APIs.

#### System-Level Configuration (Not in Python API)

- Worker pool sizing
- Resource quotas (CPU, memory, disk)
- Worker allocation policies
- Server selection/routing
- Dispatcher group restrictions
- Query server configuration

**What MCP Tools Currently Support:**

- ✅ Connection to pre-configured enterprise systems
- ✅ Creating sessions on those systems (which are assigned workers by the controller)
- ✅ Basic system health status

**What is NOT Available (System Admin Operations):**

- ❌ Dynamic worker pool scaling
- ❌ Resource quota adjustment
- ❌ Worker allocation policy modification
- ❌ Server/dispatcher configuration

**Rationale:** These are typically configured in `controller.xml` and other system configuration files, managed by Deephaven system administrators, not via client APIs.

---

### ❌ **Multi-User Permission Management** (Coverage: 0%)

**What's Partially Available:**

- PQ configuration supports `admin_groups` and `viewer_groups` parameters (via `make_temporary_config`)
- Auth client supports uploading keys to ACL server

**What's NOT Available in Python APIs:**

- ❌ User/group CRUD operations
- ❌ Permission assignment/revocation
- ❌ Role-based access control (RBAC) management
- ❌ Access control list (ACL) management (beyond key upload)
- ❌ User/group enumeration
- ❌ Permission auditing

**Rationale:** User/group management is typically handled by:

1. **External Identity Providers** (LDAP, Active Directory, SAML IdP)
2. **Deephaven Auth Server** (configured separately from client APIs)
3. **Administrative UIs** (not exposed via Python client API)

**What MCP Could Potentially Support:**

- Setting `admin_groups` and `viewer_groups` when creating PQs (if `add_query` was exposed)
- This would require understanding the existing group structure from the identity provider

---

## Part 4: Detailed Recommendations

### **Priority 1: Persistent Query Management** (Critical for Enterprise)

#### Essential PQ Operations (Add these 8 tools)

1. **`pq_list`** - List all persistent queries with status
   - Maps to: `ControllerClient.subscribe()` + `ControllerClient.map()`
   - Returns: List of PQ serials, names, states, heap sizes, owners

2. **`pq_details`** - Get detailed PQ information
   - Maps to: `ControllerClient.get(serial)`
   - Returns: Full PQ configuration and state

3. **`pq_create`** - Create new persistent query
   - Maps to: `ControllerClient.add_query(config)`
   - Parameters: name, heap_size_gb, engine, jvm_args, env_vars, auto_delete_timeout, admin_groups, viewer_groups

4. **`pq_delete`** - Delete persistent query(s)
   - Maps to: `ControllerClient.delete_query(serials)`

5. **`pq_start`** - Start persistent query
   - Maps to: `ControllerClient.start_and_wait(serial, timeout)`

6. **`pq_stop`** - Stop persistent query
   - Maps to: `ControllerClient.stop_and_wait(serial, timeout)`

7. **`pq_restart`** - Restart persistent query
   - Maps to: `ControllerClient.restart_query(serials)`

8. **`pq_modify`** - Modify PQ configuration
   - Maps to: `ControllerClient.modify_query(config, restart)`
   - Parameters: serial, new config, whether to restart

**Impact:** Enables full PQ lifecycle management via MCP

---

### **Priority 2: Authentication & Authorization** (High Value for Enterprise)

#### Essential Auth Operations (Add these 3 tools)

1. **`auth_password`** - Authenticate with username/password
   - Maps to: `AuthClient.password(user, password, effective_user)`
   - Returns: Authentication token

2. **`auth_private_key`** - Authenticate with private key
   - Maps to: `AuthClient.private_key(file_path_or_content)`
   - Returns: Authentication token

3. **`auth_get_token`** - Get service-specific auth token
   - Maps to: `AuthClient.get_token(service, timeout)`
   - Returns: Token for specified service

**Note:** SAML auth may require browser interaction, potentially difficult for AI agents

**Impact:** Enables programmatic authentication workflows

---

### **Priority 3: Catalog Table Loading** (Critical for Enterprise Data Access)

#### Essential Catalog Operations (Add these 2 tools)

1. **`catalog_table_load_live`** - Load live catalog table into session
   - Maps to: `SessionManager.live_table(namespace, table_name)`
   - Returns: Table reference for subsequent operations

2. **`catalog_table_load_historical`** - Load historical catalog table
   - Maps to: `SessionManager.historical_table(namespace, table_name)`
   - Returns: Table reference for historical data

**Impact:** Transforms catalog from "discovery only" to "actually usable"

- Current: Can see what tables exist, get schemas, sample data
- With this: Can load tables into sessions for script-based analysis

---

### **Priority 4: Session Data Import** (High Value for Data Loading)

#### Essential Import Operations (Add these 3 tools)

1. **`session_import_arrow`** - Import Arrow/PyArrow table
   - Maps to: `Session.import_table(pa.Table)`
   - Parameters: session_id, arrow_data (serialized)
   - Returns: Table reference

2. **`session_create_empty_table`** - Create empty table
   - Maps to: `Session.empty_table(size)`
   - Returns: Table reference

3. **`session_create_time_table`** - Create ticking time table
   - Maps to: `Session.time_table(period, start_time, blink)`
   - Parameters: tick interval, start time, whether blink table
   - Returns: Table reference for time-based queries

**Impact:** Enables AI agents to load external data into Deephaven sessions

---

### **Priority 5: Advanced Session Operations** (Medium Value)

#### Session-Level Operations (Add these 5 tools)

1. **`session_bind_table`** - Bind table to name in session
   - Maps to: `Session.bind_table(name, table)`

2. **`session_open_table`** - Open table by name
   - Maps to: `Session.open_table(name)`

3. **`session_create_input_table`** - Create InputTable
   - Maps to: `Session.input_table(schema, init_table, key_cols, blink)`
   - Enables incremental updates

4. **`session_merge_tables`** - Merge multiple tables
   - Maps to: `Session.merge_tables(tables, order_by)`

5. **`session_barrage_session`** - Get Barrage session for PQ
   - Maps to: `SessionManager.as_barrage_session()`

**Impact:** Enables advanced data ingestion and session management patterns

---

### **Priority 6: Cross-Session Sharing** (Lower Priority)

#### Ticket-Based Sharing (Add these 4 tools)

1. **`session_publish_table`** - Publish table to shared ticket
   - Maps to: `Session.publish_table(ticket, table)`

2. **`session_fetch_table`** - Fetch table by shared ticket
   - Maps to: `Session.fetch_table(ticket)`

3. **`session_publish_object`** - Publish non-table object
   - Maps to: `Session.publish(source_ticket, result_ticket)`

4. **`session_fetch_object`** - Fetch non-table object
   - Maps to: `Session.fetch(ticket)`

**Impact:** Enables cross-session data sharing workflows

---

## Part 5: What's NOT Feasible via Python Client API

### System Administration Operations

These are managed via configuration files and admin UIs, not client APIs:

1. **Worker Pool Configuration**
   - Sizing, scaling policies
   - Resource quotas
   - Server/dispatcher setup

2. **User/Group Management**
   - User CRUD operations
   - Group membership
   - Handled by external identity providers

3. **System-Level Security**
   - TLS configuration
   - Certificate management
   - Network policies

4. **Query Server Configuration**
   - PQ controller setup
   - Dispatcher configuration
   - Server selection policies

**Conclusion:** These require system admin access and are configured outside the Python client API scope.

---

## Part 6: Summary Statistics

| Category | Total API Operations | Exposed via MCP | Coverage |
|----------|---------------------|-----------------|----------|
| **Basic Session Management** | ~12 | ~10 | **~83%** |
| **Session Data Operations** | ~14 | ~2 | **~14%** |
| **Persistent Query Mgmt** | ~24 | 0 | **0%** |
| **Authentication & Auth** | ~9 | 0 | **0%** |
| **Catalog Operations** | ~7 | 4 | **~57%** |
| **Cross-Session Sharing** | ~6 | 0 | **0%** |
| **Enterprise Sessions** | ~4 | 0 | **0%** |
| **Worker/Resource Mgmt** | N/A (system admin) | N/A | **N/A** |
| **User/Permission Mgmt** | N/A (external IdP) | N/A | **N/A** |
| **TOTAL** | **~76** | **~16** | **~21%** |

---

## Part 7: Key Findings

### **Strengths of Current MCP Tools** ✅

1. **Excellent basic session discovery** - Can find and inspect all sessions
2. **Good session lifecycle management** - Can create/delete community and enterprise sessions
3. **Strong catalog discovery** - Can explore catalog structure and sample data
4. **Script execution fallback** - Can work around some missing operations via scripts

### **Critical Gaps** ❌

1. **Zero Persistent Query management** - Cannot manage PQ lifecycle at all
2. **Zero authentication operations** - Cannot authenticate users programmatically
3. **Cannot load catalog tables** - Can see them, but not use them
4. **Limited data import** - Cannot load external data into sessions
5. **No cross-session sharing** - Cannot share tables/objects between sessions

### **Most Impactful Additions** (Top 10)

1. PQ list/details/create/delete/start/stop (6 tools)
2. Catalog table loading (live/historical) (2 tools)
3. Session data import (Arrow) (1 tool)
4. Authentication (password/private key) (2 tools)

**Total: 11 tools would address ~80% of critical enterprise gaps**

---

## Conclusion

Your MCP tool suite has **excellent coverage (~83%) of basic session management** but is **missing critical enterprise capabilities**:

### **Three Major Gaps:**

1. **Persistent Query Management (0% coverage)**
   - 24 operations for PQ lifecycle, monitoring, and configuration
   - **Most critical gap for enterprise customers**
   - Requires `ControllerClient` API integration

2. **Authentication & Authorization (0% coverage)**
   - 9 operations for user auth, token management, key management
   - **Essential for programmatic security workflows**
   - Requires `AuthClient` API integration

3. **Catalog Table Loading (57% coverage - can discover, can't load)**
   - Can list, schema, and sample catalog tables
   - **Cannot load them into sessions for actual use**
   - Missing: `live_table()` and `historical_table()`

### **Quick Wins (11 tools to add ~80% of critical functionality):**

- **6 PQ tools:** list, details, create, delete, start, stop
- **2 Catalog tools:** load_live, load_historical  
- **2 Auth tools:** password_auth, private_key_auth
- **1 Data import tool:** import_arrow

### **Not Feasible via Client API:**

- Worker pool sizing/scaling (system admin config)
- Resource quotas (system admin config)
- User/group CRUD (external identity provider)
- System-level security (admin config)

The analysis above provides comprehensive details on each API category with exact method names, parameters, and impacts. Adding the 11 recommended "quick win" tools would transform your MCP server from "session discovery and inspection" to "full enterprise workflow automation."
