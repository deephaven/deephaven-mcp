# Deephaven MCP Tool Tests

Each prompt below is self-contained. Paste one at a time into Claude Desktop. Each prompt creates what it needs, runs its tests, cleans up, and reports results.

---

## Prompt 1: Session Discovery

```text
Test the Deephaven MCP session discovery tools. Do the following steps in order and report results.

1. Call sessions_list. Verify success==true. Record how many sessions are returned and their types (COMMUNITY/ENTERPRISE).

2. For each session returned (up to 10), call session_details with attempt_to_connect=false. Verify success==true for each. Record the session type and source.

3. Call session_details with session_id="community:dynamic:nonexistent-xyz" and attempt_to_connect=false. Verify success==false (this is expected — it is a negative test).

Report: PASS/FAIL for each step with a one-line explanation.
```

---

## Prompt 2: Dynamic Session Create and Delete

```text
Test creating and deleting a dynamic Deephaven Community session. Do the following steps in order and report results.

Pre-cleanup (idempotent): Call session_community_delete with session_id="community:dynamic:mcp-test-create-delete". Ignore any error — this just ensures a clean starting state.

1. Call session_community_create with session_name="mcp-test-create-delete", launch_method="python", auth_type="anonymous". Verify success==true. Record the session_id.

2. Call sessions_list. Verify success==true. Verify "community:dynamic:mcp-test-create-delete" appears in the list.

3. Call session_details with session_id="community:dynamic:mcp-test-create-delete" and attempt_to_connect=true. Verify success==true.

4. CLEANUP: Call session_community_delete with session_id="community:dynamic:mcp-test-create-delete". Verify success==true.

5. Call sessions_list again. Verify "community:dynamic:mcp-test-create-delete" is no longer present.

Report: PASS/FAIL for each numbered step with a one-line explanation.
```

---

## Prompt 3: Script Execution

```text
Test script execution on a Deephaven Community session. Do the following steps in order and report results.

Pre-cleanup (idempotent): Call session_community_delete with session_id="community:dynamic:mcp-test-script". Ignore any error — this just ensures a clean starting state.

Setup: Call session_community_create with session_name="mcp-test-script", launch_method="python", auth_type="anonymous". Verify success==true before proceeding.

1. Call session_script_run with session_id="community:dynamic:mcp-test-script" and this script:
   from deephaven import new_table
   from deephaven.column import int_col, string_col
   hello_table = new_table([int_col("ID", [1, 2, 3]), string_col("Msg", ["hello", "world", "test"])])
   Verify success==true.

2. Call session_script_run with session_id="community:dynamic:mcp-test-script" and script="this is not valid python !!!". Verify success==false (expected error).

3. Call session_script_run with session_id="community:dynamic:mcp-test-script" with neither script nor script_path provided. Verify success==false and error mentions "Must provide either script or script_path".

CLEANUP: Call session_community_delete with session_id="community:dynamic:mcp-test-script". Verify success==true.

Report: PASS/FAIL for each numbered step with a one-line explanation.
```

---

## Prompt 4: Table Listing and Schema

```text
Test table listing and schema retrieval on a Deephaven Community session. Do the following steps in order and report results.

Pre-cleanup (idempotent): Call session_community_delete with session_id="community:dynamic:mcp-test-tables". Ignore any error — this just ensures a clean starting state.

Setup: 
- Call session_community_create with session_name="mcp-test-tables", launch_method="python", auth_type="anonymous". Verify success==true.
- Call session_script_run with session_id="community:dynamic:mcp-test-tables" and this script:
  from deephaven import new_table
  from deephaven.column import int_col, string_col, double_col
  trades = new_table([int_col("TradeID", [1, 2, 3]), string_col("Symbol", ["AAPL", "GOOG", "MSFT"]), double_col("Price", [150.0, 2800.0, 300.0])])
  orders = new_table([int_col("OrderID", [10, 20]), string_col("Side", ["BUY", "SELL"])])
  Verify success==true before proceeding.

1. Call session_tables_list with session_id="community:dynamic:mcp-test-tables". Verify success==true. Verify both "trades" and "orders" appear in table_names.

2. Call session_tables_schema with session_id="community:dynamic:mcp-test-tables" and no table_names (fetch all). Verify success==true. Verify schemas for "trades" and "orders" are present. For "trades", verify columns TradeID (int), Symbol (string), Price (double) appear.

3. Call session_tables_schema with session_id="community:dynamic:mcp-test-tables" and table_names=["trades"]. Verify success==true and only the "trades" schema is returned.

4. Call session_tables_schema with session_id="community:dynamic:mcp-test-tables" and table_names=["does_not_exist_xyz"]. Note whether the overall success is true or false and whether the individual table entry shows an error.

CLEANUP: Call session_community_delete with session_id="community:dynamic:mcp-test-tables". Verify success==true.

Report: PASS/FAIL for each numbered step with a one-line explanation.
```

---

## Prompt 5: Table Data Retrieval

```text
Test reading table data from a Deephaven Community session. Do the following steps in order and report results.

Pre-cleanup (idempotent): Call session_community_delete with session_id="community:dynamic:mcp-test-data". Ignore any error — this just ensures a clean starting state.

Setup:
- Call session_community_create with session_name="mcp-test-data", launch_method="python", auth_type="anonymous". Verify success==true.
- Call session_script_run with session_id="community:dynamic:mcp-test-data" and this script:
  from deephaven import new_table
  from deephaven.column import int_col, string_col, double_col
  results = new_table([int_col("ID", [1, 2, 3, 4, 5]), string_col("Name", ["Alice", "Bob", "Charlie", "Diana", "Eve"]), double_col("Score", [95.5, 87.3, 92.1, 78.9, 88.0])])
  Verify success==true before proceeding.

1. Call session_table_data with session_id="community:dynamic:mcp-test-data" and table_name="results". Verify success==true. Verify 5 rows are returned with correct ID, Name, and Score values.

2. Call session_table_data with session_id="community:dynamic:mcp-test-data", table_name="results", and max_rows=2. Verify success==true. Verify only 2 rows are returned.

3. Call session_table_data with session_id="community:dynamic:mcp-test-data" and table_name="no_such_table_xyz". Verify success==false (expected error).

CLEANUP: Call session_community_delete with session_id="community:dynamic:mcp-test-data". Verify success==true.

Report: PASS/FAIL for each numbered step with a one-line explanation.
```

---

## Prompt 6: Pip Package Listing

```text
Test pip package listing on a Deephaven Community session. Do the following steps in order and report results.

Pre-cleanup (idempotent): Call session_community_delete with session_id="community:dynamic:mcp-test-pip". Ignore any error — this just ensures a clean starting state.

Setup: Call session_community_create with session_name="mcp-test-pip", launch_method="python", auth_type="anonymous". Verify success==true before proceeding.

1. Call session_pip_list with session_id="community:dynamic:mcp-test-pip". Verify success==true. Verify the result list is non-empty. Check whether "deephaven-server" or "deephaven" appears in the package list. Record the total number of packages returned.

2. Call session_pip_list with session_id="community:dynamic:nonexistent-pip-xyz". Verify success==false (expected error).

CLEANUP: Call session_community_delete with session_id="community:dynamic:mcp-test-pip". Verify success==true.

Report: PASS/FAIL for each numbered step with a one-line explanation.
```

---

## Prompt 7: Configuration Reload

```text
Test the mcp_reload tool. Do the following steps in order and report results.

Pre-cleanup (idempotent): Call session_community_delete with session_id="community:dynamic:mcp-test-reload". Ignore any error — this just ensures a clean starting state.

Setup: Call session_community_create with session_name="mcp-test-reload", launch_method="python", auth_type="anonymous". Verify success==true. Call sessions_list and confirm "community:dynamic:mcp-test-reload" is present.

1. Call mcp_reload. Verify success==true.

2. Call sessions_list. Verify success==true. Verify "community:dynamic:mcp-test-reload" is NO LONGER in the list (mcp_reload clears all dynamic sessions — this is expected behavior).

Note: mcp_reload serves as the cleanup for this test since it removes all dynamic sessions.

Report: PASS/FAIL for each numbered step with a one-line explanation.
```

---

## Prompt 8: Community Session Credentials

```text
Test the session_community_credentials tool. Do the following steps in order and report results.

Pre-cleanup (idempotent): Call session_community_delete with session_id="community:dynamic:mcp-test-credentials". Ignore any error — this just ensures a clean starting state.

Setup: Call session_community_create with session_name="mcp-test-credentials", launch_method="python", auth_type="anonymous". Verify success==true before proceeding.

1. Call session_community_credentials with session_id="community:dynamic:mcp-test-credentials".
   - If success==false and the error mentions "disabled" or "mode='none'": this is the EXPECTED result when credential retrieval is disabled (the default). Record as PASS and note that testing the success path requires enabling credential_retrieval_mode in the server config.
   - If success==true: verify auth_type is "ANONYMOUS", auth_token is empty string "", and connection_url_with_auth equals connection_url (no auth appended). Record as PASS.

2. Call session_community_credentials with session_id="community:dynamic:nonexistent-xyz". Verify success==false (negative test — session does not exist).

3. Call session_community_credentials with session_id="enterprise:prod:some-session". Verify success==false and error mentions this tool only works for community sessions (negative test — wrong session type).

CLEANUP: Call session_community_delete with session_id="community:dynamic:mcp-test-credentials". Verify success==true.

Report: PASS/FAIL for each numbered step with a one-line explanation.
```

---

## Enterprise Server Tests

> **Prerequisites**: Prompts 9–16 require the enterprise MCP server (`dh-mcp-enterprise-server`) to be running and configured with a valid DHE connection. Prompts 11, 12, 14, and 15 additionally require the enterprise server to be connected to a live DHE system (liveness_status == "ONLINE"). Run Prompt 9 first to verify system status before proceeding.

---

## Prompt 9: Enterprise System Status

```text
Test the enterprise_systems_status tool on the enterprise MCP server. Do the following steps in order and report results.

1. Call enterprise_systems_status with attempt_to_connect=false. Verify success==true. Record the system name (from 'systems[0].name'), liveness_status, and is_alive. Note: liveness_status may be "OFFLINE" if no prior connection exists — that is acceptable for this step.

2. Call enterprise_systems_status with attempt_to_connect=true. Verify success==true. Record whether liveness_status changed. A status of "ONLINE" means the system is healthy and subsequent tests can proceed. A status of "OFFLINE" or "UNAUTHORIZED" is a PASS for this tool test (it behaved correctly) but means Prompts 11, 12, 14, 15 will not be fully testable.

3. Verify the response structure: confirm 'systems' is a non-empty list, and that each entry contains 'name', 'liveness_status', 'is_alive', and 'config' fields. Verify that if the config contains credentials, sensitive fields such as 'password' are redacted (replaced with something like "REDACTED").

Report: PASS/FAIL for each numbered step with a one-line explanation. Include the system name and liveness_status from step 2.
```

---

## Prompt 10: Enterprise Session Discovery

```text
Test session discovery tools on the enterprise MCP server. Do the following steps in order and report results.

1. Call sessions_list. Verify success==true. Record how many sessions are returned and how many have type "ENTERPRISE". Note any enterprise session IDs (format: "enterprise:{system_name}:{session_name}").

2. For each enterprise session returned (up to 5), call session_details with that session_id and attempt_to_connect=false. Verify success==true for each. Record the liveness_status for each.

3. Call session_details with session_id="enterprise:nonexistent-system-xyz:fake-session" and attempt_to_connect=false. Verify success==false (negative test — system name does not exist on this server).

Report: PASS/FAIL for each numbered step with a one-line explanation. Include the count of enterprise sessions found in step 1.
```

---

## Prompt 11: Enterprise Session Create and Delete

```text
Test creating and deleting enterprise sessions. Requires the enterprise system to be ONLINE (verify with Prompt 9 first). Do the following steps in order and report results.

Pre-cleanup (idempotent): Call sessions_list. If a session named "mcp-test-ent-create-delete" appears in the list, note its session_id and call session_enterprise_delete with that session_id. Ignore any error.

1. Call session_enterprise_create with session_name="mcp-test-ent-create-delete". Verify success==true. Record the returned session_id (format: "enterprise:{system_name}:mcp-test-ent-create-delete") and system_name.

2. Call sessions_list. Verify success==true. Verify the session_id from step 1 appears in the list.

3. Call session_details with the session_id from step 1 and attempt_to_connect=true. Verify success==true.

4. Call session_enterprise_create with session_name="mcp-test-ent-create-delete" again (duplicate). Verify success==false (negative test — duplicate session name must be rejected).

5. CLEANUP: Call session_enterprise_delete with the session_id from step 1. Verify success==true.

6. Call sessions_list again. Verify the session_id from step 1 is no longer present.

7. Call session_enterprise_delete with the session_id from step 1 again (already deleted). Verify success==false (negative test — deleting a nonexistent session must be rejected).

Report: PASS/FAIL for each numbered step with a one-line explanation. Include the session_id from step 1.
```

---

## Prompt 12: Enterprise Shared Tools (Script, Tables, Data, Pip)

```text
Test session_script_run, session_tables_list, session_tables_schema, session_table_data, and session_pip_list via an enterprise session. Requires the enterprise system to be ONLINE (verify with Prompt 9 first). Do the following steps in order and report results.

Pre-cleanup (idempotent): Call sessions_list. If a session named "mcp-test-ent-shared" is present, note its session_id and call session_enterprise_delete with it. Ignore any error.

Setup: Call session_enterprise_create with session_name="mcp-test-ent-shared". Verify success==true. Record the session_id.

1. Call session_script_run with the session_id from Setup and this script:
   from deephaven import new_table
   from deephaven.column import int_col, string_col
   ent_test = new_table([int_col("ID", [1, 2, 3]), string_col("Label", ["alpha", "beta", "gamma"])])
   Verify success==true.

2. Call session_tables_list with the session_id from Setup. Verify success==true. Verify "ent_test" appears in the table list.

3. Call session_tables_schema with the session_id from Setup and table_names=["ent_test"]. Verify success==true. Verify columns ID (int) and Label (string) are present.

4. Call session_table_data with the session_id from Setup and table_name="ent_test". Verify success==true. Verify 3 rows are returned with correct values.

5. Call session_table_data with the session_id from Setup, table_name="ent_test", and max_rows=2. Verify success==true. Verify exactly 2 rows are returned.

6. Call session_pip_list with the session_id from Setup. Verify success==true. Verify the result list is non-empty.

7. Call session_script_run with the session_id from Setup and script="this is not valid python!!!". Verify success==false (negative test).

CLEANUP: Call session_enterprise_delete with the session_id from Setup. Verify success==true.

Report: PASS/FAIL for each numbered step with a one-line explanation.
```

---

## Prompt 13: PQ Discovery (Read-Only)

```text
Test read-only PQ discovery tools on the enterprise MCP server. These are non-destructive operations safe to run on any enterprise system. Do the following steps in order and report results.

1. Call pq_list. Verify success==true. Record the total number of PQs returned. If zero PQs are returned, note this and skip steps 3 and 4 (no PQs to inspect).

2. Call pq_details with pq_id="enterprise:nonexistent-system-xyz:999999". Verify success==false (negative test — invalid pq_id format or nonexistent system).

3. (Skip if step 1 returned zero PQs.) From the pq_list result, pick the first PQ. Record its pq_id (format: "enterprise:{system_name}:{serial}") and name. Call pq_details with that pq_id. Verify success==true. Verify the response includes a 'config' dict with at minimum 'name', 'serial', and 'script_language' fields.

4. (Skip if step 1 returned zero PQs.) Using the name recorded in step 3, call pq_name_to_id with pq_name set to that name. Verify success==true. Verify the returned pq_id matches the one from step 3.

Report: PASS/FAIL for each numbered step (or SKIPPED with reason). Include the total PQ count from step 1.
```

---

## Prompt 14: PQ Lifecycle (Create, Modify, Start, Stop, Restart, Delete)

```text
Test the full PQ lifecycle. Requires the enterprise system to be ONLINE (verify with Prompt 9 first).
WARNING: This creates and starts a real worker process on your DHE system. Do the following steps in order and report results.

Pre-cleanup (idempotent): Call pq_list. If any PQ named "mcp-test-pq-lifecycle" is found, record its pq_id and call pq_delete with that pq_id. Ignore any error.

1. Call pq_create with pq_name="mcp-test-pq-lifecycle", heap_size_gb=1, script_body="t = None". Verify success==true. Record the returned pq_id (format: "enterprise:{system_name}:{serial}").

2. Call pq_details with the pq_id from step 1. Verify success==true. Note the current state in the response.

3. Call pq_modify with the pq_id from step 1, setting script_body="t = 42". Verify success==true. Call pq_details again to confirm script_body changed to "t = 42".

4. Call pq_start with pq_id set to the pq_id from step 1 (a single ID, not a list). Verify success==true. Note any per-item result in the response.

5. Call pq_stop with pq_id set to the pq_id from step 1. Verify success==true.

6. Call pq_restart with pq_id set to the pq_id from step 1. Verify success==true.

7. CLEANUP: Call pq_delete with pq_id set to the pq_id from step 1. Verify success==true.

8. Call pq_details with the pq_id from step 1. Verify success==false (negative test — deleted PQ must not be found).

Report: PASS/FAIL for each numbered step with a one-line explanation. Include the pq_id from step 1.
```

---

## Prompt 15: Enterprise Catalog Tools

```text
Test the enterprise catalog tools. These are read-only operations. The catalog may be empty on development systems — steps that require catalog data are conditional. Requires the enterprise system to be ONLINE and an enterprise session to be available. Do the following steps in order and report results.

Setup:
- Call sessions_list. If at least one enterprise session exists, pick one and use its session_id. If no enterprise sessions exist, call session_enterprise_create with session_name="mcp-test-catalog" and record the returned session_id.
- Record whether you created a new session (true/false) — this determines cleanup.

1. Call catalog_namespaces_list with the session_id from Setup. Verify success==true. Record the count of namespaces returned (may be 0 if no catalog is configured — that is acceptable).

2. Call catalog_tables_list with the session_id from Setup. Verify success==true. Record the count of catalog tables returned (may be 0).

3. (Skip if step 2 returned zero tables.) Pick any table from the catalog_tables_list result. Record its namespace and table name separately. Call catalog_tables_schema with the session_id from Setup, namespace set to the recorded namespace, and table_names set to a list containing just the recorded table name. Verify success==true. Note the column count in the schema.

4. (Skip if step 2 returned zero tables.) Call catalog_table_sample with the session_id from Setup, namespace and table_name from step 3, and max_rows=3. Verify success==true. Note the number of rows returned (may be 0 for empty tables).

5. Call catalog_tables_schema with the session_id from Setup, namespace="nonexistent_ns_xyz", and table_names=["nonexistent_table_xyz"]. Note whether success==true (with per-table error info) or success==false. Either behavior is acceptable — record which occurred.

CLEANUP: If you created a new session in Setup, call session_enterprise_delete with that session_id. Verify success==true.

Report: PASS/FAIL for each numbered step (or SKIPPED with reason). Include namespace count and table count from steps 1 and 2.
```

---

## Prompt 16: Enterprise mcp_reload

```text
Test the mcp_reload tool on the enterprise MCP server. Do the following steps in order and report results.

Setup: Call sessions_list. Record the session IDs of any enterprise sessions currently listed.

1. Call mcp_reload. Verify success==true.

2. Call enterprise_systems_status with attempt_to_connect=false. Verify success==true. Record the liveness_status — the enterprise system should still be visible after a reload.

3. Call sessions_list. Verify success==true. Compare the session list to Setup. Note any changes. (Unlike the community server, mcp_reload on the enterprise server does not necessarily clear sessions — record what changed, if anything.)

Report: PASS/FAIL for each numbered step with a one-line explanation. Note any session list changes between Setup and step 3.
```
