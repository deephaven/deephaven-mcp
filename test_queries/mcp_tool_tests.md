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

Pre-cleanup (idempotent): Call session_community_delete with session_name="mcp-test-create-delete". Ignore any error — this just ensures a clean starting state.

1. Call session_community_create with session_name="mcp-test-create-delete", launch_method="python", auth_type="anonymous". Verify success==true. Record the session_id.

2. Call sessions_list. Verify success==true. Verify "community:dynamic:mcp-test-create-delete" appears in the list.

3. Call session_details with session_id="community:dynamic:mcp-test-create-delete" and attempt_to_connect=true. Verify success==true.

4. CLEANUP: Call session_community_delete with session_name="mcp-test-create-delete". Verify success==true.

5. Call sessions_list again. Verify "community:dynamic:mcp-test-create-delete" is no longer present.

Report: PASS/FAIL for each numbered step with a one-line explanation.
```

---

## Prompt 3: Script Execution

```text
Test script execution on a Deephaven Community session. Do the following steps in order and report results.

Pre-cleanup (idempotent): Call session_community_delete with session_name="mcp-test-script". Ignore any error — this just ensures a clean starting state.

Setup: Call session_community_create with session_name="mcp-test-script", launch_method="python", auth_type="anonymous". Verify success==true before proceeding.

1. Call session_script_run with session_id="community:dynamic:mcp-test-script" and this script:
   from deephaven import new_table
   from deephaven.column import int_col, string_col
   hello_table = new_table([int_col("ID", [1, 2, 3]), string_col("Msg", ["hello", "world", "test"])])
   Verify success==true.

2. Call session_script_run with session_id="community:dynamic:mcp-test-script" and script="this is not valid python !!!". Verify success==false (expected error).

3. Call session_script_run with session_id="community:dynamic:mcp-test-script" with neither script nor script_path provided. Verify success==false and error mentions "Must provide either script or script_path".

CLEANUP: Call session_community_delete with session_name="mcp-test-script". Verify success==true.

Report: PASS/FAIL for each numbered step with a one-line explanation.
```

---

## Prompt 4: Table Listing and Schema

```text
Test table listing and schema retrieval on a Deephaven Community session. Do the following steps in order and report results.

Pre-cleanup (idempotent): Call session_community_delete with session_name="mcp-test-tables". Ignore any error — this just ensures a clean starting state.

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

CLEANUP: Call session_community_delete with session_name="mcp-test-tables". Verify success==true.

Report: PASS/FAIL for each numbered step with a one-line explanation.
```

---

## Prompt 5: Table Data Retrieval

```text
Test reading table data from a Deephaven Community session. Do the following steps in order and report results.

Pre-cleanup (idempotent): Call session_community_delete with session_name="mcp-test-data". Ignore any error — this just ensures a clean starting state.

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

CLEANUP: Call session_community_delete with session_name="mcp-test-data". Verify success==true.

Report: PASS/FAIL for each numbered step with a one-line explanation.
```

---

## Prompt 6: Pip Package Listing

```text
Test pip package listing on a Deephaven Community session. Do the following steps in order and report results.

Pre-cleanup (idempotent): Call session_community_delete with session_name="mcp-test-pip". Ignore any error — this just ensures a clean starting state.

Setup: Call session_community_create with session_name="mcp-test-pip", launch_method="python", auth_type="anonymous". Verify success==true before proceeding.

1. Call session_pip_list with session_id="community:dynamic:mcp-test-pip". Verify success==true. Verify the result list is non-empty. Check whether "deephaven-server" or "deephaven" appears in the package list. Record the total number of packages returned.

2. Call session_pip_list with session_id="community:dynamic:nonexistent-pip-xyz". Verify success==false (expected error).

CLEANUP: Call session_community_delete with session_name="mcp-test-pip". Verify success==true.

Report: PASS/FAIL for each numbered step with a one-line explanation.
```

---

## Prompt 7: Configuration Reload

```text
Test the mcp_reload tool. Do the following steps in order and report results.

Pre-cleanup (idempotent): Call session_community_delete with session_name="mcp-test-reload". Ignore any error — this just ensures a clean starting state.

Setup: Call session_community_create with session_name="mcp-test-reload", launch_method="python", auth_type="anonymous". Verify success==true. Call sessions_list and confirm "community:dynamic:mcp-test-reload" is present.

1. Call mcp_reload. Verify success==true.

2. Call sessions_list. Verify success==true. Verify "community:dynamic:mcp-test-reload" is NO LONGER in the list (mcp_reload clears all dynamic sessions — this is expected behavior).

Note: mcp_reload serves as the cleanup for this test since it removes all dynamic sessions.

Report: PASS/FAIL for each numbered step with a one-line explanation.
```
