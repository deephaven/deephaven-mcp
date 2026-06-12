---
name: _cli-tool-wrapping
description: Conventions for dh-mcp commands that wrap an MCP tool — wrapper categories, type scoping, and the wraps_tool drift contract — invoke when adding/editing a cli/_commands verb that fronts a tool; not for authoring MCP server tools (_mcp-module-organization)
user-invocable: false
---

# CLI tool-wrapping conventions

Runtime `dh-mcp` commands (`session`, `system`, `table`, `script`, `catalog`, `pq`) wrap MCP tools. This skill is the how; the *why* is [`docs/design/CLI_TOOL_WRAPPING.md`](../../../docs/design/CLI_TOOL_WRAPPING.md). `cli-command-add` loads it for any wrapper verb and supplies the general click/Pattern-B/error conventions; this skill adds the wrapper-specific concern. Apply `_cli-help-standards` for the help contract. It does **not** apply to `daemon`/`config` (not tool wrappers) or to MCP server tools themselves (`_mcp-module-organization`).

## The shared flow

Every wrapper routes through `cli/_commands/_wrapping.py`, never a hand-rolled daemon/MCP dance. Two top-level helpers cover the two output shapes; both surface a partial-but-successful result's diagnostic siblings (e.g. the `partial_result` block from enterprise-session discovery) to stderr so shaping never silently drops them:

- `call_and_echo(runtime, "<tool>", *, retry_command, arguments)` — whole-payload verb: emit the tool's success payload as-is on stdout (the envelope carries any diagnostics).
- `call_and_echo_field(runtime, "<tool>", *, retry_command, arguments, field, default)` — shaping verb: emit one payload field (e.g. a `list` verb's array) on stdout, and surface diagnostic siblings on stderr. Use this instead of hand-rolling `call_for_payload` + `echo_payload`, which would drop them.

```python
# Whole-payload verb (emit the tool's result as-is):
await call_and_echo(
    runtime, "<tool>", retry_command="dh-mcp <noun> <verb>", arguments=arguments
)

# Shaping verb (emit one field as a bare value):
await call_and_echo_field(
    runtime, "<tool>", retry_command="dh-mcp <noun> <verb>",
    arguments=arguments, field="<key>", default=[],
)
```

Both build on the lower-level fetch/render pieces — `call_for_payload` (acquire → `call_tool` → `tool_payload` → `require_success`, which raises exit-3 `tool_returned_error` on `success=False`) and `echo_payload` (the one place that reads `runtime.config.cli.output.format` and prints via `format_output`). Reach for those directly only when a command builds a bespoke value (e.g. `session credentials` assembles a dict from several fields). Shape the payload per command — but never branch on output mode; that is `format_output`'s job.

Data-returning tools take a `format` (data-encoding) parameter — do **not** expose it as a CLI flag. Request the structured `json-row` encoding in the argument dict and emit the full result envelope; `-o`/`format_output` owns all presentation (human mode renders the row list as an aligned table). The tool's `format` is not a second output knob.

Repeatable `KEY=VALUE` options parse their tokens with `parse_key_value(token, decode_json=...)`: `decode_json=True` JSON-decodes values (`--arg n=42` → `42`), `decode_json=False` keeps raw strings (`--env LOG=DEBUG`); a malformed token raises `arg_parse_error`.

## Help error/exit codes (single-sourced)

A wrapper's help error codes come from `wrapper_error_codes()` in `_wrapping.py`: `ErrorCode.TOOL_RETURNED_ERROR` plus the shared codes the `acquire` + `call_tool` flow raises. They are `ErrorCode` members, so `build_help` renders their text from the enum — never re-type a code's description (see `_cli-help-standards` §3).

- Default: `error_codes=wrapper_error_codes()`.
- Read-only tool that never reports failure (`system list`, `tool list`): `wrapper_error_codes(tool_error=False)` (drops `tool_returned_error`).
- Extra command-specific codes go first: `error_codes=(ErrorCode.ARG_PARSE_ERROR, *wrapper_error_codes())`.
- Exit codes: `(ExitCode.SUCCESS, ExitCode.USER_ERROR, ExitCode.TOOL_ERROR)`, or drop `TOOL_ERROR` when there is no tool-error path.

`test_wrapper_help_lists_acquire_error_codes` fails a wrapper whose help omits the shared acquire codes.

## The four wrapper categories

These describe how a verb *selects* its tool(s); the output shape (`call_and_echo` vs `call_and_echo_field`, from *The shared flow*) is chosen independently per command. A router picks its tool, then calls a helper with it — `tool = ...; await call_and_echo(runtime, tool, retry_command=..., arguments=...)`.

1. **Passthrough** — one tool; flags → tool args. Set `wraps_tool="..."`. Example: `system list`.
2. **Id-router** — one verb; the `session_id` prefix selects the tool. Set `wraps_tools=("...community...","...enterprise...")`. Parse the `type:system:name` prefix and dispatch.
3. **System-router** — one verb; the `--system` value's type selects the tool. Set `wraps_tools=(...)` and `router_params=frozenset({"system"})` (`--system` steers dispatch and is forwarded only to the branch that declares it). Group flags by type in help; reject a wrong-type flag.
4. **Client-side composite** — wrap a tool for data, then act locally (e.g. `session open` → `webbrowser.open`). Set `wraps_tool="..."`; surface only the input args. For browser/host actions, provide a `--print` (or headless/no-`DISPLAY`) fallback and raise a dedicated `ErrorCode` on failure — never hang.

## Type scoping: never a subgroup

`community`/`enterprise` is never a command subgroup. Carry it via: `--system` at `create`; the id's `type:` prefix at every verb that takes an id; and the group docstring for wholly-Enterprise nouns (`pq`, `catalog` open with "Enterprise (Core+) only"). A community-only verb (`session credentials`/`url`/`open`) stays flat and errors on an enterprise id.

## The drift contract (required)

Declare the tool binding on the command so `tests/cli/test_tool_wrapper_drift.py` can verify it:

- `wraps_tool` / `wraps_tools` — the tool(s) fronted.
- `intentionally_unsupported` — tool params you deliberately omit (allowlist; otherwise a required param you skip reads as drift).
- `router_params` — dispatch-router flags that steer which tool runs but *are* a param of some wrapped tool (e.g. `--system` on `session create`). Exempt from the phantom check; the drift test still asserts they are real tool params.
- `client_only_params` — flags that are *not* a param of any wrapped tool (e.g. `--print` on `session open`, which only controls local behavior). Use this — not `router_params` — for flags with no tool counterpart.

Name each click flag/argument exactly as the tool's parameter (snake_case) so the drift test joins them by name — e.g. the positional session id is `session_id`, and `--system` uses dest `system`. After adding or editing a wrapper, run the drift test:

```bash
uv run --extra test pytest tests/cli/test_tool_wrapper_drift.py -q
```

If a tool signature later changes, this test fails until the wrapper and its binding are updated in the same change.

## Testing

Per command, extend `tests/cli/_commands/test_<noun>.py`:

- **Assert the call.** A success case asserting the wrapper invoked the right tool name and argument dict — the test that matters most — plus the `success=False` → exit-3 path.
- **One structured mode, not three.** The `human`/`json`/`yaml` matrix is `format_output`'s own test; cover at least one structured mode here and leave the matrix to it.
- **Patch the I/O seam.** Mock `_wrapping.acquire` and `_wrapping.call_tool` (the primitives the helpers call), not the composed `call_and_echo`/`call_for_payload`. The real fetch/render/`success=False`→exit-3 flow then runs, so a single `call_tool` mock covers the tool name + argument dict, the rendered output, and the exit-3 path for every verb shape. The helpers are additionally unit-tested in `tests/cli/_commands/test__wrapping.py`.
- **Client-side composites** mock the side effect (e.g. `webbrowser.open`).
- **Coverage + integration.** Target 100% per-file coverage. Add an integration round-trip in `tests/cli/test__daemon_integration.py` for community-reachable verbs; Enterprise-only verbs have no CI fixture (unit + drift only).
