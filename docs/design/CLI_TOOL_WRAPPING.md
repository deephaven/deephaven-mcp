# Design: wrapping MCP tools as first-class `dh-mcp` CLI commands

**Audience:** contributors and architects working on the `dh-mcp` CLI.
**Scope:** *why* the CLI wraps MCP tools the way it does — the noun
structure, the type-scoping rule, the wrapper categories, output shaping
and diagnostics, and the schema-drift contract. Per-command reference
(flags, output fields, examples) lives in [`docs/CLI.md`](../CLI.md), not
here.

## Table of Contents

- [Why a wrapper layer exists](#why-a-wrapper-layer-exists)
- [Three planes](#three-planes)
- [Four authentication concepts, not one](#four-authentication-concepts-not-one)
- [Type scoping: never a subgroup](#type-scoping-never-a-subgroup)
- [Wrapper categories](#wrapper-categories)
- [Output shaping and diagnostics](#output-shaping-and-diagnostics)
- [The drift contract](#the-drift-contract)
- [Command catalog](#command-catalog)

## Why a wrapper layer exists

`dh-mcp tool call <name> --arg key=value` can already invoke any MCP
tool, but it gives users and AI agents no typed flags, no
tab-completion, no per-argument validation, and no shaped output, and
it forces JSON-encoding tricks (`--arg n=42`). The wrapper layer turns
each MCP tool into a friendly noun-verb command with real flags, help,
and structured output. `tool call` remains as the raw escape hatch.

**Completeness is decided per noun group:** if a noun is exposed, every
one of its verbs is wrapped. A `pq` group that could list/start/stop but
not create would be surprising. The drift risk this creates is handled
by automation (the drift test below), not by withholding commands.

## Three planes

CLI operations fall into three planes; the noun structure keeps them
distinct.

- **Lifecycle** — the `daemon` group (start/stop/status/restart/reset/
  logs). The local daemon process.
- **Static config** — the on-disk JSON5 tree (`server.json`, `cli.json`,
  `community/`, `enterprise/`, and all credentials / PSK / TLS).
  Read-only today (`config show` / `config validate`). Future setup
  growth (add a system, set credentials, rotate the PSK) belongs here,
  under `config` — never on a runtime noun.
- **Runtime** — MCP tools executed against a running daemon: session
  lifecycle, tables, scripts, catalog, persistent queries, and
  credential *retrieval*. This is what the wrapper layer covers.

A consequence worth stating: the runtime `session` noun stays lean
permanently, because setup/auth growth lands on the static-config plane.
`session credentials` is a runtime *token fetch* (a live browser-login
URL for a running Community session), not credential management, so it
belongs with the runtime sessions.

## Four authentication concepts, not one

There is no single "auth" concept to model as one noun. Four orthogonal
mechanisms exist, each owned by the entity it secures:

| Concept | Secures | Owner / where it lives |
|---------|---------|------------------------|
| PSK | client → daemon (HTTP gate) | the daemon (`server.json`) |
| Community credentials | daemon → community worker | a community session def (`community/sessions/*.json`) |
| Enterprise credentials | daemon → enterprise system | an enterprise system def (`enterprise/systems/*.json`) |
| Browser-login retrieval | hand a human a live session URL+token | a running session (runtime, security-gated) |

So the CLI has no monolithic `auth` noun. Future auth/setup operations
attach to their owning entity (PSK under `daemon`/`config`, system
credentials under `config`), and live browser-login retrieval stays a
runtime `session` verb.

## Type scoping: never a subgroup

Every session is addressed by a fully qualified id `type:system:name`
where `type` is `community` or `enterprise`. A system name uniquely
determines its type (community is the single fixed system `community`;
enterprise systems are individually named). The CLI therefore never
branches the command tree by type. Type is carried three ways:

- **`--system` at `create`** — the only verb with no id yet. The chosen
  system's type selects the underlying tool; flags are grouped by type
  in help and a mismatched flag errors.
- **the id's `type:` prefix** — at every verb that takes an existing id
  (`show`, `delete`, `exec`, `pip-list`, `credentials`, `url`, `open`,
  and all table verbs). Dispatchers route on the prefix.
- **group-level documentation** — for wholly single-type resources
  (`pq`, `catalog` are top-level and documented "Enterprise (Core+)
  only").

This is resource-first, like every mature CLI (kubectl, docker, gh,
aws, gcloud). Type-first (`community …` / `enterprise …` at the top)
was rejected because it duplicates the genuinely shared resources
(`table`, `session list/show/delete/exec`) under both prefixes and
destroys the cross-type `session list`.

**Scripts are input, not a resource.** A script has no server-side
identity — nothing to `list` or `show` — so it never gets a noun. Code
rides verbs and flags (`session exec --script/--script-path`,
`pq create --script-body/--script-body-path/--git-script-path`), the way
`kubectl apply -f` takes a file without a `file` noun.

## Path locality

Every path-valued flag resolves in exactly one of two places, and the
command surface must make that place unmistakable:

- **The CLI machine.** The flag names a local file. The CLI reads it
  itself (`read_local_script` in `_wrapping.py`) and forwards the
  *contents* to the tool, so a relative path resolves against the
  invoking shell's working directory, `-` means stdin, and an unreadable
  file fails fast with `file_read_failed` (exit 2) before any daemon
  round trip. Examples: `session exec --script-path`,
  `pq create --script-body-path`.
- **The server.** The flag is an identifier in a server-side namespace
  and is forwarded verbatim — e.g. `pq --git-script-path` (a path into
  the controller's Git-backed script repository, read at PQ start) or
  `pq --python-venv` (a venv name configured on the Enterprise server).
  The flag name and help must say so; a bare name like `--script-path`
  on a server-side flag reads as a local file and was renamed for
  exactly that reason.

The daemon is deliberately **never** a file-resolution locus for the CLI
surface: it is a background process whose working directory and lifetime
are unrelated to the user's shell, so "a file readable by the daemon" is
a footgun, not a feature. (The MCP tools themselves keep server-side
path parameters like `session_script_run`'s `script_path` for agents
that are co-resident with the server; the CLI wrappers simply do not
forward them.)

## Wrapper categories

Every wrapper is one of five shapes. The `_cli-tool-wrapping` skill
maps each to a code template; the first four route through the shared
flow in
[`cli/_commands/_wrapping.py`](../../src/deephaven_mcp/cli/_commands/_wrapping.py).
`call_for_payload` runs the fetch half
(`acquire` → `call_tool` → `tool_payload` → `require_success`) and feeds
one of two top-level helpers, both of which render via `echo_payload`:
`call_and_echo` (emit the tool's whole success payload) or
`call_and_echo_field` (emit one field — e.g. a `list` verb's array — and
re-surface a partial-result diagnostic on stderr; see *Output shaping*).

1. **Passthrough** — one tool; flags map to tool args; the payload is
   rendered. Example: `system list` → `list_systems`.
2. **Id-router** — one verb whose `session_id` prefix selects among
   several tools. Example: `session delete` → `session_community_delete`
   or `session_enterprise_delete`.
3. **System-router** — one verb whose `--system` type selects among
   several tools. Example: `session create` →
   `session_community_create` or `session_enterprise_create`.
4. **Client-side composite** — wraps a tool for data, then adds local
   behavior. Example: `session open` fetches the authed URL via
   `session_community_credentials`, then launches a browser.
5. **Direct-URL** — one tool on a remote MCP server named by a
   `cli.json` URL; the local daemon is not involved and is never
   started, so the acquire half (and its error codes) does not apply.
   The wrapper builds its own `McpClient` from config and reuses the
   render half (`tool_payload` → `require_success` → `echo_payload`).
   Example: `docs ask` → `docs_chat` at `docs.url`. The drift test
   still verifies the `wraps_tool` binding; the wrapper's help lists
   the transport error codes (`mcp_request_failed`,
   `mcp_request_timeout`) instead of the acquire codes
   (`_DIRECT_URL_WRAPPERS` in `tests/cli/test_tool_wrapper_drift.py`).

### Worked example (passthrough)

```python
@system.command("list", output_spec=_OUTPUT_LIST, wraps_tool="list_systems", help=...)
@click.pass_obj
@run_async
async def system_list(runtime: Runtime) -> None:
    await call_and_echo_field(
        runtime, "list_systems", retry_command="dh-mcp system list",
        arguments={}, field="systems", default=[],
    )
```

## Output shaping and diagnostics

Presentation is owned by a single knob, `-o human|json|json-pretty|yaml`
(`format_output`); a wrapper never branches on output mode. It chooses
only *what value* to render:

- **Whole envelope** (`call_and_echo`) — the tool's full success payload.
- **One field** (`call_and_echo_field`) — a bare value (e.g. a `list`
  verb's array), so `-o json` stays `jq`-friendly.

Two consequences shaped the helpers:

- **Data-returning tools take a `format` (data-encoding) parameter.** The
  wrapper requests the structured `json-row` encoding internally and never
  exposes it as a CLI flag — `-o` is the one presentation knob, so the
  tool's encoding is not a competing second one.
- **A partial-but-successful result must not be dropped.** Tools whose
  result spans systems (`sessions_list`, `enterprise_systems_status`)
  attach a `partial_result` block (`phase` / `detail` / optional per-system
  `errors`) when discovery is incomplete; truncating tools
  (`catalog_namespaces_list`) flag a row-capped result with
  `is_complete: false`. Whole-envelope verbs carry these in
  stdout; field-shaping verbs would discard them, so `call_and_echo_field`
  re-surfaces both as a **stderr** warning (`_warn_if_incomplete`), keeping
  stdout clean for piping.

## The drift contract

A wrapper redeclares, as click flags, the parameters of the MCP tool it
fronts. That binding can silently rot when a tool's signature changes.
Two mechanisms keep them honest.

**Declared binding.** Each wrapping `HelpfulCommand` sets:

- `wraps_tool: str` — the single tool a leaf wraps, or
- `wraps_tools: tuple[str, ...]` — the tools a dispatcher fronts;
- `intentionally_unsupported: frozenset[str]` — tool params the wrapper
  deliberately omits (an explicit allowlist, so an intentional subset is
  distinguishable from drift);
- `router_params: frozenset[str]` — dispatch-router flags that steer
  which tool runs but *are* a parameter of some wrapped tool (e.g.
  `system` on `session create`: it selects the backend and is the
  enterprise tool's required `system` arg). Exempt from the phantom
  check, but the drift test still asserts they are real tool params.
- `client_only_params: frozenset[str]` — flags that are not a parameter
  of any wrapped tool (e.g. `print_only` on `session open`, which only
  controls local browser launch). Exempt from the phantom check and from
  the real-param assertion. Use this — never `router_params` — for flags
  with no tool counterpart, so a stale `router_params` entry can't hide.

The binding is emitted in the `agents` manifest (`wraps`) so
`review-changes` and other tooling can read it without importing Python.

**Automated guard.** [`tests/cli/test_tool_wrapper_drift.py`](../../tests/cli/test_tool_wrapper_drift.py) builds every
tool's JSON schema in-process (registering the `_tools` modules on a
throwaway `FastMCP`), walks the live click tree for wrapper bindings,
and asserts per wrapper:

- *Drift:* every **required** tool parameter is a declared flag/argument
  or is listed in `intentionally_unsupported`.
- *Phantom:* every declared flag is a real parameter of at least one
  wrapped tool, or is a declared `router_param`.

When a tool signature changes, this test fails until the wrapper and
its binding are updated in the same change. The contract is reinforced
by skills (`cli-command-add` requires the binding; `review-changes`
runs the drift check on `_tools/**` or `cli/_commands/**` edits).

## Command catalog

| Group | Verbs → MCP tool(s) |
|-------|---------------------|
| `session` | `list`→`sessions_list`; `show`→`session_details`; `create --system`→`session_community_create`\|`session_enterprise_create`; `delete`→`session_community_delete`\|`session_enterprise_delete`; `exec`→`session_script_run`; `pip-list`→`session_pip_list`; `credentials`/`url`/`open`→`session_community_credentials` |
| `system` | `list`→`list_systems`; `status`→`enterprise_systems_status` |
| `table` | `list`→`session_tables_list`; `schema`→`session_table_schema`; `data`→`session_table_data` |
| `catalog` | (Enterprise only) `tables`→`catalog_tables_list`; `namespaces`→`catalog_namespaces_list`; `schema`→`catalog_table_schema`; `sample`→`catalog_table_sample` |
| `pq` | (Enterprise only) `list`/`details`/`create`/`modify`/`delete`/`start`/`stop`/`restart`/`name-to-id` → `pq_*` |
| `docs` | (Direct-URL; docs server at `docs.url`, no daemon) `ask`→`docs_chat`; `status`→ connectivity probe (no tool binding) |

Every group above is implemented, each complete (all of a noun's verbs
are wrapped). Any tool is still reachable directly via `dh-mcp tool call`
as the raw escape hatch.
