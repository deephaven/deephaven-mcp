# Output Shaping for MCP Tools and `dhcli`

## Problem

Collection tools can return more rows and columns than a caller needs. Detail
tools can return deep objects with many null or empty values. Agents then spend
tokens receiving and searching data that the server could have narrowed before
serializing it.

This proposal adds a consistent output-shaping API:

| Capability | Purpose |
| --- | --- |
| `match` | Keep collection rows that satisfy simple predicates. |
| `fields` | Return only selected fields, including nested detail fields. |
| `prune_empty` | Remove null and empty values from an otherwise selected result. |
| `max_rows` | Bound a collection response and report whether it was truncated. |

The first pass covers every in-memory list the servers already return — `pq
list`, `sessions list`, `list systems`, `pip list`, and the catalog listings —
plus `pq details`. The API is designed for later adoption by the remaining
detail and table-backed tools.

## Design goals

The API must be easy for an AI to generate and easy for a human to verify. Those
are compatible goals: a small, regular grammar lowers generation errors, while
readable inputs and explicit feedback make results inspectable.

| Goal | Design response |
| --- | --- |
| One obvious spelling per intent | Repeated `--match` means AND; `or` within one expression means OR. No aliases such as `AND` or `&&`. |
| Reliable structured calls | Repeated strings use a non-null JSON array schema, avoiding inspector-specific union editors. |
| Discoverability | Each tool documents its matchable and selectable fields, operators, examples, and parse errors. |
| Repairability | Invalid input returns the expected form and a concrete example. |
| Verifiability | Source order is preserved; projection changes only which fields are present; response metadata reports truncation and unmatched field paths. |
| Consistency | A tool uses the same vocabulary for `match` and `fields` where both apply. |

## API

### Collection tools

| Argument | Type | Default | Meaning |
| --- | --- | --- | --- |
| `match` | array of strings | `[]` | Output-layer row predicates. Separate entries are ANDed. Available on every collection tool. |
| `filters` | array of strings | `[]` | Engine-side Deephaven Query Language where-clauses. Offered only by tools whose rows come from a live Deephaven table (e.g. `catalog_tables_list`); absent otherwise. |
| `fields` | array of strings | `[]` | Fields to retain from each returned row. |
| `max_rows` | integer or null | `null` | Maximum returned rows; `null` is unbounded. |
| `prune_empty` | boolean | `false` | Remove null and empty values before projection. |

`match` is universal; `filters` is conditional on a Deephaven backing and so is
not part of every collection tool's signature. Where both are present they
compose — `filters` at the source, then the output-shaping layer — as detailed
under [Why both `match` and `filters`](#why-both-match-and-filters). A
successful projected collection response includes `unmatched_fields: []` when
all requested paths matched at least one delivered row. It lists any requested
path that matches no delivered row. `is_complete: false` means `max_rows`
truncated the matched result.

### Detail tools

| Argument | Type | Default | Meaning |
| --- | --- | --- | --- |
| `fields` | array of strings | `[]` | Fields to retain from the detail object. |
| `prune_empty` | boolean | `false` | Remove null and empty values before projection. |

A successful projected detail response always retains its identity keys (`success`
and `id`) and includes `unmatched_fields`. An empty list confirms every
requested path was found in the delivered result; a non-empty list identifies
paths that did not produce output.

`dhcli` exposes array arguments as repeatable flags. `--fields` additionally
accepts comma-separated names:

```bash
dhcli pq list prod --match 'owner=user or owner=user2' --fields id,owner
dhcli pq details enterprise:prod:123 --fields config.owner,state_details.status
```

The CLI writes an `unmatched_fields` warning to stderr when the list is
non-empty. Structured tool output remains the source of truth; warnings do not
change the normal stdout payload.

## Match grammar

`match` is a lightweight, case-insensitive adaptation of Deephaven quick
filters for named fields. `filters` remains reserved for Deephaven Query
Language where-clauses that are evaluated by the engine.

| Form | Meaning | Example |
| --- | --- | --- |
| `field=value` | Value equals | `owner=user` |
| `field!=value` | Value does not equal | `status!=STOPPED` |
| `field~value` | Value contains | `name~report` |
| `field!~value` | Value does not contain | `name!~archived` |
| `*` in `=` or `!=` | Zero-or-more-character wildcard | `name=*Query` |
| `null` | Empty field | `owner=null` |
| `\` | Escape the next character | `name=\null` |
| `or` | OR terms in one entry | `owner=user or owner=user2` |

Separate `match` entries are ANDed. For example, `--match 'owner=user or
owner=user2' --match status=RUNNING` retains running PQs owned by either user.

### Why both `match` and `filters`

The two are complementary, not redundant, and several tools should carry both.

`match` is the universally available layer. It filters the dictionary rows a
tool has already built, so it works on every collection — including those with
no Deephaven table behind them (`sessions list`, `list systems`, `pip list`).
It is deliberately simple: a small, case-insensitive equality / contains /
wildcard grammar over named fields that an agent can generate without knowing a
tool's internals. Even where a `filters` backing does exist, `match` is the
cheaper API for the common case — narrowing a table-backed result to a handful
of rows without composing a Deephaven Query Language expression.

`filters` is the powerful layer, and it lives lower down. A tool exposes it only
when its rows come from a live Deephaven table, because the where-clauses are
Deephaven Query Language evaluated by the engine before the rows are ever
serialized — as `catalog_tables_list` already does, pushing `Namespace` /
`TableName` predicates (`contains`, `startsWith`, `matches`, `in`) to the
worker. That reach is the payoff: full DQL, applied at the source, over data
that may be far larger than any response. The cost is that it is more complex to
write, requires knowing the backing column names, and is simply unavailable on
any tool with no table behind it.

| | `match` | `filters` |
| --- | --- | --- |
| Evaluated by | Tool output layer | Deephaven engine |
| Grammar | This simple grammar | Deephaven Query Language |
| Availability | Every collection tool | Only table-backed tools |
| Cost to write | Low; equality / contains over field names | Higher; full DQL over backing columns |
| Applied | After the tool builds its rows | Before the rows are read from the source |

Offering both on a table-backed tool is not ambiguous: `filters` cuts the data
at the source, then `match` (and the rest of the output-shaping layer) narrows
whatever came back. `catalog_tables_list` keeps its engine-side `filters`
unchanged and gains the output-side capabilities on top.

## Scope and prior art

This layer reduces a payload; it does not transform it. Field selection follows
`google.protobuf.FieldMask` semantics (name a parent, keep its subtree), and
`match` is a compact filter string in the RSQL / Google AIP-160 family, kept
deliberately distinct from engine-side `filters` (Deephaven Query Language).

The simple grammar is intentionally not meant to do everything. Its job is to
cut down the data the MCP server sends — and to do so cheaply on the server,
since filtering a large result to a size a model can actually process quickly
matters more than supporting every possible filter or transform. Anything
beyond selection and narrowing is out of scope: a caller who needs exact
reshaping runs a downstream transform such as `jq` on the CLI. The server does
not grow an expression language.

## Field grammar

`fields` uses dotted paths with protobuf `FieldMask`-style semantics.

| Form | Result |
| --- | --- |
| `config` | Keep the entire `config` object. |
| `config.owner` | Keep only `config.owner`. |
| `config,config.owner` | Same as `config`; a parent supersedes descendants. |
| `replicas.status` | Keep `status` from every replica. |
| `weird\.key` | Match a literal dot in a key. |

Paths through arrays apply to every element. Array indexing and wildcard paths
are not supported. A bare field such as `name` is the flat projection case.

Projection operates after pruning. Consequently, `unmatched_fields` describes
what was absent from the response the caller actually received: a requested
empty value removed by `prune_empty` is reported as unmatched. This makes the
feedback usable without reconstructing the pre-pruned object.

Validation is intentionally per-tool:

- Tools with a closed, stable row vocabulary reject unknown paths. `pq list` is
  in this category.
- Detail tools and variable-shape responses omit unmatched paths and report them
  through `unmatched_fields`. This allows optional or version-specific fields
  without turning a partial projection into a failed request.

## First-pass scope

A survey of the systems server's collection and detail tools decides which get
the layer now and which wait. The dividing line is cost: every tool that already
builds an in-memory list of dictionaries can adopt the output-shaping layer
cheaply, so all of them are in the first pass. Tools whose shaping needs a
synthetic field, a nested-projection design review, or an engine refactor wait.

| Tool | Kind | Row / shape | First pass | Notes |
| --- | --- | --- | --- | --- |
| `pq_list` | collection | dict rows (`id`, `serial`, `name`, `status`, `status_category`, `owner`, `enabled`) | `match`, `fields`, `max_rows`, `prune_empty` | No `filters` yet — see the refactor note below. |
| `sessions_list` | collection | dict rows (session identity, `type`, `system`, `origin`) | `match`, `fields`, `max_rows`, `prune_empty` | Keeps its existing `type` / `system` / `origin` scoping arguments. |
| `list_systems` | collection | `{name, type}` | `match`, `fields`, `max_rows`, `prune_empty` | In-memory; no Deephaven backing. |
| `session_pip_list` | collection | `{package, version}` | `match`, `fields`, `max_rows`, `prune_empty` | In-memory; no Deephaven backing. |
| `catalog_tables_list` | collection | `{namespace, table_name}` | add `match`, `fields`, `prune_empty` | Retains its existing engine-side `filters` and `max_rows`. |
| `catalog_namespaces_list` | scalar collection | namespace strings | add `match` via synthetic `name` | Retains its existing `filters` and `max_rows`. |
| `session_tables_list` | scalar collection | table-name strings | `match`, `max_rows` via synthetic `name` | Scalar list; single documented `name` field. |
| `enterprise_systems_status` | collection | per-system records with optional diagnostics | `match`, `prune_empty`, `fields` | `prune_empty` drops absent diagnostic fields. |
| `pq_details` | detail | envelope with nested `config`, `state_details`, `replicas`, `spares` | `fields`, `prune_empty` | `prune_empty` already exists. |

Detail and table-data tools with a deeper output shape — `session_details`, the
schema tools, `session_table_data`, `catalog_table_sample` — stay out of the
first pass; they are the [follow-on candidates](#follow-on-candidates).

### `pq list`

Adds `match`, `fields`, `max_rows`, and `prune_empty` to the list of PQ
summaries. Matching happens server-side; then the result is bounded, pruned, and
projected. Its closed vocabulary is:

| Field | Meaning |
| --- | --- |
| `id` | Fully qualified PQ identifier. |
| `serial` | Controller serial number. |
| `name` | PQ name. |
| `status` | Current PQ state. |
| `status_category` | Stable state category. |
| `owner` | Owning user. |
| `enabled` | Whether the PQ is enabled. |

`pq list` gains no `filters` in this pass. It reads the controller's PQ snapshot
(`controller.map()`), an in-memory map with no engine behind it, so only the
output-side layer applies. A later refactor can add engine-side `filters`:
routing the listing through `connect_to_persistent_query` and a `PluginClient`
against the `WebClientData` query's `QueryInfo` table replaces the
un-filterable snapshot with a live Deephaven table, letting Deephaven Query
Language where-clauses run on the worker before any row is returned. The same
route generalizes to other controller-snapshot listings. It is out of scope
here; `match` covers the common narrowing case in the meantime.

### `pq details`

Adds `fields` alongside the existing `prune_empty` option. Projection applies to
the detail envelope, including nested `config`, `state_details`, `replicas`, and
`spares`; `success` and `id` are always retained.

### Other first-pass collections

`sessions list`, `list systems`, and `pip list` each build a plain list of
dictionaries, so they take the full output-shaping layer (`match`, `fields`,
`max_rows`, `prune_empty`) with the same helpers as `pq list`. `sessions list`
keeps its existing `type` / `system` / `origin` scoping arguments; those select
*which* sessions to enumerate, while `match` narrows the enumerated rows.

The catalog listings already carry engine-side `filters` and `max_rows`; this
pass adds the output-side capabilities on top — `match`, `fields`, and
`prune_empty` for `catalog_tables_list`, and `match` for the scalar
`catalog_namespaces_list`. The two scalar lists (`catalog_namespaces_list` and
`session_tables_list`) expose `match` through one documented synthetic field,
`name`, matched against each string element.

`enterprise_systems_status` returns one record per system with several optional
diagnostic fields; it takes `match` (e.g. narrow to unhealthy systems) and
`prune_empty` (drop the diagnostics that are absent on a healthy system).

## Follow-on candidates

With the in-memory lists handled in the first pass, the remaining candidates are
the deeper-shaped tools whose projection design warrants its own review:

| Capability | Candidates |
| --- | --- |
| Nested `fields` | `session details`, the schema tools (`session_table_schema`, `catalog_table_schema`), and `session_table_data` / `catalog_table_sample`. |
| `prune_empty` | `session details` and any detail result with optional diagnostics. |
| `max_rows` + `is_complete` | `session_table_data` and any other row-data tool still returning an unbounded result. |
| Engine-side `filters` | `pq_list`, once its listing is refactored onto a live `QueryInfo` table (see [`pq list`](#pq-list)). |

Catalog tools already use `filters` for engine-side Deephaven Query Language;
that behavior is unchanged and the first pass only adds the output-side layer
beside it.

## Implementation approach

1. Rename the existing quick-filter surface to `match` and expose it as
   `--match`; reserve `filters` for Deephaven engine filters.
2. Extract reusable matching, nested projection, pruning, and row-shaping
   helpers so tools share one set of semantics.
3. Apply the layer to the first-pass collections — `pq list`, `sessions list`,
   `list systems`, `pip list`, `enterprise_systems_status`, and the catalog
   listings (`match` / `fields` / `prune_empty` atop their existing `filters`)
   — and add nested `fields` to `pq details`.
4. Add `unmatched_fields` to every successful response that received `fields`.
   Populate it after pruning and projection; preserve requested-path order.
5. Update CLI help, machine-readable command metadata, and user documentation
   with the grammar tables and canonical examples.
6. Apply the same layer to the follow-on candidates only after their field
   vocabularies and output contracts are reviewed.

## Testing and verification

Tests cover:

- Every match operator, wildcard, escape, `null`, case-insensitivity, OR, and
  AND behavior.
- Flat and nested projection, parent/descendant precedence, arrays,
  escaped-dot keys, and empty projection.
- `unmatched_fields` for absent paths, partially present paths, and values
  removed by `prune_empty`.
- `pq list` ordering: match, bound, prune, project; truncation and strict
  unknown-field validation.
- The other first-pass collections (`sessions list`, `list systems`, `pip
  list`, catalog listings, `enterprise_systems_status`): the same shaping layer,
  the synthetic `name` field on the scalar lists, and `filters` composing with
  `match` on the catalog tools.
- `pq details` identity-key retention, nested paths, pruning, and unmatched
  path reporting.
- CLI forwarding, comma-separated fields, stderr warnings, tool-wrapper
  contract checks, help checks, and full per-file coverage.

Verification runs focused matching/projection/PQ/CLI tests, then precommit, then
the complete test suite. Smoke checks include a multi-owner `--match`, a nested
`pq details --fields` request, and a deliberately missing path that produces an
`unmatched_fields` report.

## Non-goals

Numeric comparison operators; matching detail objects; field-path wildcards or
array indexes; grouping syntax; server-side output transformation or expression
languages; replacing Deephaven engine `filters`; new configuration or
environment variables.
