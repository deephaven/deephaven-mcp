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

The initial scope is `pq list` and `pq details`. The API is designed for later
adoption by other collection and detail tools.

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
| `match` | array of strings | `[]` | Row predicates. Separate entries are ANDed. |
| `fields` | array of strings | `[]` | Fields to retain from each returned row. |
| `max_rows` | integer or null | `null` | Maximum returned rows; `null` is unbounded. |
| `prune_empty` | boolean | `false` | Remove null and empty values before projection. |

A successful projected collection response includes `unmatched_fields: []` when
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

`filters` and `match` intentionally serve different layers:

| Argument | Evaluated by | Grammar | Example use |
| --- | --- | --- | --- |
| `filters` | Deephaven engine | Deephaven Query Language | Restrict a catalog table before it is read. |
| `match` | Tool output layer | This grammar | Narrow returned rows after a tool has built them. |

A tool may offer both where that distinction is useful. For example, catalog
tools can retain engine-side `filters` and add output-side `match` without
ambiguity.

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

## Initial scope

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

### `pq details`

Adds `fields` alongside the existing `prune_empty` option. Projection applies to
the detail envelope, including nested `config`, `state_details`, `replicas`, and
`spares`; `success` and `id` are always retained.

## Follow-on candidates

| Capability | Candidates |
| --- | --- |
| Nested `fields` | `session details`, catalog/table schema tools, `sessions list`, `system status`, catalog tables, and package lists. |
| `match` | `sessions list`, session table lists, package lists, systems, and status lists. |
| `prune_empty` | `session details` and system-status results with optional diagnostics. |
| `max_rows` + `is_complete` | Session lists, system lists, table lists, and package lists that are currently unbounded. |

Catalog tools already use `filters` for engine-side Deephaven Query Language.
That behavior remains unchanged; `match` is a separate output-side capability.
Scalar lists, such as table names or namespaces, can adopt `match` later using a
single documented synthetic field such as `name`.

## Implementation approach

1. Rename the existing quick-filter surface to `match` and expose it as
   `--match`; reserve `filters` for Deephaven engine filters.
2. Extract reusable matching, nested projection, pruning, and row-shaping
   helpers so tools share one set of semantics.
3. Apply the layer to `pq list` and add nested `fields` to `pq details`.
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
