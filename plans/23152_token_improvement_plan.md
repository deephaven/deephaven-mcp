# Output Shaping for MCP Tools and `dhcli`

## Table of Contents

- [Problem](#problem)
- [Design goals](#design-goals)
- [API](#api)
- [Match grammar](#match-grammar)
- [Scope and prior art](#scope-and-prior-art)
- [Field grammar](#field-grammar)
- [Pruning](#pruning)
- [First-pass scope](#first-pass-scope)
- [Follow-on candidates](#follow-on-candidates)
- [Implementation approach](#implementation-approach)
- [Testing and verification](#testing-and-verification)
- [Non-goals](#non-goals)

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
| Reliable structured calls | The new repeated-string args (`match`, `fields`) use a non-null JSON array schema, avoiding inspector-specific union editors; the pre-existing `filters` keeps its established nullable schema. |
| Discoverability | Each tool documents its matchable and selectable fields, operators, examples, and parse errors. |
| Repairability | Invalid input returns the expected form and a concrete example. |
| Verifiability | Source order is preserved; projection changes only which fields are present; response metadata reports truncation and unmatched field paths. |
| Consistency | A tool uses the same vocabulary for `match` and `fields` where both apply. |

## API

### Collection tools

| Argument | Type | Default | Meaning |
| --- | --- | --- | --- |
| `match` | array of strings | `[]` | Row predicates. Separate entries are ANDed. Available on every collection tool; compiled to engine-side `filters` and run on the server when the tool is table-backed, else evaluated in the output layer. |
| `filters` | array of strings or null | `null` | Engine-side Deephaven Query Language where-clauses. Offered only by tools whose rows come from a live Deephaven table (e.g. `catalog_tables_list`); absent otherwise. Keeps the existing catalog signature (`list[str] \| None`, default `null`) unchanged — the non-null-array convention below applies only to the new `match` / `fields`. |
| `fields` | array of strings | `[]` | Fields to retain from each returned row. Empty (the default) applies no projection: every field is preserved. Omission and an explicit `[]` are identical. |
| `max_rows` | positive integer or null | per-tool default | Maximum returned rows; must be a positive integer (zero or negative is rejected). Each tool's default is a conservative positive integer (see [First-pass scope](#first-pass-scope)); uncapped output is an explicit opt-in via `null`. |
| `prune_empty` | boolean | `false` | Recursively remove empty values (see [Pruning](#pruning)) before projection; booleans and numbers are always kept. |

`match` is universal; `filters` is conditional on a Deephaven backing and so is
not part of every collection tool's signature. Where both are present they
compose without ambiguity: `match` compiles to `filters`, so both run at the
source before `max_rows` bounds the result, and only then does the
output-shaping layer (`fields`, `prune_empty`) trim whatever came back —
detailed under [Why both `match` and `filters`](#why-both-match-and-filters). A
successful collection response that received a non-empty `fields` selection
includes `unmatched_fields: []` when all requested paths matched at least one
delivered row, and lists any requested path that matched none; an omitted or
explicitly empty `fields` is the no-projection case and carries no
`unmatched_fields`. `is_complete: false` means `max_rows` truncated the matched
result.

### Detail tools

| Argument | Type | Default | Meaning |
| --- | --- | --- | --- |
| `fields` | array of strings | `[]` | Fields to retain from the detail object. Empty (the default) applies no projection: the whole object is preserved. Omission and an explicit `[]` are identical. |
| `prune_empty` | boolean | `false` | Recursively remove empty values (see [Pruning](#pruning)) before projection; booleans and numbers are always kept. |

A successful detail response always retains `success` and the tool's existing
identity path (`pq_details` retains top-level `id`). When a non-empty `fields`
selection was supplied it also includes `unmatched_fields`: an empty list
confirms every requested path was found in the delivered result, a non-empty
list identifies paths that produced no output. An omitted or explicitly empty
`fields` is the no-projection case and carries no `unmatched_fields`.

`dhcli` exposes array arguments as repeatable flags. `--fields` additionally
accepts comma-separated names:

```bash
dhcli pq list prod --match 'owner=user or owner=user2' --fields id,owner
dhcli pq details enterprise:prod:123 --fields config.owner,state_details.status
```

The CLI writes an `unmatched_fields` warning to stderr when the list is
non-empty. Structured tool output remains the source of truth; warnings do not
change the normal stdout payload.

`--max-rows` takes a positive integer to cap the result, or the literal `all`
for the tool's uncapped mode — the CLI spelling of the MCP `max_rows: null`,
mirroring `docker logs --tail all`. A shared parser preserves all three states
and does not fall through to Click's integer parsing: omission causes the wrapper
to omit `max_rows` (selecting the tool's default), `all` explicitly forwards
`{"max_rows": null}`, and a positive integer forwards that integer. Zero,
negatives, and any other value raise `CliError` with `arg_parse_error` (exit
`2`) rather than Click's unstructured `UsageError`. Every wrapper that exposes
`max_rows` carries the flag with matching help and a test for each path.

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
| `field>value`, `field>=value` | Greater than (or equal); numeric fields only | `serial>=1000` |
| `field<value`, `field<=value` | Less than (or equal); numeric fields only | `serial<50` |
| `*` in `=` or `!=` | Zero-or-more-character wildcard | `name=*Query` |
| `null` | Null field | `owner=null` |
| `\` | Escape the next character | `name=\null` |
| `or` | OR terms in one entry | `owner=user or owner=user2` |

Separate `match` entries are ANDed. For example, `--match 'owner=user or
owner=user2' --match status=RUNNING` retains running PQs owned by either user.

This shape is **conjunctive normal form (CNF)** — an AND of OR-clauses, where
each `--match` entry is one OR-clause and separate entries are ANDed. CNF is the
general form for any boolean filter written as a flat AND of ORs with no nesting.
Restricting the language to it is what makes it robust: there is exactly one
shape and one precedence, so no parentheses, no ambiguity, and every predicate
parses and compiles the same way.

A closed numeric range is therefore two ANDed predicates (`--match serial>10
--match serial<50`), and a union is `or` (`serial<10 or serial>90`). `match`
does not adopt Deephaven quick filters' `&&` / `||`: AND is the repetition of
`--match` and OR is the `or` keyword, so `&&` / `||` would be redundant second
spellings, and mixing `&&` with `or` in one entry would demand the precedence
and grouping this grammar omits. Reach for `filters` when you need engine-native
`&&` / `||` or grouping.

Every tool documents a closed match vocabulary — the field names it accepts on
the left of a predicate. A predicate naming a field outside that vocabulary
(`owenr=user`) is rejected with an error that echoes the allowed field names; it <!-- codespell:ignore owenr -->
is never silently treated as a no-match. This keeps a typo distinguishable from
a valid empty result and satisfies the repairability goal. For a tool that also
supports `fields`, the match vocabulary is the same set of names it exposes
there; the two scalar lists (`catalog_namespaces_list`, `session_tables_list`)
expose only the synthetic `name` to `match` and offer no `fields`.

### Typed fields and operator validity

Each matchable field carries a declared type in the tool's vocabulary — string,
integer, or boolean (an enum is a string). That one type table drives both the
output-layer matcher and the DQL compiler, so the two cannot disagree about a
predicate.

- **Coercion, not stringification.** The literal on the right is parsed to the
  field's declared type: an integer field parses `serial=12345` as an integer, a
  boolean field accepts `enabled=true` / `enabled=false` (case-insensitive), and
  a string field takes the literal verbatim and compares case-insensitively.
  Comparison then happens in the native type — the matcher never stringifies a
  typed field. A literal that will not parse to the field's type (`serial=abc`,
  `enabled=maybe`) is rejected with the same shape of repair error as an unknown
  field.
- **One literal, both layers.** From that coerced value the compiler emits the
  matching DQL literal — a backtick string, a bare integer, `true` / `false`, or
  `isNull(Field)` for `null` — and the output-layer matcher applies the identical
  typed comparison. Neither side stringifies.
- **Operator validity is per type.** Equality (`=`, `!=`) and the `null` sentinel
  apply to every type. Contains and wildcard (`~`, `!~`, and `*` inside `=` /
  `!=`) apply only to string fields: a substring test on an integer or boolean
  has no well-defined, compilable meaning, so a predicate such as `serial~5` or
  `enabled=*e` is rejected with an error naming the field's type and the
  operators it accepts. Ordering comparisons (`>`, `>=`, `<`, `<=`) apply only to
  numeric fields.

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
when its rows come from a persistent, queryable Deephaven table large enough that
pushing predicates to the engine avoids transferring a big result — the catalog
tables today, the PQ `QueryInfo` table later — because the where-clauses are
Deephaven Query Language evaluated by the engine before the rows are ever
serialized — as `catalog_tables_list` already does, pushing `Namespace` /
`TableName` predicates (`contains`, `startsWith`, `matches`, `in`) to the
worker. That reach is the payoff: full DQL, applied at the source, over data
that may be far larger than any response. The cost is that it is more complex to
write, requires knowing the backing column names, and is simply unavailable on
any tool with no such table behind it. A tool that materializes a small,
transient table on demand (`session_pip_list` builds `_pip_packages_table` from
the installed packages) is treated as output-layer: it exposes no `filters`,
though it still pushes `max_rows` into the fetch so the cap bounds transfer.

`match` is not confined to the output layer when a table is available. Where a
tool is table-backed, its predicates are first parsed into validated
field/operator/value nodes. Field names come only from the tool's closed
vocabulary, and values are rendered through a dedicated DQL literal encoder;
raw user text is never interpolated into a where-clause. The resulting
engine-side `filters` are pushed to the server, so matching runs on the worker
before rows are read. Output-layer evaluation remains the fallback for tools
with no table behind them, and both evaluators implement the same parsed
semantics.

| | `match` | `filters` |
| --- | --- | --- |
| Evaluated by | Engine when table-backed (compiled), else the tool output layer | Deephaven engine |
| Grammar | This simple grammar | Deephaven Query Language |
| Availability | Every collection tool | Only table-backed tools |
| Cost to write | Low; equality / contains over field names | Higher; full DQL over backing columns |
| Applied | At the source when table-backed, else after the tool builds its rows | Before the rows are read from the source |

Offering both on a table-backed tool is not ambiguous, and the order is fixed:
`filters` and the compiled `match` predicates are applied together at the source,
*before* the engine-side `max_rows` bounds the result, so `max_rows` and
`is_complete` describe the matched result rather than a pre-match slice. The
output-side layer (`fields`, `prune_empty`) then trims whatever came back.
`catalog_tables_list` keeps its engine-side `filters` unchanged and gains the
output-side capabilities on top.

## Scope and prior art

This layer reduces a payload; it does not transform it. Field selection follows
`google.protobuf.FieldMask`-style semantics (name a parent, keep its subtree), and
`match` is a compact filter string in the RSQL / Google AIP-160 family, kept
deliberately distinct from engine-side `filters` (Deephaven Query Language).

The simple grammar is intentionally not meant to do everything. Its job is to
cut down the data the MCP server sends — and to do so cheaply on the server,
since filtering a large result to a size a model can actually process quickly
matters more than supporting every possible filter or transform. Anything
beyond selection and narrowing is out of scope: a caller who needs exact
reshaping runs a downstream transform such as `jq` on the CLI. The server does
not grow an expression language.

`match` is intended to stay small permanently. Its proposed scope — equality,
contains, wildcard, numeric comparison, and the `null` sentinel — is essentially
complete; it may gain a small operator at most, but never move toward arbitrary
reshaping. The compilable, one-spelling-per-intent character that lets
it push down to the engine and be verified at a glance is the whole point;
expanding it past that would forfeit both. If customers do need a general query
language, the answer is a *separate* argument (something like JMESPath), never a
bigger `match`. Such an argument would be relevant only to the MCP tools — a CLI
caller already has `jq` downstream — and it carries the costs `match`
deliberately avoids: it cannot compile to engine-side `filters`, and it can
reshape the response into an arbitrary, less predictable output shape that
callers and tests can no longer rely on. It stays out of scope unless and until
that demand is real.

## Field grammar

`fields` uses dotted paths with protobuf `FieldMask`-style semantics — name a
parent to keep its subtree. Two behaviors below are project-specific extensions,
not part of `FieldMask`, so implementers should not assume protobuf
compatibility: a repeated field may appear before the final segment (a path
through an array applies to every element), and a literal dot in a key can be
escaped (`weird\.key`).

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

## Pruning

`prune_empty` removes only values that carry no information, applied recursively
to nested objects and arrays. The exact set pruned is:

- JSON `null`.
- Empty strings (`""`).
- Empty arrays (`[]`).
- Empty objects (`{}`), including objects that become empty after their own
  children are pruned.

Booleans and numbers are never pruned: `enabled: false` and a numeric `0` are
meaningful values, so pruning is a structural emptiness test, never a truthiness
test. Detail tools always retain their identity keys (`success`, `id`) even when
otherwise empty.

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
| `session_pip_list` | collection | `{package, version}` | `match`, `fields`, `max_rows`, `prune_empty` | Rows come from a small, transient Deephaven table (`_pip_packages_table`); treated as output-layer (no `filters`), but `max_rows` is pushed into the `get_table` fetch. |
| `catalog_tables_list` | collection | `{namespace, table_name}` | add `match`, `fields`, `prune_empty` | Retains its existing engine-side `filters` and `max_rows`. |
| `catalog_namespaces_list` | scalar collection | namespace strings | add `match` via synthetic `name` | Retains its existing `filters` and `max_rows`. |
| `session_tables_list` | scalar collection | table-name strings | `match`, `max_rows` via synthetic `name` | Scalar list; single documented `name` field. |
| `enterprise_systems_status` | collection | per-system records with optional diagnostics | `match`, `fields`, `max_rows`, `prune_empty` | `prune_empty` drops absent diagnostic fields. |
| `pq_details` | detail | envelope with nested `config`, `state_details`, `replicas`, `spares` | `fields`, `prune_empty` | `prune_empty` already exists. |

Detail and table-data tools with a deeper output shape — `session_details`, the
schema tools, `session_table_data`, `catalog_table_sample` — stay out of the
first pass; they are the [follow-on candidates](#follow-on-candidates).

Every first-pass collection has a conservative numeric `max_rows` default and
always reports `is_complete`, per the repository's bounded-output contract (e.g.
`table.py`, `catalog.py`): a default discovery call must never build an unbounded
payload that only fails at the size guard. `catalog_tables_list` keeps 10,000 and
`catalog_namespaces_list` 1,000; the others (`pq_list`, `sessions_list`,
`list_systems`, `session_pip_list`, `session_tables_list`,
`enterprise_systems_status`) default to 1,000. Uncapped output is an explicit
opt-in — MCP `max_rows: null`, CLI `--max-rows all` — never the default. For the
tools that are unbounded today this changes their default: a caller that needs
the complete list must now pass the opt-in, and `is_complete: false` flags any
truncation. That is a deliberate breaking change, documented in the changelog
with the opt-in as the migration path, and preferred over an uncapped default
that conflicts with the bounded-output contract.

### `pq list`

Adds `match`, `fields`, `max_rows`, and `prune_empty` to the list of PQ
summaries. `pq_list` has no Deephaven table behind it (see the refactor note
below), so `match` runs in the MCP server's output layer; the result is then
bounded by `max_rows`, pruned, and projected, in that order. Its closed match
and projection vocabulary is:

| Field | Type | Meaning |
| --- | --- | --- |
| `id` | string | Fully qualified PQ identifier. |
| `serial` | integer | Controller serial number. |
| `name` | string | PQ name. |
| `status` | string | Current PQ state. |
| `status_category` | string | Stable state category. |
| `owner` | string | Owning user. |
| `enabled` | boolean | Whether the PQ is enabled. |

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
`pip list` is the one that materializes a transient Deephaven table
(`_pip_packages_table`): it stays output-layer (no `filters`), but its
`max_rows` is passed into the `get_table` fetch rather than fetching every row
and capping afterward.

The catalog listings already carry engine-side `filters` and `max_rows`; this
pass adds the output-side capabilities on top — `match`, `fields`, and
`prune_empty` for `catalog_tables_list`, and `match` for the scalar
`catalog_namespaces_list`. Because these tools apply `max_rows` at the engine
before converting rows to Arrow, their `match` predicates must compile to
engine-side `filters` and be combined with any existing `filters` *before* that
row limit; otherwise the limit could discard rows that a later `match` would
have kept, and `is_complete` would no longer describe the matched result. For
`catalog_namespaces_list` the combined filters must additionally run before
`select_distinct("Namespace")`, since distinct extraction drops the `TableName`
column that documented `TableName` filters reference. The two scalar lists
(`catalog_namespaces_list` and `session_tables_list`) expose `match` through one
documented synthetic field, `name`, matched against each string element.

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

1. Make the new quick-filter surface `match` and expose it as
   `--match`; reserve `filters` for Deephaven engine filters.
2. Extract reusable helpers with one set of semantics: match compilation,
   nested projection, recursive pruning, and row shaping. The matcher validates
   each predicate against the tool's documented vocabulary — rejecting an unknown
   field, a literal that will not coerce to the field's declared type, and an
   operator not valid for that type — with an error that lists the allowed names
   and operators. Coercion and comparison are driven by the one per-field type
   table so the output matcher and the DQL compiler cannot disagree. The shared
   shaping path also estimates the emitted payload and calls `check_response_size`
   before returning — a row cap alone does not bound serialized size, since wide
   string fields or diagnostics can make even a capped result oversized —
   mirroring the existing catalog guard (`mcp_systems_server/_tools/catalog.py`).
3. For table-backed tools, combine compiled `match` with existing `filters`.
   `get_catalog_table` already applies filters before `_apply_row_limit`;
   preserve that order. For `distinct_namespaces=True`, move combined filtering
   before `select_distinct("Namespace")` so existing `TableName` filters remain
   valid and filtering precedes both distinct extraction and the row limit.
4. Give every first-pass collection a conservative numeric `max_rows` default per
   the bounded-output contract — catalog tools keep 10,000 / 1,000, the others
   default to 1,000 — and reserve uncapped output for an explicit opt-in (MCP
   `max_rows: null`, CLI `--max-rows all`). Validate `max_rows` as a positive
   integer. For the currently-unbounded tools this changes the default; document
   it as a breaking change with the opt-in as the migration path.
5. Apply the layer to the first-pass collections — `pq list`, `sessions list`,
   `list systems`, `pip list`, `session_tables_list`,
   `enterprise_systems_status`, and the catalog listings (`match` / `fields` /
   `prune_empty` atop their existing `filters`) — and add nested `fields` to
   `pq details`.
6. Add `unmatched_fields` to every successful response that received a
   *non-empty* `fields` selection. An omitted or explicitly empty `fields` is the
   no-projection case and adds no `unmatched_fields`, keeping the two identical.
   Populate it after pruning and projection; preserve requested-path order.
7. Update every affected MCP tool docstring (the tool-reference source of truth),
   the CLI `HelpSpec` / agents manifest, and `docs/CLI.md` (CLI surface only)
   with the grammar tables and canonical examples.
8. Apply the same layer to the follow-on candidates only after their field
   vocabularies and output contracts are reviewed.

## Testing and verification

Tests cover:

- Every match operator, wildcard, escape, `null`, case-insensitivity, OR, and
  AND behavior, including the numeric comparisons (`>`, `>=`, `<`, `<=`), a
  closed range as two ANDed `--match` entries, and a union via `or`.
- Typed-field coercion: an integer field parsing `serial=12345`, a boolean field
  parsing `enabled=true`/`false`, and an unparseable literal (`serial=abc`)
  rejected; the output matcher and the compiled DQL agreeing on the same typed
  comparison rather than a stringified one.
- Operator-validity rejection per type: `~` / `!~` / `*` on a non-string field
  (`serial~5`, `enabled=*e`) and a comparison on a non-numeric field, each
  erroring with the field's type and its allowed operators.
- Rejection of an unknown match field, asserting the error echoes the allowed
  vocabulary (the `owenr=user` typo case). <!-- codespell:ignore owenr -->
- Flat and nested projection, parent/descendant precedence, arrays,
  escaped-dot keys, and empty projection (an omitted and an explicit `[]`
  `fields` both preserve the full result and emit no `unmatched_fields`).
- `prune_empty` removing exactly `null` / `""` / `[]` / `{}` recursively while
  preserving `false` and numeric `0`.
- `max_rows` defaults: catalog 10,000 / 1,000 and the other first-pass tools
  1,000; the CLI parser's three states (omit → tool default, `all` → MCP `null`,
  positive integer → cap) with zero / negative / other raising `CliError`
  (`arg_parse_error`, exit `2`) not Click's `UsageError`; and `is_complete:
  false` when a default or explicit cap truncates.
- The shared shaping path calling `check_response_size`, so a capped-but-wide
  result still errors when it would exceed the serialized-response limit.
- `unmatched_fields` for absent paths, partially present paths, and values
  removed by `prune_empty`.
- `pq list` ordering: match, bound, prune, project; truncation and strict
  unknown-field validation.
- Catalog `match` compiled to `filters` and applied before the engine `max_rows`
  limit, with `is_complete` describing the matched result.
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

Matching detail objects; field-path wildcards or array indexes; grouping syntax;
`&&` / `||` combinators (AND is a repeated `--match`, OR is the `or` keyword);
server-side output transformation or expression languages (a JMESPath-style
capability, if ever needed, would be a separate argument, not a bigger
`match`); replacing Deephaven engine `filters`; new configuration or environment
variables.
