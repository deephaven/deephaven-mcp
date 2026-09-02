# Output Shaping for MCP Tools and `dhcli`

## Table of Contents

- [Problem](#problem)
- [Design goals](#design-goals)
- [API](#api)
- [Filter syntax](#filter-syntax)
- [Sort order](#sort-order)
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
| `filter` | Keep collection rows that satisfy an AIP-160 filter expression. |
| `order_by` | Sort collection rows by one or more fields, ascending or descending. |
| `fields` | Return only selected fields, including nested detail fields. |
| `prune_empty` | Remove null and empty values from an otherwise selected result. |
| `limit` | Bound a collection response and report whether it was truncated. |

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
| No invented language | Every surface is borrowed: `filter` is [AIP-160](https://google.aip.dev/160), `order_by` is [AIP-132](https://google.aip.dev/132), `fields` is an [AIP-157](https://google.aip.dev/157) partial response carrying `google.protobuf.FieldMask` paths. Each is implemented as specified rather than as a subset. |
| One obvious spelling per intent | Applies to what this project chooses, not to a borrowed grammar: where AIP-160 defines two spellings (`NOT` and `-`), both are accepted, because a caller's prior knowledge is only worth borrowing if it transfers intact. |
| Reliable structured calls | `filter`, `order_by`, and `fields` are each a single string, matching their specifications exactly (`fields` is a `FieldMask` in its canonical JSON encoding) and priming models that already know them; only the pre-existing `engine_filters` is an array, keeping its established nullable-array schema. |
| Discoverability | Each tool documents its filterable, sortable, and selectable fields, operators, examples, and parse errors. |
| Repairability | Invalid input returns the expected form and a concrete example. |
| Verifiability | Ordering is explicit and stable; projection changes only which fields are present; response metadata reports truncation and unmatched field paths. |
| Consistency | A tool uses one closed field vocabulary for `filter`, `order_by`, and `fields`. |

## API

### Collection tools

| Argument | Type | Default | Meaning |
| --- | --- | --- | --- |
| `filter` | string | `""` | One [AIP-160](https://google.aip.dev/160) filter expression over the tool's documented fields. Available on every collection tool; compiled to engine-side filters and run on the server when the tool is table-backed, else evaluated in the output layer. |
| `order_by` | string | `""` | One [AIP-132](https://google.aip.dev/132) ordering expression — comma-separated fields, a `desc` suffix for descending. Empty means the tool's natural order. Compiled to an engine-side sort when table-backed, else sorted in the output layer. |
| `engine_filters` | array of strings or null | `null` | Deephaven Query Language where-clauses, evaluated by the engine over the *backing table columns*. Offered only by tools whose rows come from a persistent Deephaven table (e.g. `catalog_tables_list`); absent otherwise. Keeps the existing catalog signature (`list[str] \| None`, default `null`) unchanged apart from the rename. |
| `fields` | string | `""` | Comma-separated [`google.protobuf.FieldMask`](https://google.aip.dev/157) paths naming the fields to retain from each returned row. Empty (the default) applies no projection: every field is preserved. |
| `limit` | positive integer or null | per-tool default | Maximum returned rows; must be a positive integer (zero or negative is rejected). Each tool's default is a conservative positive integer (see [First-pass scope](#first-pass-scope)); uncapped output is an explicit opt-in via `null`. |
| `prune_empty` | boolean | `false` | Recursively remove empty values (see [Pruning](#pruning)) before projection; booleans and numbers are always kept. |

The five compose as a fixed pipeline, and the order is part of the contract:

```text
filter → order_by → limit → prune_empty → fields
```

Sorting precedes the row cap so that `limit` yields a deterministic top-N rather
than an arbitrary slice that happens to be sorted, and projection comes last so
a field may be filtered or sorted on without being selected. `filter` is
universal; `engine_filters` is conditional on a Deephaven backing and so is not
part of every collection tool's signature. Where both are present they compose
without ambiguity: `filter` compiles to engine-side filters, so both run at the
source before `limit` bounds the result — detailed under [Why both `filter` and
`engine_filters`](#why-both-filter-and-engine_filters). A successful collection
response that received a non-empty `fields` selection includes
`unmatched_fields: []` when all requested paths matched at least one delivered
row, and lists any requested path that matched none; an omitted or empty
`fields` is the no-projection case and carries no `unmatched_fields`.
`is_complete: false` means `limit` truncated the filtered result.

### Detail tools

| Argument | Type | Default | Meaning |
| --- | --- | --- | --- |
| `fields` | string | `""` | Comma-separated [`google.protobuf.FieldMask`](https://google.aip.dev/157) paths naming the fields to retain from the detail object. Empty (the default) applies no projection: the whole object is preserved. |
| `prune_empty` | boolean | `false` | Recursively remove empty values (see [Pruning](#pruning)) before projection; booleans and numbers are always kept. |

A successful detail response always retains `success` and the tool's existing
identity path (`pq_details` retains top-level `id`). When a non-empty `fields`
selection was supplied it also includes `unmatched_fields`: an empty list
confirms every requested path was found in the delivered result, a non-empty
list identifies paths that produced no output. An omitted or empty
`fields` is the no-projection case and carries no `unmatched_fields`.

`dhcli` exposes `--filter`, `--order-by`, and `--fields` as single-valued flags,
one per invocation, mirroring the single-string MCP arguments:

```bash
dhcli pq list prod --filter 'owner = "user" OR owner = "user2"' --fields id,owner
dhcli pq list prod --order-by 'status, serial desc' --limit 10
dhcli pq details enterprise:prod:123 --fields config.owner,state_details.status
```

The CLI writes an `unmatched_fields` warning to stderr when the list is
non-empty. Structured tool output remains the source of truth; warnings do not
change the normal stdout payload.

`--limit` takes a positive integer to cap the result, or the literal `all`
for the tool's uncapped mode — the CLI spelling of the MCP `limit: null`,
mirroring `docker logs --tail all`. A shared parser preserves all three states
and does not fall through to Click's integer parsing: omission causes the wrapper
to omit `limit` (selecting the tool's default), `all` explicitly forwards
`{"limit": null}`, and a positive integer forwards that integer. Zero,
negatives, and any other value raise `CliError` with `arg_parse_error` (exit
`2`) rather than Click's unstructured `UsageError`. The `arg_parse_error`
registry text in `_errors.py` must be updated to cover invalid `--limit`
values alongside its existing cases (malformed `key=value` tokens and
`--history` JSON), so `dhcli agents errors` gives accurate guidance. Every
wrapper that exposes `limit` carries the flag with matching help and a test
for each path.

## Filter syntax

`filter` is an [AIP-160](https://google.aip.dev/160) filter expression — the
same syntax Google Cloud APIs accept on their List methods — implemented as the
specification defines it. Operators, precedence, and negation all behave the way
a conforming processor behaves; the only things not accepted are constructs with
no referent in this data model, tabulated below. `engine_filters` remains
reserved for Deephaven Query Language where-clauses evaluated by the engine.

| Form | Meaning | Example |
| --- | --- | --- |
| `field = value` | Equals | `owner = "jsmith"` |
| `field != value` | Not equals | `status != "STOPPED"` |
| `field : value` | Has, string case: case-insensitive substring (string fields only) | `name : report` |
| `field > value`, `field >= value` | Greater than (or equal); numeric fields only | `serial >= 1000` |
| `field < value`, `field <= value` | Less than (or equal); numeric fields only | `serial < 50` |
| `NOT expr`, `-expr` | Negation (AIP-160 defines both spellings) | `NOT name : archived`, `-name : archived` |
| `expr AND expr` | Conjunction | `enabled = true AND serial > 10` |
| `expr OR expr` | Disjunction | `owner = "a" OR owner = "b"` |
| `( expr )` | Grouping | `(owner = "a" OR owner = "b") AND enabled = true` |
| `NULL` | Null field | `owner = NULL` |
| `"..."` | Quoted string literal (required when the value has spaces or operators) | `name = "my daily query"` |

### Precedence

AIP-160's grammar is an **AND of OR-clauses**: `expression` is a series of
`sequence`s joined by `AND`, and `factor` — a series of `term`s joined by `OR` —
nests inside it. So negation binds tightest, then `OR`, then `AND`, and
`a AND b OR c` reads as `a AND (b OR c)`.

That is conjunctive normal form, and it is the shape filters naturally take: a
query is a set of independent conditions that must all hold, where any single
condition may accept several alternatives. `status = "RUNNING" AND owner = "a"
OR owner = "b"` means running queries belonging to either owner, which is
almost always what someone writing that line intends. Parentheses are available
whenever a different grouping is wanted.

The precedence is implemented exactly as specified, with no project-specific
reinterpretation and no requirement to parenthesize a mixed expression. Adopting
a published grammar and then bending it is what would raise error rates: a
caller's prior knowledge of AIP-160 — including a model's — is only worth
borrowing if it transfers intact.

A closed numeric range is `serial > 10 AND serial < 50`; a union is
`serial < 10 OR serial > 90`.

### Restrictions

AIP-160 is a grammar for filtering arbitrary protobuf messages; these tools
return flat records of scalars. The rows below are constructs a conforming
processor accepts that have no meaning against that row shape, so they are
rejected at parse time with an actionable message. None is reinterpreted, and
none is a stylistic preference — each is an absence of anything to evaluate
against.

| AIP-160 construct | Behavior | Reason |
| --- | --- | --- |
| `:` beyond the string case — `field:*` presence, map-key, and repeated-contains forms | Rejected; error names the field's type and its valid operators | Rows are flat records of scalars, so there is no map, no repeated field, and no unset-vs-set distinction for `has` to test. The string case is implemented as case-insensitive substring. |
| Bare terms with no field reference (`foo` alone as a restriction) | Rejected; error echoes the allowed field names | Every comparison must name a field from the tool's closed vocabulary, which is what makes a typo distinguishable from a valid empty result. |
| Functions and other extensions a host API may layer on | Rejected as unknown syntax | Use `engine_filters` for anything needing regex, functions, or `in` lists. |

Implicit conjunction (`a = 1 b = 2`, meaning `a = 1 AND b = 2`) is accepted, as
the specification defines it. It cannot silently mis-parse here: a bare word in
that position must resolve to a documented field, so an unquoted multi-word
value such as `name = my daily query` fails on `daily` with the vocabulary
error rather than quietly becoming a conjunction.

Every tool documents a closed filter vocabulary — the field names it accepts on
the left of a comparison. A predicate naming a field outside that vocabulary
(`owenr = "x"`) is rejected with an error that echoes the allowed field names; it <!-- codespell:ignore owenr -->
is never silently treated as a no-match. This keeps a typo distinguishable from
a valid empty result and satisfies the repairability goal. For a tool that also
supports `fields`, the filter vocabulary is the same set of names it exposes
there; the two scalar lists (`catalog_namespaces_list`, `session_tables_list`)
expose only the synthetic `name` to `filter` and offer no `fields`.

### Typed fields and operator validity

Each filterable field carries a declared type in the tool's vocabulary — string,
integer, or boolean (an enum is a string). That one type table drives both the
output-layer evaluator and the DQL compiler, so the two cannot disagree about a
predicate.

- **Coercion, not stringification.** The literal on the right is parsed to the
  field's declared type: an integer field parses `serial = 12345` as an integer,
  a boolean field accepts `enabled = true` / `false`, and a string field takes
  the literal verbatim and compares case-insensitively. Comparison then happens
  in the native type — the evaluator never stringifies a typed field. A literal
  that will not parse to the field's type (`serial = "abc"`, `enabled = maybe`)
  is rejected with the same shape of repair error as an unknown field.
- **One literal, both layers.** From that coerced value the compiler emits the
  matching DQL literal — a backtick string, a bare integer, `true` / `false`, or
  `isNull(Field)` for `NULL` — and the output-layer evaluator applies the
  identical typed comparison. Neither side stringifies.
- **Operator validity is per type.** Equality (`=`, `!=`) and the `NULL` sentinel
  apply to every type. Containment (`:`) applies only to string fields: a
  substring test on an integer or boolean has no well-defined, compilable
  meaning, so `serial : 5` or `enabled : true` is rejected with an error naming
  the field's type and the operators it accepts. Ordering comparisons (`>`,
  `>=`, `<`, `<=`) apply only to numeric fields.

### Why the names `filter` and `engine_filters`

The names are chosen for the primary caller — an AI agent reading a tool schema
cold, with no documentation and no prior turn to learn from.

**`filter` is the standard's own field name.** AIP-160 specifies that List
methods take a singular `filter` string, and Google Cloud APIs follow it
universally. Models therefore carry a strong prior: a string parameter named
`filter` on a list operation reliably elicits `owner = "x" AND serial > 5`. The
unqualified name *is* the signal — any qualifier (`match`, `field_filter`,
`json_filter`) weakens the prior precisely by deviating from the convention, and
a name like `json_filter` actively misdirects toward JSON-object or JMESPath
syntax. The type shape reinforces it: a single string says AIP-160, where an
array would say "some other convention."

**`engine_filters` names the layer and the language.** The qualifier tells a
cold reader three things the bare word could not: the expressions are evaluated
by the Deephaven **engine**, they are therefore written in Deephaven Query
Language, and they reference *backing table columns* rather than response field
names. The plural is accurate rather than sloppy — it is an array of
where-clauses, where `filter` is one expression.

**The asymmetry is deliberate and load-bearing.** An unqualified name marks the
default; a qualified name marks a specialization. An agent scanning schemas sees
`filter` on every collection tool and `engine_filters` on only specific ones, and
reads the hierarchy without any prose telling it which to prefer. Parallel names
(`field_filter` / `engine_filters`) would imply the two are peers, which is the
opposite of the intended relationship. This also avoids the `filter` / `filters`
trap: a bare singular/plural pair carrying "AIP-160 vs DQL" would be actively
harmful, whereas here the qualifier carries the distinction and the plural only
describes shape.

**What agents get concretely.** One filter concept, identically spelled and
identically behaved on every collection tool, expressible zero-shot from prior
knowledge; a second, clearly-marked parameter for the table case needing full
DQL. Both tool docstrings state the vocabulary contrast side by side, because
that — not the parameter name — is what prevents a wrong-syntax call:

```text
filter:          owner = "jsmith"      # AIP-160; response field names
engine_filters:  ["Owner = `jsmith`"]  # Deephaven Query Language; backing columns
```

`limit` follows the same reasoning at smaller stakes: it is the widely known
SQL/`jq` spelling for a row cap, instantly legible without documentation.

### Why both `filter` and `engine_filters`

The two are complementary, not redundant, and several tools should carry both.

`filter` is the universally available layer. It is expressed over the fields the
caller can actually see in the response, so it works on every collection —
including those with no Deephaven table behind them (`sessions list`,
`list systems`, `pip list`). Even where an engine backing does exist, `filter`
is the cheaper API for the common case: narrowing a table-backed result to a
handful of rows without composing a Deephaven Query Language expression or
knowing the backing column names.

`engine_filters` is the powerful layer, and it lives lower down. A tool exposes
it only when its rows come from a persistent, queryable Deephaven table large
enough that pushing predicates to the engine avoids transferring a big result —
the catalog tables today, the PQ `QueryInfo` table later. The where-clauses are
Deephaven Query Language evaluated by the engine before the rows are ever
serialized, as `catalog_tables_list` already does, pushing `Namespace` /
`TableName` predicates (`contains`, `startsWith`, `matches`, `in`) to the
worker. That reach is the payoff, and it is strictly greater than AIP-160's:
regex, function calls, `in` lists, column-to-column comparisons, and arithmetic
are all expressible in DQL and none are expressible in `filter`. The cost is
that it is more complex to write, requires knowing the backing column names, and
is simply unavailable on any tool with no such table behind it. A tool that
materializes a small, transient table on demand (`session_pip_list` builds
`_pip_packages_table` from the installed packages) is treated as output-layer:
it exposes no `engine_filters`, fetches all rows from the transient table, then
applies `filter` and `limit` in the output layer so the cap describes the
filtered result.

`filter` is not confined to the output layer when a table is available. Where a
tool is table-backed, the expression is first parsed into a validated AST. Field
names come only from the tool's closed vocabulary, and values are rendered
through a dedicated DQL literal encoder; raw user text is never interpolated
into a where-clause. The resulting engine-side filters are pushed to the server,
so filtering runs on the worker before rows are read. Output-layer evaluation
remains the fallback for tools with no table behind them, and both evaluators
implement the same parsed semantics.

| | `filter` | `engine_filters` |
| --- | --- | --- |
| Language | AIP-160 | Deephaven Query Language |
| Vocabulary | Response field names | Backing table column names |
| Evaluated by | Engine when table-backed (compiled), else the tool output layer | Deephaven engine |
| Availability | Every collection tool | Only persistent-table-backed tools |
| Cost to write | Low; comparisons over documented field names | Higher; full DQL over backing columns |
| Applied | At the source when table-backed, else after the tool builds its rows | Before the rows are read from the source |

Offering both on a table-backed tool is not ambiguous, and the order is fixed:
`engine_filters` and the compiled `filter` are applied together at the source,
*before* the engine-side `limit` bounds the result, so `limit` and
`is_complete` describe the filtered result rather than a pre-filter slice. The
output-side layer (`prune_empty`) then trims whatever came back.
`catalog_tables_list` keeps its engine-side where-clauses unchanged apart from
the rename, and gains the output-side capabilities on top.

## Sort order

`order_by` is an [AIP-132](https://google.aip.dev/132) ordering expression.
AIP-132 defines the standard `List` method, and AIP-160 defines the `filter`
field that method carries; `filter` and `order_by` are the two halves of one
convention, so a caller who knows either already knows the other.

| Form | Meaning | Example |
| --- | --- | --- |
| `field` | Sort ascending on that field | `name` |
| `field desc` | Sort descending on that field | `serial desc` |
| `a, b desc` | Multiple keys, applied left to right | `status, serial desc` |
| `""` | Omitted — the tool's natural order | |

This is AIP-132 as specified: a comma-separated list of field names, ascending
by default, with a `desc` suffix for descending, keys applied in written order.
AIP-132 defines only the `desc` suffix, so `asc` is **not** accepted — a bare
field name is the single spelling for ascending, which is both what the
specification says and what the project's one-spelling-per-intent goal requires.

Unlike `filter`, there is no operator-validity matrix: every type in a tool's
vocabulary is orderable, so any documented field may be a sort key. Unknown
fields are rejected with the same vocabulary-echoing error `filter` uses.

### Where AIP-132 is silent, follow Deephaven

AIP-132 specifies the *syntax* but not the *collation*. Those gaps are filled by
matching the Deephaven engine exactly, because the same expression must produce
the same ordering whether it was pushed into a `sort()` on a worker or evaluated
over in-memory rows. A divergence there would be invisible until a caller
compared two tools.

| Question | Behavior | Why |
| --- | --- | --- |
| Null ordering | Nulls sort as the smallest value: first ascending, last descending. | Deephaven's convention. AIP-132 is silent, and Python's `sorted` raises on `None` vs `str`, so the output-layer evaluator must implement this explicitly rather than inherit it. |
| Ties | Stable — rows that compare equal retain their prior relative order. | Both Deephaven's sort and Python's `sorted` are stable, so multi-key sorting can be implemented as successive stable single-key sorts and still agree with the engine. |
| Case | Case-sensitive, Java/UTF-16 lexicographic order. | Deephaven sorts strings by Java's natural order. Python compares Unicode code points instead, so the output-layer evaluator must compare UTF-16 code units explicitly to reproduce engine ordering, including for non-BMP characters. This is deliberately *not* symmetric with `filter`, whose `=` and `:` are case-insensitive. |

## Scope and prior art

This layer reduces a payload; it does not transform it. Every surface is
borrowed rather than invented: `filter` is
[AIP-160](https://google.aip.dev/160) — the filter language Google Cloud APIs
expose on List methods — kept deliberately distinct from `engine_filters`
(Deephaven Query Language); `order_by` is [AIP-132](https://google.aip.dev/132);
and `fields` is an [AIP-157](https://google.aip.dev/157) partial response whose
paths are `google.protobuf.FieldMask` paths (name a parent, keep its subtree).
Adopting published specifications rather than a bespoke grammar is a deliberate
reversal: a hand-rolled syntax has to be specified, documented, and defended
from scratch, and gives a caller nothing they already know.

`filter` implements AIP-160 as specified — operators, precedence, and negation
all behave as a conforming processor behaves — and the only rejections are the
[restrictions](#restrictions) forced by the data model: constructs that assume a
protobuf message with maps, repeated fields, or unset-vs-set distinctions, where
these tools return flat records of scalars. That is the whole point of borrowing
a specification. Its value is that a caller's prior knowledge transfers, and
that value survives an absence of anything to evaluate against; it would not
survive bending the grammar to taste, which is how a borrowed language quietly
becomes an invented one. AIP-160 is also small enough that the unsupported set
is short and enumerable — exactly why it is preferred here to a subset of
Deephaven Query Language, whose Java-expression surface is open-ended and whose
boundary could not be enumerated at all.

The filter layer's job is to cut down the data the MCP server sends — and to do
so cheaply on the server, since filtering a large result to a size a model can
actually process quickly matters more than supporting every possible transform.
Anything beyond selection and narrowing is out of scope: a caller who needs
exact reshaping runs a downstream transform such as `jq` on the CLI. The server
does not grow an expression language of its own.

Where AIP-160 stops, `engine_filters` continues — that is the escape hatch for
regex, function calls, `in` lists, and column-to-column comparisons, and it is
why the DQL surface is retained rather than replaced. If callers ever need
arbitrary *reshaping* (as opposed to filtering), the answer is a separate
argument such as JMESPath, never an extension to `filter`. Such an argument
would be relevant only to the MCP tools — a CLI caller already has `jq`
downstream — and it carries costs both current parameters avoid: it cannot
compile to engine-side filters, and it can reshape the response into an
arbitrary, less predictable output shape that callers and tests can no longer
rely on. It stays out of scope unless and until that demand is real.

## Field grammar

`fields` is the [AIP-157](https://google.aip.dev/157) partial-response pattern:
a caller names the parts of the result it wants and the server omits the rest.
AIP-157's proto spelling is a `read_mask` field of type
`google.protobuf.FieldMask`; the name `fields` follows the equivalent spelling
in Google's JSON/HTTP surface, where `?fields=` carries the same mask, and it
sits naturally beside `filter` and `order_by`. Path semantics are `FieldMask`'s
— name a parent to keep its subtree.

The value is a **single comma-separated string**, not an array, because that is
`FieldMask`'s canonical JSON encoding (`"config.owner,state_details.status"`).
Taking the specification's own wire form rather than an array of paths keeps the
argument byte-identical to what a caller would send to any other `FieldMask`
API, and makes all three shaping arguments — `filter`, `order_by`, `fields` —
single strings with one spelling each.

Two behaviors below are project-specific extensions, not part of `FieldMask`, so
implementers should not assume protobuf compatibility: a repeated field may
appear before the final segment (a path through an array applies to every
element), and a literal dot in a key can be escaped (`weird\.key`).

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

### Projection pushdown

Like `filter` and `order_by`, `fields` compiles to a table operation where a
table backs the tool. A selection whose paths are all top-level maps onto the
backing columns through the same vocabulary table and is applied as a
column-selecting view, so unwanted columns are never converted to Arrow or
serialized — the projection saves transfer, not just response bytes. The
collection tools in the first pass have flat rows (`{namespace, table_name}`,
`{package, version}`), so their projections push down entirely.

The pushdown is applied *after* filtering and sorting, which is what allows a
caller to filter or sort on a field they did not select. A selection containing
any nested path falls back to output-layer projection for the whole request: a
Deephaven table is flat, so a nested path has no column to select, and splitting
one selection across two layers would make `unmatched_fields` describe two
different objects. `prune_empty` never pushes down — emptiness is a per-cell
property of the assembled response, not a column-level one.

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
cheaply, as can scalar-list tools whose first pass only adds a single synthetic
`name` field for matching. Tools that need deeper synthetic shaping, a
nested-projection design review, or an engine refactor wait.

| Tool | Kind | Row / shape | First pass | Notes |
| --- | --- | --- | --- | --- |
| `pq_list` | collection | dict rows (`id`, `serial`, `name`, `status`, `status_category`, `owner`, `enabled`) | `filter`, `order_by`, `fields`, `limit`, `prune_empty` | No `engine_filters` yet — see the refactor note below. |
| `sessions_list` | collection | dict rows (session identity, `type`, `system`, `origin`) | `filter`, `order_by`, `fields`, `limit`, `prune_empty` | Keeps its existing `type` / `system` / `origin` scoping arguments. |
| `list_systems` | collection | `{name, type}` | `filter`, `order_by`, `fields`, `limit`, `prune_empty` | In-memory; no Deephaven backing. |
| `session_pip_list` | collection | `{package, version}` | `filter`, `order_by`, `fields`, `limit`, `prune_empty` | Rows come from a small, transient Deephaven table (`_pip_packages_table`); treated as output-layer (no `engine_filters`), so the fetch is uncapped and `limit` is applied after `filter` and `order_by` in the output layer. |
| `catalog_tables_list` | collection | `{namespace, table_name}` | add `filter`, `order_by`, `fields`, `prune_empty` | Retains its engine-side where-clauses (renamed to `engine_filters`) and its row cap (renamed to `limit`). |
| `catalog_namespaces_list` | scalar collection | namespace strings | add `filter`, `order_by` via synthetic `name` | Same renames as above; its existing implicit ascending sort stays the default. |
| `session_tables_list` | scalar collection | table-name strings | `filter`, `order_by`, `limit` via synthetic `name` | Scalar list; single documented `name` field. |
| `enterprise_systems_status` | collection | per-system records with optional diagnostics | `filter`, `order_by`, `fields`, `limit`, `prune_empty` | `prune_empty` drops absent diagnostic fields. |
| `pq_details` | detail | envelope with nested `config`, `state_details`, `replicas`, `spares` | `fields`, `prune_empty` | `prune_empty` already exists. |

Detail and table-data tools with a deeper output shape — `session_details`, the
schema tools, `session_table_data`, `catalog_table_sample` — stay out of the
first pass; they are the [follow-on candidates](#follow-on-candidates).

Every first-pass collection has a conservative numeric `limit` default and
always reports `is_complete`, per the repository's bounded-output contract (e.g.
`table.py`, `catalog.py`): a default discovery call must never build an unbounded
payload that only fails at the size guard. `catalog_tables_list` keeps 10,000 and
`catalog_namespaces_list` 1,000; the others (`pq_list`, `sessions_list`,
`list_systems`, `session_pip_list`, `session_tables_list`,
`enterprise_systems_status`) default to 1,000. Uncapped output is an explicit
opt-in — MCP `limit: null`, CLI `--limit all` — never the default. For the
tools that are unbounded today this changes their default: a caller that needs
the complete list must now pass the opt-in, and `is_complete: false` flags any
truncation. That is a deliberate breaking change, documented in the changelog
with the opt-in as the migration path, and preferred over an uncapped default
that conflicts with the bounded-output contract.

### `pq list`

Adds `filter`, `order_by`, `fields`, `limit`, and `prune_empty` to the list of
PQ summaries. `pq_list` has no persistent Deephaven table behind it (see the
refactor note below), so the whole pipeline runs in the MCP server's output
layer, in the documented order: filter, sort, bound, prune, project. Its closed
vocabulary — shared by `filter`, `order_by`, and `fields` — is:

| Field | Type | Meaning |
| --- | --- | --- |
| `id` | string | Fully qualified PQ identifier. |
| `serial` | integer | Controller serial number. |
| `name` | string | PQ name. |
| `status` | string | Current PQ state. |
| `status_category` | string | Stable state category. |
| `owner` | string | Owning user. |
| `enabled` | boolean | Whether the PQ is enabled. |

`pq list` gains no `engine_filters` in this pass. It reads the controller's PQ
snapshot (`controller.map()`), an in-memory map with no engine behind it, so only
the output-side layer applies. A later refactor can add engine-side filtering:
routing the listing through `connect_to_persistent_query` and a `PluginClient`
against the `WebClientData` query's `QueryInfo` table replaces the
un-filterable snapshot with a live Deephaven table, letting Deephaven Query
Language where-clauses run on the worker before any row is returned. The same
route generalizes to other controller-snapshot listings. It is out of scope
here; `filter` covers the common narrowing case in the meantime.

### `pq details`

Adds both `fields` and `prune_empty` to the detail surface. Projection applies
to the detail envelope, including nested `config`, `state_details`, `replicas`,
and `spares`; `success` and `id` are always retained.

### Other first-pass collections

`sessions list`, `list systems`, and `pip list` each build a plain list of
dictionaries, so they take the full output-shaping layer (`filter`, `order_by`,
`fields`, `limit`, `prune_empty`) with the same helpers as `pq list`.
`sessions list` keeps its existing `type` / `system` / `origin` scoping
arguments; those select *which* sessions to enumerate, while `filter` narrows
the enumerated rows. `pip list` is the one that materializes a transient
Deephaven table (`_pip_packages_table`): it stays output-layer (no
`engine_filters`). Because `filter` and `order_by` run in the output layer, the
`limit` cap must also be applied there — after both — rather than being pushed
into the `get_table` fetch. Pushing the cap into the fetch would discard source
rows before filtering and sorting, so a package that would have matched after
the first `limit` source rows would be silently omitted and `is_complete` would
describe the unfiltered table rather than the filtered result. `pip list`
therefore fetches all rows from `_pip_packages_table` and runs the whole
pipeline in the output layer.

The persistent/queryable catalog listings already carry engine-side
where-clauses and a row cap; this pass renames those to `engine_filters` and
`limit` and adds the output-side capabilities on top — `filter`, `order_by`,
`fields`, and `prune_empty` for `catalog_tables_list`, and `filter` /
`order_by` for the scalar `catalog_namespaces_list`, whose existing implicit
ascending sort remains its default when `order_by` is omitted. Because these
tools apply `limit` at the engine
before converting rows to Arrow, the compiled `filter` must be combined with any
`engine_filters` *before* that row limit; otherwise the limit could discard rows
that a later `filter` would have kept, and `is_complete` would no longer describe
the filtered result. For `catalog_namespaces_list` the combined filters must
additionally run before `select_distinct("Namespace")`, since distinct extraction
drops the `TableName` column that documented `TableName` where-clauses reference.
Transient table-backed tools (for example `pip list`) remain output-layer for
`filter`. The two scalar lists (`catalog_namespaces_list` and
`session_tables_list`) expose `filter` through one documented synthetic field,
`name`, matched against each string element.

`enterprise_systems_status` returns one record per system with several optional
diagnostic fields; it takes `filter` (e.g. narrow to unhealthy systems) and
`prune_empty` (drop the diagnostics that are absent on a healthy system).

## Follow-on candidates

With the in-memory lists handled in the first pass, the remaining candidates are
the deeper-shaped tools whose projection design warrants its own review:

| Capability | Candidates |
| --- | --- |
| Nested `fields` | `session details`, the schema tools (`session_table_schema`, `catalog_table_schema`), and `session_table_data` / `catalog_table_sample`. |
| `prune_empty` | `session details` and any detail result with optional diagnostics. |
| `limit` + `is_complete` | `session_table_data` and any other row-data tool still returning an unbounded result. |
| `engine_filters` | `pq_list`, once its listing is refactored onto a live `QueryInfo` table (see [`pq list`](#pq-list)). |

Catalog tools already use engine-side Deephaven Query Language; that behavior is
unchanged apart from the rename to `engine_filters`, and the first pass only
adds the output-side layer beside it.

## Implementation approach

1. Add `filter` (AIP-160) and `order_by` (AIP-132) as the universal output-side
   surfaces, rename the existing catalog `filters` to `engine_filters`, and
   rename `max_rows` to `limit`. Both renames are breaking and are recorded in
   the changelog.
2. Build the AIP-160 front end: a tokenizer and recursive-descent parser
   producing a validated AST. The grammar is AIP-160's as written — an `AND` of
   `OR`-clauses, so negation binds tightest, then `OR`, then `AND`, with both
   `NOT` and `-` accepted and implicit conjunction supported. Each construct in
   the restrictions table raises a distinct, actionable message rather than a
   generic syntax error. Two backends consume the AST — a DQL compiler and an
   in-memory evaluator — so both layers share one semantics.
3. Build the AIP-132 `order_by` parser over the same vocabulary: comma-separated
   keys, an optional `desc` suffix, no `asc` synonym. Two backends again — an engine
   `sort()` and an in-memory stable multi-key sort — which must agree on null
   ordering (nulls smallest), tie stability, and case-sensitive comparison, since
   AIP-132 specifies none of the three and Deephaven's behavior is the reference.
4. Extract reusable helpers with one set of semantics: filter parsing, sort-key
   parsing, nested projection, recursive pruning, and row shaping. The parsers
   validate each field reference against the tool's documented vocabulary —
   rejecting an unknown
   field, a literal that will not coerce to the field's declared type, and an
   operator not valid for that type — with an error that lists the allowed names
   and operators. Coercion and comparison are driven by the one per-field type
   table so the evaluator and the DQL compiler cannot disagree. The shared
   shaping path also estimates the emitted payload and calls `check_response_size`
   before returning — a row cap alone does not bound serialized size, since wide
   string fields or diagnostics can make even a capped result oversized —
   mirroring the existing catalog guard (`mcp_systems_server/_tools/catalog.py`).
   Because `get_response_limits` requires a fully qualified session id
   (`shared.py:566-598`), tools that span multiple sessions or have no single
   session id use strictest configured limits across every section the request
   can span (community and/or enterprise). If no relevant section is configured
   and the tool's success path is necessarily empty, skip the size check for
   that empty result.
   `pq_details` has no variable-sized list to reduce, so its error message omits
   the "reduce `limit`" advice and instead tells the caller to use a narrower
   `fields` selection or `prune_empty`.
5. For table-backed tools, push the whole pipeline to the engine in order:
   combined filters, then sort, then the row limit, then a column-selecting view
   for a fully top-level `fields` selection. `get_catalog_table` already applies
   filters before `_apply_row_limit`; preserve that order and insert the sort
   between them. For `distinct_namespaces=True`, move combined filtering
   before `select_distinct("Namespace")` so existing `TableName` where-clauses
   remain valid and filtering precedes both distinct extraction and the row limit.
   A selection containing any nested path skips the view and projects in the
   output layer instead.
6. Give every first-pass collection a conservative numeric `limit` default per
   the bounded-output contract — catalog tools keep 10,000 / 1,000, the others
   default to 1,000 — and reserve uncapped output for an explicit opt-in (MCP
   `limit: null`, CLI `--limit all`). Validate `limit` as a positive
   integer. For the currently-unbounded tools this changes the default; document
   it as a breaking change with the opt-in as the migration path.
7. Apply the layer to the first-pass collections — `pq list`, `sessions list`,
   `list systems`, `pip list`, `session_tables_list`,
   `enterprise_systems_status`, and the catalog listings (`filter` / `order_by` /
   `fields` / `prune_empty` atop their renamed `engine_filters`) — and add
   nested `fields` to `pq details`.
8. Add `unmatched_fields` to every successful response that received a
   *non-empty* `fields` selection. An omitted or empty `fields` is the
   no-projection case and adds no `unmatched_fields`, keeping the two identical.
   Populate it after pruning and projection; preserve requested-path order.
9. Update every affected MCP tool docstring (the tool-reference source of truth),
   the CLI `HelpSpec` / agents manifest, and `docs/CLI.md` (CLI surface only)
   with the AIP-160 / AIP-132 / AIP-157 citations, the operator and ordering
   tables, the vocabulary contrast between `filter` and `engine_filters`, the
   sort-cost note, and canonical examples.
10. Apply the same layer to the follow-on candidates only after their field
    vocabularies and output contracts are reviewed.

## Testing and verification

Tests cover:

- Every supported operator (`=`, `!=`, `:`, `>`, `>=`, `<`, `<=`), both negation
  spellings (`NOT`, `-`), `AND`, `OR`, parentheses, `NULL`, quoted string
  literals, and implicit conjunction; a closed numeric range and a union.
- AIP-160 precedence: `a = 1 AND b = 2 OR c = 3` evaluates as
  `a = 1 AND (b = 2 OR c = 3)`, with parenthesized forms confirming the
  alternative grouping — asserted against the reading a conforming processor
  produces, on both the engine and in-memory paths.
- Every row of the restrictions table, each asserting a *distinct* actionable
  error rather than a generic parse failure: the non-string `:` forms
  (`field:*`), a bare term with no field reference, and unknown function syntax.
- That an unquoted multi-word value (`name = my daily query`) fails with the
  vocabulary error on `daily` rather than silently becoming a conjunction.
- `order_by`: single and multi-key ordering, the `desc` suffix, keys applied left to right,
  rejection of `asc` and of an unknown sort field; null ordering (nulls first
  ascending, last descending), stable ties, and case-sensitive comparison — each
  asserted to produce identical results from the engine sort and the in-memory
  sort.
- Pipeline order: `order_by` applied before `limit`, so a capped request returns
  a deterministic top-N rather than a sorted arbitrary slice.
- Projection pushdown: a fully top-level `fields` selection compiles to a
  column-selecting view on a table-backed tool, a selection containing a nested
  path falls back to output-layer projection, and both produce the same payload;
  filtering and sorting on an unselected field still work.
- Typed-field coercion: an integer field parsing `serial = 12345`, a boolean
  field parsing `enabled = true` / `false`, and an unparseable literal
  (`serial = "abc"`) rejected; the output evaluator and the compiled DQL agreeing
  on the same typed comparison rather than a stringified one.
- Operator-validity rejection per type: `:` on a non-string field
  (`serial : 5`, `enabled : true`) and an ordering comparison on a non-numeric
  field, each erroring with the field's type and its allowed operators.
- Rejection of an unknown filter field, asserting the error echoes the allowed
  vocabulary (the `owenr = "x"` typo case). <!-- codespell:ignore owenr -->
- Flat and nested projection, parent/descendant precedence, arrays,
  escaped-dot keys, and empty projection (an omitted and an empty-string
  `fields` both preserve the full result and emit no `unmatched_fields`).
- `prune_empty` removing exactly `null` / `""` / `[]` / `{}` recursively while
  preserving `false` and numeric `0`.
- `limit` defaults: catalog 10,000 / 1,000 and the other first-pass tools
  1,000; the CLI parser's three states (omit → tool default, `all` → MCP `null`,
  positive integer → cap) with zero / negative / other raising `CliError`
  (`arg_parse_error`, exit `2`) not Click's `UsageError`; and `is_complete:
  false` when a default or explicit cap truncates.
- The shared shaping path calling `check_response_size`, so a capped-but-wide
  result still errors when it would exceed the serialized-response limit.
- `unmatched_fields` for absent paths, partially present paths, and values
  removed by `prune_empty`.
- `pq list` ordering: filter, sort, bound, prune, project; truncation and strict
  unknown-field validation.
- Catalog `filter` compiled to engine-side where-clauses and applied before the
  engine `limit`, with the sort between them and `is_complete` describing the
  filtered result.
- The other first-pass collections (`sessions list`, `list systems`, `pip
  list`, catalog listings, `enterprise_systems_status`): the same shaping layer,
  the synthetic `name` field on the scalar lists, and `engine_filters` composing
  with `filter` on the catalog tools.
- `pq details` identity-key retention, nested paths, pruning, and unmatched
  path reporting.
- CLI forwarding of the three single-valued flags (`--filter`, `--order-by`,
  `--fields`), including that passing any of them twice is a usage error rather
  than a silent last-wins; stderr warnings, tool-wrapper contract checks, help
  checks, and full per-file coverage.

Verification runs focused filter/sort/projection/PQ/CLI tests, then precommit,
then the complete test suite. Smoke checks include a multi-owner `--filter`, a
`--order-by` with mixed directions combined with `--limit` to confirm top-N, a
nested `pq details --fields` request, and a deliberately missing path that
produces an `unmatched_fields` report.

## Non-goals

Filtering or sorting detail objects; field-path wildcards or array indexes;
extending `filter` or `order_by` past their specifications — both implement
AIP-160 and AIP-132 as written, and widening either beyond what its
specification defines is not on the roadmap;
sorting by an expression rather than a documented field; server-side output
transformation or reshaping languages (a JMESPath-style capability, if ever
needed, would be a separate argument, not a bigger `filter`); replacing
`engine_filters` — the Deephaven Query Language surface is retained precisely
because AIP-160 cannot express regex, function calls, `in` lists, or
column-to-column comparisons; new configuration or environment variables.
