# Field docstrings on config schemas

The full rules for documenting a field on a `StrictSchema` /
`RedactableSchema` subclass. Load this when adding or editing a
schema field; the one-line rule is in `SKILL.md` *Every field
carries a PEP 257 trailing docstring*.

## Why the trailing docstring, specifically

The project enables `use_attribute_docstrings=True` on
`StrictSchema.model_config`, so Pydantic v2 harvests the
triple-quoted string under a field declaration into
`model_fields[name].description` at class-build time. That
description is what flows into:

- `Model.model_json_schema()` — the JSON Schema for the model.
- The MCP tool/resource schemas that AI agents consume at runtime
  (agents have no other way to learn what a parameter means).
- `pydantic.fields.FieldInfo.description` — used by hover-info,
  OpenAPI generators, and `inspect`-based tooling.

Verify a field actually reached runtime:

```bash
uv run python -c "from <module> import <Model>; print(<Model>.model_fields['<field>'].description)"
```

`None` or an empty string means the field is undocumented at
runtime regardless of what the class docstring says.

## Forbidden — invisible to runtime introspection

- **Sphinx-style `Attributes:` block on the class docstring.** This
  is plain text in `__doc__` and never reaches
  `model_fields[name].description` or `model_json_schema()`. An AI
  agent calling the model's MCP-exposed schema sees the field with
  no description. `tests/test_field_docs_contract.py` fails the
  build when any production field has no runtime description.

## Discouraged — works at runtime but violates project style

- **Explicit `Field(description="...")`.** This *does* populate
  `description` correctly, so it is not a functional bug — but the
  project standardizes on the trailing-docstring style for
  consistency and readability (especially for multi-line prose,
  where triple-quoted strings beat parenthesized string
  concatenations). New code uses trailing docstrings; existing
  `Field(description=...)` usages should be converted
  opportunistically.

## What the class docstring keeps

Only **class-level** prose: what the model represents, when it is
used, validator behavior, wire-format examples. It does *not* list
fields — the field declarations are the single source of truth for
field documentation.

## Content — state what the field holds, then stop

A field docstring names the thing the field carries, in domain
terms. Two opposite failures are both wrong:

- **Vacuous restatement.** Re-spelling the field name and type
  teaches nothing the declaration does not already state — name,
  type, and required-ness are all already visible.
- **Brittle over-specification.** Narrating a nested model's own
  sub-fields, enumerating another type's members, or naming which
  consumer calls what. That detail is owned by the nested type's own
  field docstrings and by the calling code; restating it here drifts
  the moment either changes.

```python
# WRONG — vacuous (restates name + type)
auth: AuthConfig
"""Required ``auth`` block."""

# WRONG — brittle (documents AuthConfig's internals and a consumer)
auth: AuthConfig
"""Its ``credentials`` (an ``AnonymousCredentials`` for anonymous
sessions) hands directly to ``CoreSession.from_credentials``."""

# RIGHT — what the field holds, in domain terms
auth: AuthConfig
"""Authentication details for connecting to the Community server."""
```
