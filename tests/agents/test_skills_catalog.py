"""Project-wide conformance checks for the agent-skill catalog.

This module checks markdown, not Python: it imports nothing from
``deephaven_mcp`` and asserts properties of the files under
``.agents/skills/`` plus the project ``AGENTS.md``. It lives under
``tests/`` because ``pytest`` is what CI runs -- ``bin/precommit.sh`` is a
local-only convenience script -- and under ``tests/agents/`` rather than
the ``tests/`` root because it mirrors ``.agents/``, not a module of the
shipped package. The sibling precedent is ``tests/scripts/``, which
mirrors the top-level ``scripts/`` directory the same way.

Three concerns are enforced here:

1. **Frontmatter.** Agent skills live at ``.agents/skills/<name>/SKILL.md``.
   Per the `Agent Skills specification <https://agentskills.io/specification>`_
   each file opens with a YAML block carrying ``name`` and ``description``.
   That block is the only part of a skill an agent reads at decision time, so
   a malformed one is not a cosmetic defect: a host that parses frontmatter
   strictly gets an error instead of a description, and the skill silently
   stops being discoverable.
2. **Catalog registration.** Every skill has a row in the catalog README,
   and the ``ref-`` name prefix agrees with the ``user-invocable`` field.
3. **Cross-reference integrity.** Section citations name real headings,
   cited skills exist on disk, canonical-implementation pointers name
   files and symbols that are really there, and every bare file-path
   citation resolves.

Nothing else in the toolchain catches this. ``markdownlint`` validates
markdown body structure and does not parse YAML, so
``./bin/precommit.sh`` passes on a skill whose frontmatter cannot be
loaded. This test closes that gap.

The regression that motivated it: ``ref-skill-effectiveness`` carried an
unquoted colon-space inside its description (``... at decision or
execution time: skill descriptions ...``). YAML read ``time:`` as a
nested mapping key and rejected the whole block with "mapping values are
not allowed here". A sibling skill (``ref-logging-standards``) had the
same construct correctly double-quoted, so the catalog was inconsistent
rather than uniformly broken — exactly the state a test detects and
review misses.

Each check below aggregates every offending skill into one assertion
message, so a contributor sees the full list at once rather than fixing
them one run at a time.

The conventions enforced here are owned by
``.agents/skills/ref-skill-authoring-standards/SKILL.md`` (*Frontmatter
contract* and *Naming and the prefix rule*).
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import pytest
import yaml

#: Project-wide convention enforcement, not a mirror of one source file
#: (``ref-python-coding-practices`` rule 5).
pytestmark = pytest.mark.guardrail

#: Repository root, derived from this file's location
#: (``<root>/tests/agents/``).
_REPO_ROOT = Path(__file__).resolve().parents[2]

#: Catalog directory holding one subdirectory per skill.
_SKILLS_DIR = _REPO_ROOT / ".agents" / "skills"

#: Present in a source checkout, absent when this file runs from an
#: installed package -- the signal that an absent catalog is a defect
#: rather than an expected packaging difference.
_SOURCE_CHECKOUT_MARKER = _REPO_ROOT / "pyproject.toml"

#: Keys the Agent Skills spec and this project permit in skill frontmatter.
_ALLOWED_KEYS = frozenset({"name", "description", "user-invocable"})

#: Spec-legal skill name: lowercase alphanumerics in hyphen-separated runs.
_NAME_PATTERN = re.compile(r"^[a-z0-9]+(-[a-z0-9]+)*$")

#: A catalog README table row whose first cell is a backticked skill
#: name. Anchored to the row start so a backticked name appearing later
#: in the same row (a cross-reference inside the purpose text) is not
#: mistaken for the row's subject.
_README_ROW_PATTERN = re.compile(r"^\|\s*`([a-z][a-z0-9-]*)`\s*\|", re.MULTILINE)

#: Spec maximum length for ``name``.
_NAME_MAX_LEN = 64

#: Spec maximum length for ``description``.
_DESCRIPTION_MAX_LEN = 1024

# ``Any``: ``yaml.safe_load`` returns arbitrary YAML scalars, sequences, and
# mappings. Frontmatter values are author-supplied and not statically known
# here, which is exactly what the checks below exist to constrain.
Frontmatter = dict[str, Any]

#: A cross-skill section citation: a backticked skill name followed by an
#: emphasized section name, as in "`ref-skill-effectiveness` *Triggerability*".
_SECTION_CITATION = re.compile(r"`([a-z][a-z0-9-]*)`\s+\*([^*\n]+)\*")

#: An ``AGENTS.md`` section citation, same shape with a fixed target file.
_AGENTS_CITATION = re.compile(r"`AGENTS\.md`\s+\*([^*\n]+)\*")

#: A linked file citing a section of its own parent, as in
#: "the one-line rule is in `SKILL.md` *Every field carries a docstring*".
_PARENT_CITATION = re.compile(r"`SKILL\.md`\s+\*([^*\n]+)\*")

#: A canonical-implementation pointer: "`cli/_help.py` (`build_help`)".
_CODE_POINTER = re.compile(r"`([A-Za-z0-9_./-]+\.py)`\s+\(([^()]*)\)")

#: A bare path citation: a backticked Python path and nothing else, as in
#: "Enforced by `tests/test_field_docs_contract.py`." The character class
#: excludes whitespace so a backticked *command* (``uv run scripts/x.py``)
#: is not mistaken for a path pointer.
_BARE_PATH = re.compile(r"`([A-Za-z0-9_./-]+\.py)`")

#: A backticked identifier inside a canonical-implementation pointer's parens.
_POINTER_SYMBOL = re.compile(r"`([A-Za-z0-9_.]+)`")

#: One backticked identifier plus its trailing separator, used to decide
#: whether a parenthetical is a pure symbol list or explanatory prose.
_POINTER_SYMBOL_ITEM = re.compile(r"`[A-Za-z0-9_.]+`\s*(?:,\s*|/\s*|$)")

#: Documented placeholder paths that name no real file by design.
_PLACEHOLDER_PATHS = frozenset({"path/to/file.py"})

#: Leading ordinal on a numbered heading, including the sub-lettered form
#: (``## 3. HelpSpec fields``, ``#### 2b. Every field carries a docstring``).
_HEADING_ORDINAL = re.compile(r"^\d+[a-z]?[.)]\s*")

#: Opening or closing fence of a code block. Lines inside a fence are content,
#: never headings — a Python comment starts with ``#`` too.
_CODE_FENCE = re.compile(r"^\s*(?:```|~~~)")


def _catalog_markdown() -> list[Path]:
    """Return every markdown file governed by the catalog conventions.

    Returns:
        list[Path]: All markdown under ``.agents/skills`` (skill bodies,
            their linked files, and the catalog README) plus the project
            ``AGENTS.md`` when present.
    """
    files = sorted(_SKILLS_DIR.rglob("*.md")) if _SKILLS_DIR.is_dir() else []
    agents_md = _REPO_ROOT / "AGENTS.md"
    if agents_md.is_file():
        files.append(agents_md)
    return files


def _headings(markdown: Path) -> set[str]:
    """Return the normalized headings of a markdown file.

    Lines inside a fenced code block are skipped: a ``#`` comment in a
    Python example is not a heading, and treating it as one would let a
    bogus citation match it.

    Args:
        markdown: Path to a markdown file.

    Returns:
        set[str]: Lowercased heading text with the leading ``#`` run and
            any numbering prefix removed, so ``## 3. HelpSpec fields``
            and ``#### 2b. Every field`` normalize to ``helpspec fields``
            and ``every field``.
    """
    found: set[str] = set()
    in_fence = False
    for line in markdown.read_text(encoding="utf-8").splitlines():
        if _CODE_FENCE.match(line):
            in_fence = not in_fence
            continue
        if not in_fence and line.startswith("#"):
            text = line.lstrip("#").strip()
            found.add(_HEADING_ORDINAL.sub("", text).lower())
    return found


def _heading_matches(cited: str, headings: set[str]) -> bool:
    """Report whether a cited section name identifies one of ``headings``.

    A citation may name a heading's leading clause (*Path locality* for
    the heading ``Path locality: decide it per flag``), so a prefix match
    in either direction counts.

    Args:
        cited: The section name as written at the call site.
        headings: Normalized headings from the target file.

    Returns:
        bool: True when the citation identifies a heading.
    """
    needle = cited.strip().rstrip(".,;:").lower()
    return any(
        needle == h or h.startswith(needle) or needle.startswith(h) for h in headings
    )


def _python_files_by_suffix() -> dict[str, list[Path]]:
    """Index every Python file under ``src``, ``tests``, and ``scripts`` by suffix.

    Indexing by suffix lets a pointer resolve regardless of how much of
    the path it spells: ``cli/_help.py`` and ``_tools/catalog.py`` both
    match without the test maintaining a list of base directories.

    The three roots are the project's source roots -- ``scripts`` holds
    top-level tooling that the catalog cites and that mirrors into
    ``tests/scripts/``.

    Returns:
        dict[str, list[Path]]: Every path suffix mapped to the files that
            end with it.
    """
    index: dict[str, list[Path]] = {}
    for root in ("src", "tests", "scripts"):
        base = _REPO_ROOT / root
        if not base.is_dir():
            continue
        for path in base.rglob("*.py"):
            parts = path.relative_to(_REPO_ROOT).parts
            for start in range(len(parts)):
                index.setdefault("/".join(parts[start:]), []).append(path)
    return index


def _skill_files() -> list[Path]:
    """Return every ``SKILL.md`` in the catalog, sorted by skill name.

    Returns:
        list[Path]: Absolute paths to each skill body. Empty when the
            catalog directory is absent (an installed-package test run
            rather than a source checkout).
    """
    if not _SKILLS_DIR.is_dir():
        return []
    return sorted(_SKILLS_DIR.glob("*/SKILL.md"))


def _frontmatter_text(body: str) -> str | None:
    """Extract the raw YAML frontmatter block from a skill body.

    Args:
        body: Full text of a ``SKILL.md`` file.

    Returns:
        str | None: The text between the opening ``---`` delimiter and
            the closing one, or ``None`` when the file does not open with
            a delimited frontmatter block. A ``---`` appearing later in
            the markdown body (a horizontal rule) is not mistaken for the
            closing delimiter because only the first one is considered.
    """
    if not body.startswith("---\n"):
        return None
    end = body.find("\n---", len("---\n"))
    if end == -1:
        return None
    return body[len("---\n") : end]


def _parse_frontmatter(path: Path) -> tuple[Frontmatter | None, str | None]:
    """Parse one skill's frontmatter, reporting failure instead of raising.

    Args:
        path: Path to a ``SKILL.md`` file.

    Returns:
        tuple[Frontmatter | None, str | None]: The parsed mapping and
            ``None`` on success, or ``None`` and a human-readable reason
            when the block is missing, is not valid YAML, or does not
            parse to a mapping.
    """
    raw = _frontmatter_text(path.read_text(encoding="utf-8"))
    if raw is None:
        return None, "no YAML frontmatter block found"
    try:
        parsed = yaml.safe_load(raw)
    except yaml.YAMLError as exc:
        return None, f"not valid YAML: {exc}"
    if not isinstance(parsed, dict):
        return None, f"parsed to {type(parsed).__name__}, expected a mapping"
    return parsed, None


@pytest.fixture(scope="session")
def skills() -> dict[str, Frontmatter]:
    """Map each skill's directory name to its parsed frontmatter.

    Returns:
        dict[str, Frontmatter]: Directory name to frontmatter mapping.
    """
    paths = _skill_files()
    if not paths:
        pytest.skip(f"skill catalog not present at {_SKILLS_DIR}")
    parsed: dict[str, Frontmatter] = {}
    for path in paths:
        front, _error = _parse_frontmatter(path)
        if front is not None:
            parsed[path.parent.name] = front
    return parsed


def test_catalog_is_discovered() -> None:
    """The catalog directory exists and holds at least one skill.

    Guards against the rest of this module silently passing because the
    glob matched nothing: every other check here skips or reports no
    violations on an empty catalog, so without this test a vanished
    ``.agents/skills`` would leave the whole family green while
    enforcing nothing.

    An absent catalog is only tolerable when this file runs from an
    installed package rather than a source checkout. ``pyproject.toml``
    beside :data:`_REPO_ROOT` distinguishes the two, so the missing
    catalog fails where it is a real defect and skips where it is not.
    """
    paths = _skill_files()
    if paths:
        return
    if _SOURCE_CHECKOUT_MARKER.is_file():
        pytest.fail(
            f"No skills found under {_SKILLS_DIR}, but this is a source "
            f"checkout ({_SOURCE_CHECKOUT_MARKER.name} is present). The "
            "catalog is expected here; every other check in this module "
            "would pass vacuously without it."
        )
    pytest.skip(f"skill catalog not present at {_SKILLS_DIR}")


def test_every_frontmatter_parses_as_yaml() -> None:
    """Every skill's frontmatter loads as a YAML mapping.

    Frontmatter is YAML per the spec. A block that does not parse costs
    the agent the description it routes on, and no other check in the
    toolchain catches it.
    """
    paths = _skill_files()
    if not paths:
        pytest.skip(f"skill catalog not present at {_SKILLS_DIR}")
    broken: list[str] = []
    for path in paths:
        _front, error = _parse_frontmatter(path)
        if error is not None:
            broken.append(f"{path.parent.name}: {error}")
    if broken:
        joined = "\n  - ".join(broken)
        pytest.fail(
            f"Skill frontmatter must parse as YAML:\n  - {joined}\n\n"
            "A description containing a colon-space, a leading '[', or other "
            "YAML-significant punctuation must be double-quoted."
        )


def test_name_matches_directory(skills: dict[str, Frontmatter]) -> None:
    """Each ``name`` field equals its containing directory name.

    Hosts resolve a skill by directory; the ``name`` field is what the
    agent sees. A mismatch makes cross-references unresolvable.
    """
    mismatched = [
        f"{directory}: name={front.get('name')!r}"
        for directory, front in skills.items()
        if front.get("name") != directory
    ]
    if mismatched:
        joined = "\n  - ".join(mismatched)
        pytest.fail(f"`name` must equal the directory name:\n  - {joined}")


def test_names_are_spec_legal(skills: dict[str, Frontmatter]) -> None:
    """Each ``name`` uses only lowercase alphanumerics and inner hyphens.

    The spec charset excludes underscores, uppercase, and leading or
    trailing hyphens, and caps the length at 64 characters.
    """
    offenders: list[str] = []
    for directory, front in skills.items():
        name = front.get("name")
        if not isinstance(name, str) or not _NAME_PATTERN.match(name):
            offenders.append(f"{directory}: illegal characters in {name!r}")
        elif len(name) > _NAME_MAX_LEN:
            offenders.append(
                f"{directory}: {len(name)} characters exceeds the {_NAME_MAX_LEN}-character limit"
            )
    if offenders:
        joined = "\n  - ".join(offenders)
        pytest.fail(f"`name` must be spec-legal:\n  - {joined}")


def test_descriptions_are_present_single_line_and_bounded(
    skills: dict[str, Frontmatter],
) -> None:
    """Each ``description`` is a non-empty single line within the length cap.

    The description is the only text an agent reads when deciding whether
    to invoke a skill. An embedded newline breaks the frontmatter
    contract, and an over-long one is truncated by the host.
    """
    offenders: list[str] = []
    for directory, front in skills.items():
        description = front.get("description")
        if not isinstance(description, str) or not description.strip():
            offenders.append(f"{directory}: missing or empty description")
            continue
        if "\n" in description:
            offenders.append(f"{directory}: description contains a newline")
        if len(description) > _DESCRIPTION_MAX_LEN:
            offenders.append(
                f"{directory}: description is {len(description)} characters, "
                f"over the {_DESCRIPTION_MAX_LEN}-character limit"
            )
    if offenders:
        joined = "\n  - ".join(offenders)
        pytest.fail(f"`description` must be a bounded single line:\n  - {joined}")


def test_no_unexpected_frontmatter_keys(skills: dict[str, Frontmatter]) -> None:
    """No skill declares a frontmatter key outside the permitted set.

    An unrecognized key is either a typo or an undocumented convention;
    both are caught here rather than being silently ignored by the host.
    """
    offenders = [
        f"{directory}: {sorted(set(front) - _ALLOWED_KEYS)}"
        for directory, front in skills.items()
        if set(front) - _ALLOWED_KEYS
    ]
    if offenders:
        joined = "\n  - ".join(offenders)
        pytest.fail(
            f"Unexpected frontmatter keys (allowed: {sorted(_ALLOWED_KEYS)}):\n  - {joined}"
        )


def test_ref_prefix_matches_user_invocable(skills: dict[str, Frontmatter]) -> None:
    """A ``ref-``-prefixed skill declares ``user-invocable: false``; others omit it.

    The prefix tracks human invocability, and ``user-invocable`` is the
    mechanism that enforces it. The two must agree, or the naming
    convention stops predicting behavior.
    """
    offenders: list[str] = []
    for directory, front in skills.items():
        declared = front.get("user-invocable")
        if directory.startswith("ref-"):
            if declared is not False:
                offenders.append(
                    f"{directory}: ref- prefixed but user-invocable={declared!r} (expected False)"
                )
        elif declared is not None:
            offenders.append(
                f"{directory}: unprefixed but declares user-invocable={declared!r} (expected absent)"
            )
    if offenders:
        joined = "\n  - ".join(offenders)
        pytest.fail(f"`ref-` prefix and `user-invocable` must agree:\n  - {joined}")


def test_every_skill_has_a_readme_row(skills: dict[str, Frontmatter]) -> None:
    """Every skill appears in the catalog README.

    ``.agents/skills/README.md`` is the human's view of the dependency
    graph; an unlisted skill is invisible to contributors.
    """
    readme = _SKILLS_DIR / "README.md"
    if not readme.is_file():
        pytest.skip(f"catalog README not present at {readme}")
    text = readme.read_text(encoding="utf-8")
    missing = [directory for directory in skills if f"`{directory}`" not in text]
    if missing:
        joined = "\n  - ".join(sorted(missing))
        pytest.fail(f"Skills absent from {readme.name}:\n  - {joined}")


def test_every_readme_row_names_a_real_skill(skills: dict[str, Frontmatter]) -> None:
    """No catalog README row names a skill that does not exist.

    The reverse of :func:`test_every_skill_has_a_readme_row`, which only
    catches the missing direction. A row left behind by a rename or a
    removal is caught by nothing else:
    :func:`test_cited_skills_exist` does read this file, but it matches
    only ``Apply``/``See`` directives, and a row is a bare table cell.
    Renaming a batch of skills -- as the ``ref-`` migration did -- is
    exactly when a stale row survives beside its replacement.
    """
    readme = _SKILLS_DIR / "README.md"
    if not readme.is_file():
        pytest.skip(f"catalog README not present at {readme}")
    text = readme.read_text(encoding="utf-8")
    named = _README_ROW_PATTERN.findall(text)
    stale = sorted({row for row in named if row not in skills})
    if stale:
        joined = "\n  - ".join(stale)
        pytest.fail(
            f"{readme.name} rows name skills that do not exist:\n  - {joined}\n\n"
            "Remove the row, or restore the skill it names."
        )


def test_headings_ignores_code_fence_comments(tmp_path: Path) -> None:
    """A ``#`` comment inside a fenced block is not a heading.

    Without fence tracking the extractor harvests Python comments, and a
    bogus section citation can then match one — the check silently stops
    catching the drift it exists to catch.
    """
    markdown = tmp_path / "sample.md"
    markdown.write_text(
        "# Real Heading\n"
        "\n"
        "```python\n"
        "# Fast path: no re-validation\n"
        "value = 1\n"
        "```\n"
        "\n"
        "## Second Real Heading\n",
        encoding="utf-8",
    )
    assert _headings(markdown) == {"real heading", "second real heading"}


def test_headings_strips_sub_lettered_ordinals(tmp_path: Path) -> None:
    """``2b.`` is stripped from a heading just as ``2.`` is.

    ``ref-configuration-conventions`` numbers its sub-rules ``2a``/``2b``/
    ``2c``; leaving the ordinal in place would make a correct citation of
    those sections fail.
    """
    markdown = tmp_path / "sample.md"
    markdown.write_text(
        "#### 2b. Every field carries a docstring\n"
        "\n"
        "### 3. Plain numbered rule\n"
        "\n"
        "## 4) Paren-style ordinal\n",
        encoding="utf-8",
    )
    assert _headings(markdown) == {
        "every field carries a docstring",
        "plain numbered rule",
        "paren-style ordinal",
    }


def test_section_citations_resolve_to_real_headings(
    skills: dict[str, Frontmatter],
) -> None:
    """Every section citation names a real heading.

    A citation is a backticked target followed by an emphasized section
    name. Three target forms are checked: another skill by name,
    ``AGENTS.md``, and ``SKILL.md`` — the last being a linked file
    pointing back at its own parent, resolved against the citing file's
    directory. Citing a bolded bullet or a renamed section produces a
    pointer the reader cannot follow, and nothing else detects it.
    """
    headings = {name: _headings(_SKILLS_DIR / name / "SKILL.md") for name in skills}
    agents_md = _REPO_ROOT / "AGENTS.md"
    agents_headings = _headings(agents_md) if agents_md.is_file() else set()

    broken: list[str] = []
    for markdown in _catalog_markdown():
        body = markdown.read_text(encoding="utf-8")
        where = markdown.relative_to(_REPO_ROOT)
        for match in _SECTION_CITATION.finditer(body):
            target, section = match.group(1), match.group(2)
            if target not in headings:
                continue
            if not _heading_matches(section, headings[target]):
                broken.append(
                    f"{where}: `{target}` *{section}* is not a heading in {target}"
                )
        for match in _AGENTS_CITATION.finditer(body):
            section = match.group(1)
            if agents_headings and not _heading_matches(section, agents_headings):
                broken.append(
                    f"{where}: `AGENTS.md` *{section}* is not a heading in AGENTS.md"
                )
        parent = markdown.parent / "SKILL.md"
        if markdown.name != "SKILL.md" and parent.is_file():
            parent_headings = _headings(parent)
            for match in _PARENT_CITATION.finditer(body):
                section = match.group(1)
                if not _heading_matches(section, parent_headings):
                    broken.append(
                        f"{where}: `SKILL.md` *{section}* is not a heading in"
                        f" {parent.relative_to(_REPO_ROOT)}"
                    )
    if broken:
        joined = "\n  - ".join(sorted(set(broken)))
        pytest.fail(
            f"Section citations must name a real heading:\n  - {joined}\n\n"
            "Cite the enclosing heading, or promote the target to a heading. "
            "For a numbered list, cite the number with its short name instead "
            "(for example, 'rule 15 (CLI is click + @run_async)')."
        )


def test_cited_skills_exist(skills: dict[str, Frontmatter]) -> None:
    """Every skill named in an ``Apply``/``See`` pointer exists on disk.

    A renamed or removed skill leaves pointers that name nothing; the
    reader has no way to tell a typo from a deleted skill.
    """
    directive = re.compile(r"\b(?:Apply|apply|See|see)\s+`([a-z][a-z0-9-]*)`")
    broken: list[str] = []
    for markdown in _catalog_markdown():
        body = markdown.read_text(encoding="utf-8")
        where = markdown.relative_to(_REPO_ROOT)
        for match in directive.finditer(body):
            named = match.group(1)
            if named not in skills and (_SKILLS_DIR / named).exists() is False:
                broken.append(f"{where}: names `{named}`, which is not a skill")
    if broken:
        joined = "\n  - ".join(sorted(set(broken)))
        pytest.fail(f"Skill pointers must name an existing skill:\n  - {joined}")


def test_bare_path_citations_resolve() -> None:
    """Every backticked ``.py`` path in the catalog names a real file.

    :func:`test_canonical_implementation_pointers_resolve` only inspects
    paths followed by a parenthetical symbol list, so a bare citation
    ("Enforced by ``tests/test_field_docs_contract.py``") went unchecked
    and survived a rename as an authoritative-looking dead reference.
    Paths are matched by suffix, so a citation may spell as much or as
    little of the path as reads well.
    """
    index = _python_files_by_suffix()
    broken: list[str] = []
    for markdown in _catalog_markdown():
        body = markdown.read_text(encoding="utf-8")
        where = markdown.relative_to(_REPO_ROOT)
        for match in _BARE_PATH.finditer(body):
            cited = match.group(1)
            if cited in _PLACEHOLDER_PATHS:
                continue
            if not index.get(cited):
                broken.append(f"{where}: `{cited}` matches no Python file")
    if broken:
        joined = "\n  - ".join(sorted(set(broken)))
        pytest.fail(
            f"Cited paths must resolve:\n  - {joined}\n\n"
            "Update the citation to the current path, or remove it."
        )


def test_canonical_implementation_pointers_resolve() -> None:
    """Every ``file.py (symbol)`` pointer names a real file and symbol.

    Canonical-implementation pointers are the catalog's defense against
    prose drift, which only holds while they resolve. A renamed symbol
    turns the pointer into a false citation that reads authoritative.
    """
    index = _python_files_by_suffix()
    broken: list[str] = []
    for markdown in _catalog_markdown():
        body = markdown.read_text(encoding="utf-8")
        where = markdown.relative_to(_REPO_ROOT)
        for match in _CODE_POINTER.finditer(body):
            cited_file, paren = match.group(1), match.group(2)
            if cited_file in _PLACEHOLDER_PATHS:
                continue
            if _POINTER_SYMBOL_ITEM.sub("", paren).strip():
                # Explanatory prose, not a symbol list. A parenthetical such as
                # "(`-` = stdin, unreadable file -> `file_read_failed`)"
                # describes behavior; its backticked tokens are values and
                # error codes, not definitions in the cited file.
                continue
            candidates = index.get(cited_file, [])
            if not candidates:
                broken.append(f"{where}: `{cited_file}` matches no Python file")
                continue
            sources = [path.read_text(encoding="utf-8") for path in candidates]
            for symbol in _POINTER_SYMBOL.findall(paren):
                if symbol.endswith(".py"):
                    continue
                if not any(symbol in source for source in sources):
                    broken.append(f"{where}: `{symbol}` not found in {cited_file}")
    if broken:
        joined = "\n  - ".join(sorted(set(broken)))
        pytest.fail(
            f"Canonical-implementation pointers must resolve:\n  - {joined}\n\n"
            "Update the pointer to the current path and symbol, or remove it."
        )
