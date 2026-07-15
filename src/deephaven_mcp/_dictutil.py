"""Dictionary utilities: recursive merging of nested mappings."""

from __future__ import annotations

__all__ = ["deep_merge"]


def deep_merge(
    base: dict[str, object], override: dict[str, object]
) -> dict[str, object]:
    """Return ``base`` with ``override`` merged in, recursing into nested dicts.

    Args:
        base (dict[str, object]): The starting mapping (not mutated).
        override (dict[str, object]): Partial mapping whose values win;
            nested dicts merge key-by-key, any other value replaces the
            base value outright.

    Returns:
        dict[str, object]: A new merged mapping.
    """
    merged = dict(base)
    for key, value in override.items():
        existing = merged.get(key)
        if isinstance(existing, dict) and isinstance(value, dict):
            merged[key] = deep_merge(existing, value)
        else:
            merged[key] = value
    return merged
