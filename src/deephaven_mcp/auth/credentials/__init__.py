"""Identity and bearer-material data types for the ``auth`` framework.

This subpackage holds **pure data** — the verified caller identity
(:class:`Principal`) and the mechanism-only credential dataclasses
returned by backends (:class:`PSKCredentials`,
:class:`PasswordCredentials`, :class:`PrivateKeyCredentials`), all of
which inherit from the abstract base :class:`Credentials`.

The module has **no behavioral coupling** to the rest of the system: it
imports only from the standard library and is therefore importable from
any consumer (a session factory, a future CLI, tests) without dragging
in backend or middleware machinery.
"""

from ._credentials import (
    Credentials,
    PasswordCredentials,
    PrivateKeyCredentials,
    PSKCredentials,
)
from ._principal import Principal

__all__ = [
    "Credentials",
    "PasswordCredentials",
    "Principal",
    "PrivateKeyCredentials",
    "PSKCredentials",
]
