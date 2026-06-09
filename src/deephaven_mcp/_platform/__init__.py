"""OS abstraction layer: the home for code that branches on ``os.name``.

Membership rule
---------------

A module belongs here if and only if it dispatches on the operating
system — a ``match os.name`` (or equivalent) with a POSIX arm and a
Windows arm. Centralizing every such branch in one package means
adding support for a new OS is a bounded edit: touch the arms in these
submodules, nothing else.

OS-*portable* primitives stay out, even when they concern processes or
the filesystem. The canonical example is
:mod:`deephaven_mcp._processes` (PID + create-time identity and
signaling): it runs the same code on every platform via
:mod:`psutil` / :func:`os.kill` and so lives at the package top level,
not here. Compare with :mod:`deephaven_mcp._platform.spawn`, whose
detached-spawn mechanics *do* branch on ``os.name`` and therefore
belong here.

Submodules:

- :mod:`deephaven_mcp._platform.fsutil` — advisory file locking, atomic
  private writes, and Windows-retry filesystem helpers.
- :mod:`deephaven_mcp._platform.spawn` — detached background-process
  spawn.
- :mod:`deephaven_mcp._platform.dir_permissions` — per-OS private-dir
  hardening and permission auditing.
"""

# This package ``__init__`` imports no submodules: the shared OS-support
# contract is an internal leaf (``._os_support``) that submodules import
# directly, so keeping ``__init__`` import-free avoids any
# partially-initialized-package import cycle.
