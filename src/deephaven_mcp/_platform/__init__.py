"""OS abstraction layer: the home for code that branches on ``os.name``.

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
