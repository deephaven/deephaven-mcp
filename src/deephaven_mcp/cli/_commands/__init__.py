"""Click subcommand modules for the ``dh-mcp`` CLI.

Each module under this package exposes one click command that the
root in :mod:`deephaven_mcp.cli._main` mounts as a top-level noun: a
:class:`click.Group` (``daemon``, ``tool``, ``config``, ``introspect``,
...) with verbs underneath. Async callbacks are wrapped with
:func:`deephaven_mcp.cli._async.run_async` and signal failures via
:class:`deephaven_mcp.cli._errors.CliError`.
"""
