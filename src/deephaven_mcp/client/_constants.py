"""Timeout constants for the Deephaven client API.

Most timeout values are in seconds (float). A small number are integer
seconds because they are forwarded verbatim to upstream APIs whose typed
stubs declare ``int`` (currently: ``PQ_STATE_CHANGE_TIMEOUT_SECONDS``,
which feeds ``ControllerClient.start_and_wait`` / ``stop_and_wait``).
The type of each constant is documented at its definition site.

Each constant can be overridden at process startup by setting the
corresponding environment variable. The environment variable must be
parseable as the constant's declared type; invalid values raise a
``ValueError`` at import time.
"""

from deephaven_mcp._env import env_float, env_int

SESSION_CONNECT_TIMEOUT_SECONDS: float = env_float(
    "DH_MCP_SESSION_CONNECT_TIMEOUT_SECONDS", 60.0
)
"""Timeout (seconds) for establishing the initial connection to a Deephaven server.

Default for the three initial-connection entry points:

- ``CorePlusSessionFactory.from_url`` — wraps the ``SessionManager(url)``
  constructor (TCP/TLS handshake plus ``connection.json`` retrieval).
- ``CorePlusSessionFactory.from_config`` — wraps the ``SessionManager``
  constructor *and* the subsequent controller subscription.
- ``CoreSession.from_config`` — wraps the ``pydeephaven.Session`` constructor
  for community Core (no ``connection.json`` involved).

Increase this on slow or high-latency networks, or when the controller has many
persistent queries to enumerate during the post-connect subscription.
Environment variable override: DH_MCP_SESSION_CONNECT_TIMEOUT_SECONDS
"""

SUBSCRIBE_TIMEOUT_SECONDS: float = env_float("DH_MCP_SUBSCRIBE_TIMEOUT_SECONDS", 30.0)
"""Timeout (seconds) for subscribing to controller state updates.

Covers the time for the controller to deliver its initial PQ state snapshot.
Increase this if the controller manages a very large number of persistent queries.
Environment variable override: DH_MCP_SUBSCRIBE_TIMEOUT_SECONDS
"""

PQ_CONNECTION_TIMEOUT_SECONDS: float = env_float(
    "DH_MCP_PQ_CONNECTION_TIMEOUT_SECONDS", 60.0
)
"""Timeout (seconds) for opening a session to a running persistent query worker.

Distinct from SESSION_CONNECT_TIMEOUT_SECONDS, which covers the initial server
connection; this covers the worker-level connection after the PQ is already running.
Environment variable override: DH_MCP_PQ_CONNECTION_TIMEOUT_SECONDS
"""

WORKER_CREATION_TIMEOUT_SECONDS: float = env_float(
    "DH_MCP_WORKER_CREATION_TIMEOUT_SECONDS", 60.0
)
"""Timeout (seconds) for provisioning and connecting to a new on-demand worker.

Covers JVM startup plus the initial connection handshake. Increase this on
systems where worker startup is slow or resources are contended.
Environment variable override: DH_MCP_WORKER_CREATION_TIMEOUT_SECONDS
"""

AUTH_TIMEOUT_SECONDS: float = env_float("DH_MCP_AUTH_TIMEOUT_SECONDS", 60.0)
"""Timeout (seconds) for standard authentication operations (password, private_key).

Covers credential exchange with the server. See SAML_AUTH_TIMEOUT_SECONDS for
the longer timeout used when browser interaction is required.
Environment variable override: DH_MCP_AUTH_TIMEOUT_SECONDS
"""

SAML_AUTH_TIMEOUT_SECONDS: float = env_float("DH_MCP_SAML_AUTH_TIMEOUT_SECONDS", 120.0)
"""Timeout (seconds) for SAML authentication.

Longer than AUTH_TIMEOUT_SECONDS to accommodate the browser redirect roundtrip
that SAML requires before the server can complete the handshake.
Environment variable override: DH_MCP_SAML_AUTH_TIMEOUT_SECONDS
"""

PQ_MANAGEMENT_TIMEOUT_SECONDS: float = env_float(
    "DH_MCP_PQ_MANAGEMENT_TIMEOUT_SECONDS", 60.0
)
"""Timeout (seconds) for persistent query management operations (add, delete, modify).

Default for ``ControllerClient.add_query`` / ``delete_query`` / ``modify_query``;
covers the controller round-trip to register, remove, or update a PQ definition.
Does not cover waiting for a worker to reach a target state — see
``PQ_STATE_CHANGE_TIMEOUT_SECONDS`` for that.
Environment variable override: DH_MCP_PQ_MANAGEMENT_TIMEOUT_SECONDS
"""

QUICK_OPERATION_TIMEOUT_SECONDS: float = env_float(
    "DH_MCP_QUICK_OPERATION_TIMEOUT_SECONDS", 5.0
)
"""Timeout (seconds) for lightweight network round-trips (ping, key management).

Kept short (5s default) because these calls should complete near-instantly;
a timeout here typically indicates a connectivity problem rather than slow work.
Environment variable override: DH_MCP_QUICK_OPERATION_TIMEOUT_SECONDS
"""

PQ_STATE_CHANGE_TIMEOUT_SECONDS: int = env_int(
    "DH_MCP_PQ_STATE_CHANGE_TIMEOUT_SECONDS", 120
)
"""Timeout (integer seconds) for waiting on persistent query state transitions.

Covers the time from issuing a start or restart to the worker reaching its target
state (e.g. RUNNING). Increase this for PQs with large heaps or slow init scripts.

This constant is ``int`` (not ``float``) because it is forwarded verbatim to
``ControllerClient.start_and_wait`` / ``stop_and_wait``, whose typed stubs
declare ``timeout_seconds: int``. Fractional env-var values
(e.g. ``DH_MCP_PQ_STATE_CHANGE_TIMEOUT_SECONDS=120.5``) raise ``ValueError``
at import time rather than being silently truncated by the underlying Java.

Environment variable override: DH_MCP_PQ_STATE_CHANGE_TIMEOUT_SECONDS
"""

NO_WAIT_SECONDS: float = env_float("DH_MCP_NO_WAIT_SECONDS", 0.0)
"""Sentinel value (0s, float) for subscription-map lookups that should not block.

Default for ``ControllerClient.get`` and ``ControllerClient.get_serial_for_name``,
which look up persistent queries in the controller's local subscription map.
A value of ``0`` instructs those methods to raise immediately if the requested
query is not already present in the map (rather than waiting for it to appear).
Override to a positive value if the caller needs to wait for an in-flight
query registration to land in the snapshot.
Environment variable override: DH_MCP_NO_WAIT_SECONDS
"""
