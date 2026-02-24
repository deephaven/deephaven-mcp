"""Timeout constants for the Deephaven client API.

All timeout values are in seconds (float) for consistency across the API.
Each constant can be overridden at process startup by setting the corresponding
environment variable.  The environment variable must be parseable as a float;
invalid values raise a ValueError at import time.
"""

import os

SESSION_CONNECT_TIMEOUT_SECONDS: float = float(
    os.environ.get("DH_MCP_SESSION_CONNECT_TIMEOUT_SECONDS", "60.0")
)
"""Timeout (seconds) for establishing the initial connection to the Deephaven server.

Covers the TCP/TLS handshake and connection.json retrieval phase. Increase this
on slow or high-latency networks.
Environment variable override: DH_MCP_SESSION_CONNECT_TIMEOUT_SECONDS
"""

SUBSCRIBE_TIMEOUT_SECONDS: float = float(
    os.environ.get("DH_MCP_SUBSCRIBE_TIMEOUT_SECONDS", "30.0")
)
"""Timeout (seconds) for subscribing to controller state updates.

Covers the time for the controller to deliver its initial PQ state snapshot.
Increase this if the controller manages a very large number of persistent queries.
Environment variable override: DH_MCP_SUBSCRIBE_TIMEOUT_SECONDS
"""

PQ_CONNECTION_TIMEOUT_SECONDS: float = float(
    os.environ.get("DH_MCP_PQ_CONNECTION_TIMEOUT_SECONDS", "60.0")
)
"""Timeout (seconds) for opening a session to a running persistent query worker.

Distinct from SESSION_CONNECT_TIMEOUT_SECONDS, which covers the initial server
connection; this covers the worker-level connection after the PQ is already running.
Environment variable override: DH_MCP_PQ_CONNECTION_TIMEOUT_SECONDS
"""

WORKER_CREATION_TIMEOUT_SECONDS: float = float(
    os.environ.get("DH_MCP_WORKER_CREATION_TIMEOUT_SECONDS", "60.0")
)
"""Timeout (seconds) for provisioning and connecting to a new on-demand worker.

Covers JVM startup plus the initial connection handshake. Increase this on
systems where worker startup is slow or resources are contended.
Environment variable override: DH_MCP_WORKER_CREATION_TIMEOUT_SECONDS
"""

AUTH_TIMEOUT_SECONDS: float = float(
    os.environ.get("DH_MCP_AUTH_TIMEOUT_SECONDS", "60.0")
)
"""Timeout (seconds) for standard authentication operations (password, private_key).

Covers credential exchange with the server. See SAML_AUTH_TIMEOUT_SECONDS for
the longer timeout used when browser interaction is required.
Environment variable override: DH_MCP_AUTH_TIMEOUT_SECONDS
"""

SAML_AUTH_TIMEOUT_SECONDS: float = float(
    os.environ.get("DH_MCP_SAML_AUTH_TIMEOUT_SECONDS", "120.0")
)
"""Timeout (seconds) for SAML authentication.

Longer than AUTH_TIMEOUT_SECONDS to accommodate the browser redirect roundtrip
that SAML requires before the server can complete the handshake.
Environment variable override: DH_MCP_SAML_AUTH_TIMEOUT_SECONDS
"""

PQ_MANAGEMENT_TIMEOUT_SECONDS: float = float(
    os.environ.get("DH_MCP_PQ_MANAGEMENT_TIMEOUT_SECONDS", "60.0")
)
"""Timeout (seconds) for persistent query management operations (add, delete, modify, stop).

Covers the controller round-trip to register or remove a PQ definition. Does not
cover waiting for a worker to reach a target state — see PQ_STATE_CHANGE_TIMEOUT_SECONDS.
Environment variable override: DH_MCP_PQ_MANAGEMENT_TIMEOUT_SECONDS
"""

QUICK_OPERATION_TIMEOUT_SECONDS: float = float(
    os.environ.get("DH_MCP_QUICK_OPERATION_TIMEOUT_SECONDS", "5.0")
)
"""Timeout (seconds) for lightweight network round-trips (ping, key management).

Kept short (5s default) because these calls should complete near-instantly;
a timeout here typically indicates a connectivity problem rather than slow work.
Environment variable override: DH_MCP_QUICK_OPERATION_TIMEOUT_SECONDS
"""

PQ_STATE_CHANGE_TIMEOUT_SECONDS: float = float(
    os.environ.get("DH_MCP_PQ_STATE_CHANGE_TIMEOUT_SECONDS", "120.0")
)
"""Timeout (seconds) for waiting on persistent query state transitions.

Covers the time from issuing a start or restart to the worker reaching its target
state (e.g. RUNNING). Increase this for PQs with large heaps or slow init scripts.
Environment variable override: DH_MCP_PQ_STATE_CHANGE_TIMEOUT_SECONDS
"""

NO_WAIT_SECONDS: float = float(os.environ.get("DH_MCP_NO_WAIT_SECONDS", "0.0"))
"""Sentinel value (0s) passed as timeout_seconds to controller methods.

A value of 0 means "fire and forget" — the call returns immediately without
waiting for the query to reach a target state. Overriding this is rarely useful.
Environment variable override: DH_MCP_NO_WAIT_SECONDS
"""
