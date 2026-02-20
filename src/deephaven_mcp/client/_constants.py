"""Timeout constants for the Deephaven client API.

All timeout values are in seconds (float) for consistency across the API.
These constants define default timeouts for various network operations.

Connection Timeouts:
    CONNECTION_TIMEOUT_SECONDS: Initial connection to Deephaven server (60s).
    SUBSCRIBE_TIMEOUT_SECONDS: Subscribing to controller state updates (60s).
    PQ_CONNECTION_TIMEOUT_SECONDS: Connecting to persistent queries (60s).
    WORKER_CREATION_TIMEOUT_SECONDS: Creating new workers (60s).

Authentication Timeouts:
    AUTH_TIMEOUT_SECONDS: Password and private key authentication (60s).
    SAML_AUTH_TIMEOUT_SECONDS: SAML authentication with browser interaction (120s).

Persistent Query Operation Timeouts:
    PQ_OPERATION_TIMEOUT_SECONDS: Query management operations (60s).
    QUICK_OPERATION_TIMEOUT_SECONDS: Quick operations like ping (30s).
    PQ_WAIT_TIMEOUT_SECONDS: Waiting for query state changes (120s).

Special Values:
    NO_WAIT_SECONDS: Return immediately without waiting (0s).
"""

# =============================================================================
# Connection Timeouts
# =============================================================================

# Timeout for establishing initial connection to the Deephaven server.
# Used when creating SessionManager instances.
CONNECTION_TIMEOUT_SECONDS: float = 60.0

# Timeout for subscribing to controller state updates.
SUBSCRIBE_TIMEOUT_SECONDS: float = 60.0

# Timeout for connecting to persistent queries.
PQ_CONNECTION_TIMEOUT_SECONDS: float = 60.0

# Timeout for creating new workers via connect_to_new_worker.
# This is passed to the SDK and also used for our outer safety wrapper.
WORKER_CREATION_TIMEOUT_SECONDS: float = 60.0

# =============================================================================
# Authentication Timeouts
# =============================================================================

# Timeout for authentication operations (password, private_key).
AUTH_TIMEOUT_SECONDS: float = 60.0

# SAML uses a longer timeout due to potential browser interaction.
SAML_AUTH_TIMEOUT_SECONDS: float = 120.0

# =============================================================================
# Persistent Query Operation Timeouts
# =============================================================================

# Timeout for persistent query management operations (add, delete, modify, etc.).
PQ_OPERATION_TIMEOUT_SECONDS: float = 60.0

# Timeout for quick network operations like ping, delete_key, upload_key.
QUICK_OPERATION_TIMEOUT_SECONDS: float = 30.0

# Timeout for waiting on query state changes (start_and_wait, stop_and_wait).
# These operations wait for workers to reach a target state.
PQ_WAIT_TIMEOUT_SECONDS: float = 120.0

# =============================================================================
# Special Values
# =============================================================================

# Value of 0 means "return immediately without waiting" in the SDK.
NO_WAIT_SECONDS: float = 0.0
