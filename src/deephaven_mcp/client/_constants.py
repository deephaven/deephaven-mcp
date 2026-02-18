"""Timeout constants for the Deephaven client API.

All timeout values are in seconds for consistency across the API.
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
# Used for get() and get_serial_for_name() to check current state without blocking.
NO_WAIT_SECONDS: float = 0.0
