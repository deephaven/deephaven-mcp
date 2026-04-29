"""Async Deephaven MCP configuration management.

This module provides the public surface for loading, validating, and managing
Deephaven MCP configuration from a JSON or JSON5 file. Specifically it exposes:

- Async configuration *manager classes* (:class:`CommunityServerConfigManager`,
  :class:`EnterpriseServerConfigManager`) whose ``get_config()`` methods load and
  cache the config file coroutine-safely.
- Synchronous *validation helpers* (:func:`validate_enterprise_config`,
  :func:`validate_community_session_config`) for programmatic validation
  of already-parsed configuration dictionaries.
- Synchronous *redaction helpers* (:func:`redact_community_session_config`,
  :func:`redact_enterprise_config`) for producing log-safe copies of
  configuration dictionaries.

Configuration is loaded from the path given by the ``DH_MCP_CONFIG_FILE``
environment variable (or an explicit ``config_path`` passed to the manager's
constructor, which takes precedence) using native async file I/O (``aiofiles``).
The configuration file supports both standard JSON and JSON5 formats. JSON5 allows single-line (//) and multi-line (/* */) comments, trailing commas, and other JSON5 features.

Two Config Formats, Two Manager Classes:
-----------------------------------------
This module supports two distinct configuration file formats, one per server type:

  1. **Community server** (``dh-mcp-community-server``): Use :class:`CommunityServerConfigManager`.
     The config file is a *flat* dict with ``sessions``, ``session_creation``, and ``security``
     as optional top-level keys. No enterprise-related keys are allowed.

  2. **Enterprise server** (``dh-mcp-enterprise-server``): Use :class:`EnterpriseServerConfigManager`.
     The config file is a *flat* dict with all enterprise system fields at the top level — there are
     no ``community`` or ``security`` sections. The enterprise schema is documented fully in the
     "Enterprise Server Configuration Schema" section below.

Features:
    - Coroutine-safe, cached loading of configuration using asyncio.Lock.
    - Strict validation of configuration structure and values.
    - Logging of configuration loading, environment variable value, and validation steps.
    - Uses aiofiles for non-blocking, native async config file reads.

Community Server Configuration Schema:
---------------------------------------
The community config file must be a JSON or JSON5 object. JSON5 allows single-line (//) and multi-line (/* */) comments, trailing commas, and other JSON5 features.
It may contain the following top-level keys (all optional):

  - `security` (dict, optional):
      Security settings for community sessions.
      If this key is present, its value must be a dictionary (which can be empty, e.g., {}).
      If this key is absent, all security settings use their secure defaults.
      The security configuration dict may contain:

        - `credential_retrieval_mode` (str, optional, default: "none"): Controls which community session credentials
          can be retrieved programmatically via the session_community_credentials MCP tool. Valid values:
            * "none": Credential retrieval disabled (secure default)
            * "dynamic_only": Only allow retrieval for auto-generated tokens (dynamic sessions)
            * "static_only": Only allow retrieval for pre-configured tokens (static sessions)
            * "all": Allow retrieval for both dynamic and static session credentials

  - `sessions` (dict, optional):
      A dictionary mapping community session names (str) to client session configuration dicts.
      If this key is present, its value must be a dictionary (which can be empty, e.g., {}).
      If this key is absent, it implies no static community sessions are configured.
      Each community session configuration dict may contain any of the following fields (all are optional):

        - `host` (str): Hostname or IP address of the community server.
        - `port` (int): Port number for the community server connection.
        - `auth_type` (str): Authentication type. Common values include:
            * "PSK" or "io.deephaven.authentication.psk.PskAuthenticationHandler": Pre-shared key authentication (shorthand and full class name).
            * "Anonymous": Default, no authentication required.
            * "Basic": HTTP Basic authentication (requires username:password format in auth_token).
            * Custom authenticator strings are also valid.
        - `auth_token` (str, optional): The direct authentication token or password. May be empty if `auth_type` is "Anonymous". Use this OR `auth_token_env_var`, but not both.
        - `auth_token_env_var` (str, optional): The name of an environment variable from which to read the authentication token. Use this OR `auth_token`, but not both.
        - `never_timeout` (bool): If True, sessions to this community server never time out.
        - `session_type` (str): Programming language for the session. Common values include:
            * "python": For Python-based Deephaven instances.
            * "groovy": For Groovy-based Deephaven instances.
        - `use_tls` (bool): Whether to use TLS/SSL for the connection.
        - `tls_root_certs` (str | None, optional): Path to a PEM file containing root certificates to trust for TLS.
        - `client_cert_chain` (str | None, optional): Path to a PEM file containing the client certificate chain for mutual TLS.
        - `client_private_key` (str | None, optional): Path to a PEM file containing the client private key for mutual TLS.

  - `session_creation` (dict, optional):
      Configuration for dynamically creating community sessions on demand.
      If this key is present, its value must be a dictionary (which can be empty, e.g., {}).
      If this key is absent, dynamic session creation is not configured.

Community Config Validation rules:
  - Only `sessions`, `session_creation`, `security`, and
    `mcp_session_idle_timeout_seconds` are valid top-level keys; any other key
    will cause validation to fail.
  - If `sessions` is present, each session's fields must have the correct type.
  - No unknown fields are permitted at any level of the configuration.
  - Sensitive values are redacted from logs for security:
      * `sessions.<name>.auth_token` is always redacted when truthy.
      * `session_creation.defaults.auth_token` is always redacted when present.
      * The TLS key-material fields `tls_root_certs`, `client_cert_chain`, and
        `client_private_key` are redacted only when the stored value is binary
        (`bytes` / `bytearray`); string values such as filesystem paths are
        logged as-is.
    (Note: the enterprise server uses a completely separate flat config format via EnterpriseServerConfigManager.)

Enterprise Server Configuration Schema:
-----------------------------------------
The enterprise config file is a flat JSON or JSON5 object. All fields sit at the top level; there
are no ``community`` or ``security`` sections. Each ``dh-mcp-enterprise-server`` instance is
configured for exactly one enterprise system.

Required fields:

  - `system_name` (str): Human-readable identifier for this enterprise system.
      Used as the ``source`` component in all session identifiers (e.g. ``"enterprise:prod:my-pq"``).

  - `connection_json_url` (str): Full URL to the Core+ ``connection.json`` endpoint
      (e.g. ``"https://dhe.example.com/iris/connection.json"``).

  - `auth_type` (str): Authentication method. Must be one of:
      * ``"password"``: Username/password authentication. Requires ``username`` and either
        ``password`` or ``password_env_var`` (mutually exclusive).
      * ``"private_key"``: Private key file authentication. Requires ``private_key_path``.

Authentication fields (required when auth_type is "password"):

  - `username` (str): Username for authentication.
  - `password` (str): Password in plaintext. Use this OR ``password_env_var``, not both.
  - `password_env_var` (str): Name of an environment variable holding the password.
      Use this OR ``password``, not both. Preferred over hardcoding the password.

Authentication fields (required when auth_type is "private_key"):

  - `private_key_path` (str): Filesystem path to the private key file used for authentication.

Optional fields:

  - `connection_timeout` (int | float, > 0): Connection timeout in seconds.
      Default: ``10.0``. Booleans are not accepted even though bool is a subclass of int.

  - `session_creation` (dict, optional): Session lifecycle configuration.
      If absent, dynamic session creation uses server defaults.

Enterprise Config Validation rules:
  - ``system_name``, ``connection_json_url``, and ``auth_type`` are always required.
  - ``auth_type`` must be exactly ``"password"`` or ``"private_key"``; no custom values.
  - For ``"password"`` auth: ``username`` is required; exactly one of ``password`` or
    ``password_env_var`` must be present.
  - For ``"private_key"`` auth: ``private_key_path`` is required.
  - ``connection_timeout`` must be a positive number if present; booleans are rejected.
  - ``max_concurrent_sessions`` must be a non-negative integer if present.
  - When ``session_creation`` is present, ``defaults`` is required and
    ``defaults.heap_size_gb`` is required.
  - Unknown fields are rejected at every level (top level, ``session_creation``,
    and ``session_creation.defaults``).
  - Sensitive field ``password`` is redacted in logs.

Environment Variables:
---------------------
- `DH_MCP_CONFIG_FILE`: Path to the Deephaven MCP configuration JSON or JSON5 file.

Security:
---------
- Sensitive information (such as authentication tokens and passwords) is redacted in logs.
- Environment variable values are logged for debugging.

Async/Await & I/O:
------------------
- All configuration loading is async and coroutine-safe.
- File I/O uses `aiofiles` for non-blocking reads.

"""

__all__ = [
    # Config manager base and concrete types
    "ConfigManager",
    "CommunityServerConfigManager",
    "EnterpriseServerConfigManager",
    # Constants
    "CONFIG_ENV_VAR",
    "DEFAULT_MCP_SESSION_IDLE_TIMEOUT_SECONDS",
    # Validators used by external callers
    "validate_enterprise_config",
    "validate_community_session_config",
    # Redaction used by external callers
    "redact_community_session_config",
    "redact_enterprise_config",
    # Constants used by external callers
    "DEFAULT_CONNECTION_TIMEOUT_SECONDS",
    # Exceptions
    "ConfigurationError",
]

from deephaven_mcp._exceptions import ConfigurationError

from ._base import (
    CONFIG_ENV_VAR,
    DEFAULT_MCP_SESSION_IDLE_TIMEOUT_SECONDS,
    ConfigManager,
)
from .community import (
    CommunityServerConfigManager,
    redact_community_config,
    redact_community_session_config,
    validate_community_config,
    validate_community_session_config,
)
from .enterprise import (
    DEFAULT_CONNECTION_TIMEOUT_SECONDS,
    EnterpriseServerConfigManager,
    redact_enterprise_config,
    validate_enterprise_config,
)
