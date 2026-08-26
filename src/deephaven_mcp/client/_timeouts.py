"""Client-layer timeout configuration.

Defines :class:`CommunityClientTimeouts` and
:class:`EnterpriseClientTimeouts`, two independent Pydantic v2 schemas
carrying the timeouts the community and enterprise *client* layers
apply to outbound Deephaven calls and persistent-query state waits.

Schema location and consumers:

- :class:`CommunityClientTimeouts` is consumed by
  :class:`~deephaven_mcp.client.CoreSession` and threaded through the
  community session registry / manager construction path. Loaded from
  ``community/settings.json: timeouts.client``.
- :class:`EnterpriseClientTimeouts` is consumed by
  :class:`~deephaven_mcp.client.CorePlusSessionFactory`,
  :class:`~deephaven_mcp.client.CorePlusControllerClient`, and
  :class:`~deephaven_mcp.client.CorePlusAuthClient`. Loaded from
  ``enterprise/settings.json: timeouts.client``.

The umbrella ``CommunityTimeouts`` / ``EnterpriseTimeouts`` schemas
that wrap these classes (alongside an ``eviction`` block) live in
:mod:`deephaven_mcp.config.schema` next to the operator
configuration loader. The eviction block lives with its consumer in
:mod:`deephaven_mcp.resource_manager._evictor`. This module owns only
the client-layer timeouts.

Every field carries its schema-level default so the JSON block is
fully optional. Authors who want to pull a value from an environment
variable write ``"<field>": "${env:NAME}"`` in the source JSON; the
templating engine resolves the placeholder before validation.
"""

from __future__ import annotations

__all__ = [
    "CommunityClientTimeouts",
    "EnterpriseClientTimeouts",
]

from typing import Annotated

from pydantic import Field

from deephaven_mcp._pydantic import StrictSchema


class CommunityClientTimeouts(StrictSchema):
    """Timeouts the Deephaven Community client layer applies to outbound calls.

    Consumed by :class:`~deephaven_mcp.client.CoreSession` and threaded
    through the community session registry / manager construction path.
    """

    session_connect_timeout_seconds: Annotated[float, Field(gt=0)] = 60.0
    """Timeout (seconds) for establishing the initial connection to a
    Deephaven Community server. Increase when first-contact setup is
    slow (TLS handshake, DNS, server cold-start)."""


class EnterpriseClientTimeouts(StrictSchema):
    """Timeouts the Deephaven Enterprise client layer applies to outbound calls.

    Consumed by :class:`~deephaven_mcp.client.CorePlusSessionFactory`
    (worker provisioning, persistent-query provisioning, authentication,
    SAML, lightweight key/ping ops) and
    :class:`~deephaven_mcp.client.CorePlusControllerClient` (subscribe,
    persistent-query CRUD, persistent-query state-change, non-blocking
    lookups). Threaded through the enterprise registry / factory
    construction path.

    ``pq_state_change_timeout_seconds`` is ``int``-typed:
    whole-second resolution; sub-second timing is not supported.
    """

    session_connect_timeout_seconds: Annotated[float, Field(gt=0)] = 60.0
    """Timeout (seconds) for establishing the initial connection to a
    Deephaven Enterprise server. Increase when first-contact setup is
    slow (TLS handshake, DNS, server cold-start)."""

    worker_creation_timeout_seconds: Annotated[float, Field(gt=0)] = 60.0
    """Timeout (seconds) for provisioning and connecting to a new
    on-demand worker. Increase when workers take longer than a minute
    to schedule, container-pull, or start their JVM."""

    pq_connection_timeout_seconds: Annotated[float, Field(gt=0)] = 60.0
    """Timeout (seconds) for opening a session to a running persistent
    query worker. Distinct from worker creation: this covers the
    client-to-worker session handshake, not the worker's startup."""

    auth_timeout_seconds: Annotated[float, Field(gt=0)] = 60.0
    """Timeout (seconds) for standard (non-SAML) authentication
    operations: password, private-key, and service-token retrieval.
    Increase when the auth server is slow to verify credentials."""

    saml_auth_timeout_seconds: Annotated[float, Field(gt=0)] = 120.0
    """Timeout (seconds) for SAML authentication. Larger than
    ``auth_timeout_seconds`` because SAML involves a browser redirect
    and user interaction at the identity provider."""

    quick_operation_timeout_seconds: Annotated[float, Field(gt=0)] = 5.0
    """Timeout (seconds) for lightweight network round-trips that
    must fail fast: ping, public-key upload/delete. Increase only if
    routine network latency exceeds a few seconds."""

    subscribe_timeout_seconds: Annotated[float, Field(gt=0)] = 30.0
    """Timeout (seconds) for subscribing to controller state updates
    (the initial snapshot must arrive within this budget)."""

    controller_resubscribe_recreate_interval_seconds: Annotated[float, Field(gt=0)] = (
        30.0
    )
    """Cadence (seconds) at which a wedged controller subscription is
    healed. When the controller subscription is stuck initializing, a
    background healer recreates the enterprise factory on this interval
    until the connection recovers; controller-backed calls fail fast with
    a status message meanwhile instead of blocking on the vendor
    subscription timeout. Increase to reduce recreate churn on a slow
    controller; decrease to recover faster once it becomes reachable."""

    pq_management_timeout_seconds: Annotated[float, Field(gt=0)] = 60.0
    """Timeout (seconds) for persistent-query management RPCs (add,
    delete, modify). These mutate controller state without waiting
    for the PQ to reach a target running state."""

    pq_state_change_timeout_seconds: Annotated[int, Field(gt=0)] = 120
    """Timeout (integer seconds) for waiting on a persistent query
    to reach a target lifecycle state (e.g. RUNNING after start, or
    STOPPED after stop). Whole-second resolution; sub-second timing
    is not supported."""

    no_wait_seconds: Annotated[float, Field(ge=0)] = 0.0
    """Wait window (seconds) for controller subscription-map lookups
    (``get_serial_for_name`` / ``get``). The default ``0.0`` makes the
    lookup fail immediately when the entry is not yet in the
    subscription snapshot. Set a small positive value to tolerate
    races between query creation and the snapshot catching up;
    most operators can leave this at the default."""
