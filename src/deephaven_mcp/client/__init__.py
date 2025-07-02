"""Deephaven client interface.

This module provides the primary client interface for interacting with Deephaven servers.
It contains wrappers and utilities for both standard and enterprise features, with enterprise
features conditionally available based on the presence of supporting packages.

The module includes:
- Client wrappers for sessions, queries, and authentication
- Base wrapper classes with asynchronous interfaces
- Feature detection for enterprise capabilities

Classes:
    ClientObjectWrapper: Generic base class for wrapping client objects with enhanced interfaces
    CoreSession: Wrapper for standard Deephaven session objects
    CorePlusSession: Wrapper for enterprise Deephaven session objects (when available)
    CorePlusAuthClient: Client for authentication with Deephaven servers

Attributes:
    is_enterprise_available (bool): Flag indicating if enterprise features are available
"""

import logging

from ._auth_client import CorePlusAuthClient
from ._base import ClientObjectWrapper, is_enterprise_available
from ._controller_client import CorePlusControllerClient
from ._protobuf import (
    CorePlusQueryConfig,
    CorePlusQueryInfo,
    CorePlusQuerySerial,
    CorePlusQueryState,
    CorePlusQueryStatus,
    CorePlusToken,
    ProtobufWrapper,
)
from ._session import CorePlusSession, CoreSession
from ._session_manager import CorePlusSessionManager

_LOGGER = logging.getLogger(__name__)

__all__ = [
    # Base wrapper class
    "ClientObjectWrapper",
    # Session classes
    "CoreSession",
    "CorePlusSession",
    # Session management
    "CorePlusSessionManager",
    # Authentication
    "CorePlusAuthClient",
    # Controller
    "CorePlusControllerClient",
    # Query and protobuf wrappers
    "ProtobufWrapper",
    "CorePlusQueryStatus",
    "CorePlusQuerySerial",
    "CorePlusQueryConfig",
    "CorePlusQueryState",
    "CorePlusQueryInfo",
    "CorePlusToken",
    # Feature detection
    "is_enterprise_available",
]
