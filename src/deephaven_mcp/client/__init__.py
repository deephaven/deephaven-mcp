"""
Deephaven Python Client Interface

This module provides the main entry point for interacting with Deephaven servers via Python. It exposes
all major client wrappers and utilities for both standard and enterprise (Core+) features. All classes
and utilities are imported from submodules and re-exported for convenience.

Features:
    - Async and sync wrappers for sessions, queries, authentication, and controllers
    - Base wrapper classes with enhanced and asynchronous interfaces
    - Automatic detection of enterprise feature availability
    - All logging is handled in submodules; this file does not log

Exported Classes and Attributes:
    ClientObjectWrapper           -- Base class for client wrappers
    CoreSession                   -- Async wrapper for standard Deephaven sessions
    CorePlusSession               -- Async wrapper for enterprise Deephaven sessions
    CorePlusAuthClient            -- Async authentication client
    CorePlusControllerClient      -- Async controller client for persistent queries
    CorePlusQueryConfig           -- Wrapper for persistent query configuration
    CorePlusQueryInfo             -- Wrapper for persistent query information
    CorePlusQuerySerial           -- Wrapper for persistent query serial numbers
    CorePlusQueryState            -- Wrapper for persistent query state
    CorePlusQueryStatus           -- Wrapper for persistent query status
    CorePlusToken                 -- Wrapper for authentication tokens
    ProtobufWrapper               -- Base wrapper for protobuf messages
    BaseSession                   -- Base async session wrapper
    CorePlusSessionFactory        -- Factory for creating enterprise sessions
    is_enterprise_available (bool) -- True if enterprise features are available

Note:
    All logging is performed in the respective submodules/classes; this file does not log directly.
"""

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
from ._session import BaseSession, CoreSession, CorePlusSession
from ._session_factory import CorePlusSessionFactory

__all__ = [
    "CorePlusAuthClient",
    "ClientObjectWrapper",
    "is_enterprise_available",
    "CorePlusControllerClient",
    "CorePlusQueryConfig",
    "CorePlusQueryInfo",
    "CorePlusQuerySerial",
    "CorePlusQueryState",
    "CorePlusQueryStatus",
    "CorePlusToken",
    "ProtobufWrapper",
    "BaseSession",
    "CoreSession",
    "CorePlusSession",
    "CorePlusSessionFactory",
]
