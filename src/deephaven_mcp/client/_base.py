"""Base classes and utilities for the Deephaven client interface.

This module provides common base classes and utility functions used throughout the
Deephaven client package. It contains functionality that is shared between both
standard and enterprise client components.

The primary purpose of this module is to establish a consistent wrapping pattern
for Java client objects, providing them with enhanced Pythonic interfaces and
asynchronous capabilities. It handles feature detection for enterprise components
and provides appropriate error handling when required features are not available.

The wrapping pattern implemented here enables several key benefits:
1. Transparent conversion of blocking Java calls to non-blocking Python async calls
2. Enhanced error handling with more descriptive Python exceptions
3. Consistent logging across all client components
4. Type safety through generic typing

Classes:
    ClientObjectWrapper: Generic base class for wrapping client objects with enhanced interfaces

Attributes:
    is_enterprise_available (bool): Flag indicating if enterprise features are available
                                   in the current environment. This is determined by attempting
                                   to import the deephaven_enterprise package.
"""

import logging
from typing import Generic, TypeVar

from deephaven_mcp._exceptions import InternalError

_LOGGER = logging.getLogger(__name__)

# Check for enterprise features
is_enterprise_available = False
try:
    # The following imports are required for enterprise features
    import deephaven_enterprise  # noqa: F401

    # # TODO: Workaround: Explicitly import all enterprise proto modules to ensure correct namespace setup -- see https://deephaven.atlassian.net/browse/DH-19813
    # # The following imports are required to ensure that the proto modules are loaded
    # import deephaven_enterprise.proto.acl_pb2  # noqa: F401
    # import deephaven_enterprise.proto.acl_pb2_grpc  # noqa: F401
    # import deephaven_enterprise.proto.auth_pb2  # noqa: F401
    # import deephaven_enterprise.proto.auth_pb2_grpc  # noqa: F401
    # import deephaven_enterprise.proto.auth_service_pb2  # noqa: F401
    # import deephaven_enterprise.proto.auth_service_pb2_grpc  # noqa: F401
    # import deephaven_enterprise.proto.common_pb2  # noqa: F401
    # import deephaven_enterprise.proto.common_pb2_grpc  # noqa: F401
    # import deephaven_enterprise.proto.controller_common_pb2  # noqa: F401
    # import deephaven_enterprise.proto.controller_common_pb2_grpc  # noqa: F401
    # import deephaven_enterprise.proto.controller_pb2  # noqa: F401
    # import deephaven_enterprise.proto.controller_pb2_grpc  # noqa: F401
    # import deephaven_enterprise.proto.controller_service_pb2  # noqa: F401
    # import deephaven_enterprise.proto.controller_service_pb2_grpc  # noqa: F401
    # import deephaven_enterprise.proto.persistent_query_pb2  # noqa: F401
    # import deephaven_enterprise.proto.persistent_query_pb2_grpc  # noqa: F401
    # import deephaven_enterprise.proto.table_definition_pb2  # noqa: F401
    # import deephaven_enterprise.proto.table_definition_pb2_grpc  # noqa: F401

    is_enterprise_available = True
    _LOGGER.debug("Enterprise features available")
except ImportError:
    _LOGGER.debug("Enterprise features not available")

# Type variable for the wrapped object
T = TypeVar("T")


class ClientObjectWrapper(Generic[T]):
    """Base class for client wrappers with generic type support.

    This class serves as a foundation for wrappers around Deephaven client objects.
    It provides common functionality for all client wrappers, such as access to the
    underlying wrapped client object. The generic type parameter T represents the
    type of the wrapped object, ensuring type safety throughout inheritance.

    Purpose:
        1. Provide a consistent pattern for wrapping Java client objects with Python interfaces
        2. Enable non-blocking asynchronous access to potentially blocking Java methods
        3. Ensure proper detection and handling of enterprise feature requirements
        4. Establish a consistent error handling pattern across client components

    Usage Pattern:
        When extending this class, implementers should:
        1. Initialize with the object to be wrapped and specify whether it requires enterprise features
        2. Create async wrapper methods that delegate to the underlying wrapped client object
        3. Use asyncio.to_thread for potentially blocking operations to prevent blocking the event loop
        4. Add enhanced error handling by catching Java exceptions and translating them to Python exceptions
        5. Implement consistent logging patterns for method entry, success, and error conditions

    Design Philosophy:
        The wrapper pattern implemented here follows the Adapter design pattern, providing a
        more Pythonic interface to underlying Java components. The asynchronous methods ensure
        that Python applications using these wrappers can maintain responsiveness even when
        interacting with potentially blocking Java operations.

    Examples:
        from typing import List
        import asyncio
        # Example assumes a user-defined class DeephavenTable
        class DeephavenTable:
            ...

        class TableWrapper(ClientObjectWrapper[DeephavenTable]):
            def __init__(self, table: DeephavenTable):
                super().__init__(table, is_enterprise=False)

            async def get_column_names(self) -> List[str]:
                # Wrap potentially blocking operation in a background thread
                return await asyncio.to_thread(lambda: self.wrapped.column_names)

            async def filter(self, condition: str):
                try:
                    # Handle potential errors with appropriate exception translation
                    result = await asyncio.to_thread(lambda: self.wrapped.filter(condition))
                    return TableWrapper(result)
                except Exception as e:
                    # Translate Java exceptions to Python exceptions
                    raise ValueError(f"Filter failed: {e}") from e
    """

    def __init__(self, wrapped: T, is_enterprise: bool) -> None:
        """Initialize a wrapper for a client object.

        This constructor creates a wrapper around a client object and verifies that the
        required features (enterprise or non-enterprise) are available. It performs
        validation to ensure that the wrapped object is not None and that enterprise
        features are available when required.

        The wrapper pattern established by this constructor is fundamental to the
        Deephaven client architecture, allowing Java client objects to be wrapped with
        enhanced Python interfaces while maintaining type safety through generic typing.

        Args:
            wrapped: The client object to wrap. Must not be None. This is the underlying
                   object (typically a Java object) that this wrapper will delegate to.
                   The type T is determined by the generic type parameter used when
                   subclassing ClientObjectWrapper.
            is_enterprise: Specifies whether the wrapped object requires enterprise features.
                          Must be True for enterprise objects and False for non-enterprise objects.
                          When True, availability of enterprise features will be verified using
                          the module-level is_enterprise_available flag. This helps prevent
                          runtime errors by ensuring required features are available at initialization.

        Raises:
            ValueError: If the wrapped object is None. A non-None wrapped object is essential
                      for the wrapper to function correctly.
            InternalError: If is_enterprise=True but enterprise features are not available.
                          This typically indicates a programming error in the library, as enterprise
                          wrappers should only be created in environments where enterprise features
                          are available.
        """
        if wrapped is None:
            _LOGGER.error("ClientObjectWrapper constructor called with None")
            raise ValueError("Cannot wrap None")

        self._wrapped = wrapped

        if is_enterprise and not is_enterprise_available:
            raise InternalError(
                "ClientObjectWrapper constructor called with enterprise=True when enterprise features are not available. Please report this issue."
            )

    @property
    def wrapped(self) -> T:
        """Access the underlying wrapped client object.

        This property provides direct access to the wrapped client object when
        needed. In most cases, consumers should use the wrapper's methods instead
        of directly accessing the wrapped client.

        The wrapped object is considered an implementation detail, and direct access
        should be avoided in client code when possible. Wrapper methods provide a more
        stable API that can evolve independently of the underlying implementation.

        This property is particularly useful in these scenarios:
        1. When implementing new wrapper methods that need to delegate to the wrapped object
        2. When accessing functionality of the wrapped object that has not yet been
           exposed through the wrapper's interface
        3. When working with advanced use cases that require direct access to the
           underlying implementation

        Usage example in wrapper implementation:
        ```python
        async def get_data(self) -> dict:
            # Use self.wrapped to access the underlying client object
            raw_data = await asyncio.to_thread(self.wrapped.get_data)
            return {k: process_value(v) for k, v in raw_data.items()}
        ```

        Returns:
            The wrapped client object of type T. This is the same object that was
            passed to the constructor, and its type is determined by the generic
            type parameter used when subclassing ClientObjectWrapper.

        Note:
            This property is primarily intended for use within subclasses that need to
            delegate to the wrapped object's methods. External code should prefer to use
            the wrapper's own methods which provide a stable, asynchronous interface.
            Direct usage of the wrapped object bypasses the safety and convenience features
            provided by the wrapper, such as exception translation and asynchronous operation.
        """
        return self._wrapped
