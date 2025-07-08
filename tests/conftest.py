import sys
import types

# TODO: remove this file and handle the imports on their own...


def pytest_sessionstart(session):
    """
    Patch sys.modules for all enterprise proto modules before any test imports.
    This allows the test suite to run even if the real enterprise package is not installed,
    and ensures is_enterprise_available=True logic works in tests.
    """
    try:
        import deephaven_enterprise.client.controller

        # Real package is present, do not patch sys.modules
    except ImportError:
        enterprise_modules = [
            "deephaven_enterprise",
            "deephaven_enterprise.client",
            "deephaven_enterprise.client.controller",
            "deephaven_enterprise.proto",
            "deephaven_enterprise.proto.acl_pb2",
            "deephaven_enterprise.proto.acl_pb2_grpc",
            "deephaven_enterprise.proto.auth_pb2",
            "deephaven_enterprise.proto.auth_pb2_grpc",
            "deephaven_enterprise.proto.auth_service_pb2",
            "deephaven_enterprise.proto.auth_service_pb2_grpc",
            "deephaven_enterprise.proto.common_pb2",
            "deephaven_enterprise.proto.common_pb2_grpc",
            "deephaven_enterprise.proto.controller_common_pb2",
            "deephaven_enterprise.proto.controller_common_pb2_grpc",
            "deephaven_enterprise.proto.controller_pb2",
            "deephaven_enterprise.proto.controller_pb2_grpc",
            "deephaven_enterprise.proto.controller_service_pb2",
            "deephaven_enterprise.proto.controller_service_pb2_grpc",
            "deephaven_enterprise.proto.persistent_query_pb2",
            "deephaven_enterprise.proto.persistent_query_pb2_grpc",
            "deephaven_enterprise.proto.table_definition_pb2",
            "deephaven_enterprise.proto.table_definition_pb2_grpc",
        ]
        for mod in enterprise_modules:
            if mod not in sys.modules:
                sys.modules[mod] = types.SimpleNamespace()
