import sys
from types import ModuleType
from unittest.mock import MagicMock

# TODO: remove this file and handle the imports on their own...


def pytest_sessionstart(session):
    """
    Set up comprehensive mock module hierarchy before any test imports.
    This ensures is_enterprise_available=True logic works and unittest.mock patching succeeds.
    """
    try:
        import deephaven_enterprise.client.controller
        # Real package is present, do not patch sys.modules
        return
    except ImportError:
        pass
    
    # Create mock classes that unittest.mock can work with
    class MockSessionManager:
        def __init__(self, *args, **kwargs):
            pass
        def __call__(self, *args, **kwargs):
            return self
    
    class MockControllerClient:
        def __init__(self, *args, **kwargs):
            pass
        def __call__(self, *args, **kwargs):
            return self
    
    def create_mock_module(name, parent_name=None):
        """Create a mock module with all attributes needed for unittest.mock traversal."""
        module = ModuleType(name)
        module.__name__ = name
        module.__file__ = f"<mock {name}>"
        module.__loader__ = None
        module.__package__ = parent_name if parent_name else name.rpartition('.')[0] or None
        module.__spec__ = None
        module.__path__ = []
        return module
    
    # Create the complete module hierarchy with proper classes
    mock_enterprise = create_mock_module("deephaven_enterprise")
    mock_client = create_mock_module("deephaven_enterprise.client", "deephaven_enterprise")
    mock_session_manager = create_mock_module("deephaven_enterprise.client.session_manager", "deephaven_enterprise.client")
    mock_controller = create_mock_module("deephaven_enterprise.client.controller", "deephaven_enterprise.client")
    
    # Set up the classes in their modules with both attribute and __dict__ access
    mock_session_manager.SessionManager = MockSessionManager
    mock_session_manager.__dict__["SessionManager"] = MockSessionManager
    
    mock_controller.ControllerClient = MockControllerClient
    mock_controller.__dict__["ControllerClient"] = MockControllerClient
    
    # Build the module hierarchy with both attribute and __dict__ access
    mock_enterprise.client = mock_client
    mock_enterprise.__dict__["client"] = mock_client
    
    mock_client.session_manager = mock_session_manager
    mock_client.__dict__["session_manager"] = mock_session_manager
    
    mock_client.controller = mock_controller
    mock_client.__dict__["controller"] = mock_controller
    
    # Provide multiple access paths for maximum compatibility
    mock_client.SessionManager = MockSessionManager
    mock_client.__dict__["SessionManager"] = MockSessionManager
    
    mock_client.ControllerClient = MockControllerClient
    mock_client.__dict__["ControllerClient"] = MockControllerClient
    
    mock_enterprise.SessionManager = MockSessionManager
    mock_enterprise.__dict__["SessionManager"] = MockSessionManager
    
    mock_enterprise.ControllerClient = MockControllerClient
    mock_enterprise.__dict__["ControllerClient"] = MockControllerClient
    
    # Install core modules in sys.modules
    sys.modules["deephaven_enterprise"] = mock_enterprise
    sys.modules["deephaven_enterprise.client"] = mock_client
    sys.modules["deephaven_enterprise.client.session_manager"] = mock_session_manager
    sys.modules["deephaven_enterprise.client.controller"] = mock_controller
    
    # Create all the proto modules as simple mock modules
    proto_modules = [
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
    
    for module_name in proto_modules:
        if module_name not in sys.modules:
            parent_name = module_name.rpartition('.')[0] if '.' in module_name else None
            sys.modules[module_name] = create_mock_module(module_name, parent_name)
