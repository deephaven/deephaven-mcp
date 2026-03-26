"""Tests for the session_widget_view tool and URL construction helpers."""

import os
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from conftest import MockContext, create_mock_instance_tracker

from deephaven_mcp._exceptions import RegistryItemNotFoundError
from deephaven_mcp.mcp_systems_server._tools.widget import (
    VIEW_URI,
    _build_community_base_url,
    _build_community_widget_url,
    _build_enterprise_widget_url,
    _get_community_psk_token,
    session_widget_view,
    session_widgets_list,
    widget_view_resource,
)
from deephaven_mcp.resource_manager import (
    CommunitySessionManager,
    DynamicCommunitySessionManager,
    EnterpriseSessionManager,
)

# ---------------------------------------------------------------------------
# Helper: _build_community_base_url
# ---------------------------------------------------------------------------


class TestBuildCommunityBaseUrl:
    """Tests for _build_community_base_url."""

    def test_static_with_server_field(self):
        mgr = MagicMock(spec=CommunitySessionManager)
        mgr._config = {"server": "http://myhost:9999"}
        assert _build_community_base_url(mgr) == "http://myhost:9999"

    def test_static_with_host_port(self):
        mgr = MagicMock(spec=CommunitySessionManager)
        mgr._config = {"host": "example.com", "port": 8080, "use_tls": False}
        assert _build_community_base_url(mgr) == "http://example.com:8080"

    def test_static_with_tls(self):
        mgr = MagicMock(spec=CommunitySessionManager)
        mgr._config = {"host": "secure.example.com", "port": 443, "use_tls": True}
        assert _build_community_base_url(mgr) == "https://secure.example.com:443"

    def test_static_defaults(self):
        mgr = MagicMock(spec=CommunitySessionManager)
        mgr._config = {}
        assert _build_community_base_url(mgr) == "http://localhost:10000"

    def test_ui_url_base_override(self):
        mgr = MagicMock(spec=CommunitySessionManager)
        mgr._config = {
            "server": "http://localhost:10000",
            "ui_url_base": "http://localhost:4000",
        }
        assert _build_community_base_url(mgr) == "http://localhost:4000"

    def test_ui_url_base_takes_precedence_over_server(self):
        mgr = MagicMock(spec=CommunitySessionManager)
        mgr._config = {
            "server": "http://localhost:10000",
            "host": "other",
            "port": 9999,
            "ui_url_base": "http://localhost:4000",
        }
        assert _build_community_base_url(mgr) == "http://localhost:4000"

    def test_dynamic_ignores_ui_url_base(self):
        mgr = MagicMock(spec=DynamicCommunitySessionManager)
        mgr.connection_url = "http://dynamic-host:12345"
        assert _build_community_base_url(mgr) == "http://dynamic-host:12345"


# ---------------------------------------------------------------------------
# Helper: _get_community_psk_token
# ---------------------------------------------------------------------------


class TestGetCommunityPskToken:
    """Tests for _get_community_psk_token."""

    def test_none_mode_never_returns_token(self):
        mgr = MagicMock(spec=CommunitySessionManager)
        mgr._config = {"auth_type": "PSK", "auth_token": "secret"}
        assert _get_community_psk_token(mgr, "none") is None

    def test_dynamic_only_blocks_static(self):
        mgr = MagicMock(spec=CommunitySessionManager)
        mgr._config = {"auth_type": "PSK", "auth_token": "secret"}
        assert _get_community_psk_token(mgr, "dynamic_only") is None

    def test_static_only_blocks_dynamic(self):
        mgr = MagicMock(spec=DynamicCommunitySessionManager)
        mgr.launched_session = MagicMock()
        mgr.launched_session.auth_type = "psk"
        mgr.launched_session.auth_token = "secret"
        assert _get_community_psk_token(mgr, "static_only") is None

    def test_all_mode_returns_static_psk(self):
        mgr = MagicMock(spec=CommunitySessionManager)
        mgr._config = {"auth_type": "PSK", "auth_token": "my_psk"}
        assert _get_community_psk_token(mgr, "all") == "my_psk"

    def test_all_mode_returns_dynamic_psk(self):
        mgr = MagicMock(spec=DynamicCommunitySessionManager)
        mgr.launched_session = MagicMock()
        mgr.launched_session.auth_type = "psk"
        mgr.launched_session.auth_token = "dynamic_psk"
        assert _get_community_psk_token(mgr, "all") == "dynamic_psk"

    def test_dynamic_only_allows_dynamic(self):
        mgr = MagicMock(spec=DynamicCommunitySessionManager)
        mgr.launched_session = MagicMock()
        mgr.launched_session.auth_type = "psk"
        mgr.launched_session.auth_token = "dynamic_token"
        assert _get_community_psk_token(mgr, "dynamic_only") == "dynamic_token"

    def test_static_only_allows_static(self):
        mgr = MagicMock(spec=CommunitySessionManager)
        mgr._config = {"auth_type": "Psk", "auth_token": "static_token"}
        assert _get_community_psk_token(mgr, "static_only") == "static_token"

    def test_anonymous_auth_returns_none(self):
        mgr = MagicMock(spec=CommunitySessionManager)
        mgr._config = {"auth_type": "Anonymous", "auth_token": ""}
        assert _get_community_psk_token(mgr, "all") is None

    def test_psk_empty_token_returns_none(self):
        mgr = MagicMock(spec=CommunitySessionManager)
        mgr._config = {"auth_type": "PSK", "auth_token": ""}
        assert _get_community_psk_token(mgr, "all") is None

    def test_static_env_var_fallback(self):
        mgr = MagicMock(spec=CommunitySessionManager)
        mgr._config = {
            "auth_type": "PSK",
            "auth_token": "",
            "auth_token_env_var": "MY_PSK_ENV",
        }
        with patch.dict(os.environ, {"MY_PSK_ENV": "env_psk_value"}):
            assert _get_community_psk_token(mgr, "all") == "env_psk_value"


# ---------------------------------------------------------------------------
# Helper: _build_community_widget_url
# ---------------------------------------------------------------------------


class TestBuildCommunityWidgetUrl:
    """Tests for _build_community_widget_url."""

    def test_anonymous_no_psk(self):
        mgr = MagicMock(spec=CommunitySessionManager)
        mgr._config = {"server": "http://localhost:10000", "auth_type": "Anonymous"}
        url = _build_community_widget_url(mgr, "my_table", "none")
        assert url == "http://localhost:10000/iframe/widget/?name=my_table"

    def test_with_psk(self):
        mgr = MagicMock(spec=CommunitySessionManager)
        mgr._config = {
            "server": "http://localhost:10000",
            "auth_type": "PSK",
            "auth_token": "abc123",
        }
        url = _build_community_widget_url(mgr, "my_table", "all")
        assert url == "http://localhost:10000/iframe/widget/?name=my_table&psk=abc123"

    def test_widget_name_url_encoded(self):
        mgr = MagicMock(spec=CommunitySessionManager)
        mgr._config = {"server": "http://localhost:10000"}
        url = _build_community_widget_url(mgr, "my table&special", "none")
        assert "name=my%20table%26special" in url

    def test_ui_url_base_override_from_config(self):
        mgr = MagicMock(spec=CommunitySessionManager)
        mgr._config = {
            "server": "http://localhost:10000",
            "auth_type": "Anonymous",
            "ui_url_base": "http://localhost:4000",
        }
        url = _build_community_widget_url(mgr, "my_table", "none")
        assert url == "http://localhost:4000/iframe/widget/?name=my_table"

    def test_ui_url_base_override_with_psk(self):
        mgr = MagicMock(spec=CommunitySessionManager)
        mgr._config = {
            "server": "http://localhost:10000",
            "auth_type": "PSK",
            "auth_token": "abc123",
            "ui_url_base": "http://localhost:4000",
        }
        url = _build_community_widget_url(mgr, "my_table", "all")
        assert url.startswith("http://localhost:4000/iframe/widget/")
        assert "&psk=abc123" in url

    def test_no_ui_url_base_uses_server(self):
        mgr = MagicMock(spec=CommunitySessionManager)
        mgr._config = {"server": "http://localhost:10000", "auth_type": "Anonymous"}
        url = _build_community_widget_url(mgr, "my_table", "none")
        assert url == "http://localhost:10000/iframe/widget/?name=my_table"

    def test_custom_widget_path(self):
        mgr = MagicMock(spec=CommunitySessionManager)
        mgr._config = {
            "server": "http://localhost:10000",
            "auth_type": "Anonymous",
            "widget_path": "/custom/path/",
        }
        url = _build_community_widget_url(mgr, "my_table", "none")
        assert url == "http://localhost:10000/custom/path/?name=my_table"

    def test_custom_widget_path_with_ui_url_base(self):
        mgr = MagicMock(spec=CommunitySessionManager)
        mgr._config = {
            "server": "http://localhost:10000",
            "auth_type": "Anonymous",
            "ui_url_base": "http://localhost:4000",
            "widget_path": "/custom/path/",
        }
        url = _build_community_widget_url(mgr, "my_table", "none")
        assert url == "http://localhost:4000/custom/path/?name=my_table"


# ---------------------------------------------------------------------------
# Helper: _build_enterprise_widget_url
# ---------------------------------------------------------------------------


class TestBuildEnterpriseWidgetUrl:
    """Tests for _build_enterprise_widget_url."""

    def test_basic_url(self):
        url = _build_enterprise_widget_url(
            "https://enterprise.example.com/iris/connection.json",
            "MyQuery",
            "revenue_chart",
        )
        assert (
            url
            == "https://enterprise.example.com/iriside/embed/widget/MyQuery/revenue_chart"
        )

    def test_url_with_port(self):
        url = _build_enterprise_widget_url(
            "https://enterprise.example.com:8443/iris/connection.json",
            "Analytics",
            "dashboard",
        )
        assert (
            url
            == "https://enterprise.example.com:8443/iriside/embed/widget/Analytics/dashboard"
        )

    def test_special_characters_encoded(self):
        url = _build_enterprise_widget_url(
            "https://host.example.com/iris/connection.json",
            "My Query",
            "my chart",
        )
        assert "/iriside/embed/widget/My%20Query/my%20chart" in url


# ---------------------------------------------------------------------------
# Resource: widget_view_resource
# ---------------------------------------------------------------------------


class TestWidgetViewResource:
    """Tests for the widget_view_resource MCP resource."""

    def test_returns_html_string(self):
        html = widget_view_resource()
        assert isinstance(html, str)
        assert "<!DOCTYPE html>" in html

    def test_html_contains_iframe_logic(self):
        html = widget_view_resource()
        assert "iframe" in html.lower()

    def test_html_contains_mcp_apps_import(self):
        html = widget_view_resource()
        assert "@modelcontextprotocol/ext-apps" in html

    def test_html_contains_app_connect(self):
        html = widget_view_resource()
        assert "app.connect()" in html


# ---------------------------------------------------------------------------
# Tool: session_widget_view
# ---------------------------------------------------------------------------


def _make_context(config_dict=None, session_registry=None):
    """Create a MockContext with config_manager and session_registry."""
    mock_config_manager = MagicMock()
    mock_config_manager.get_config = AsyncMock(return_value=config_dict or {})
    if session_registry is None:
        session_registry = MagicMock()
        session_registry.get = AsyncMock(
            side_effect=RegistryItemNotFoundError("not found")
        )
    return MockContext(
        {
            "config_manager": mock_config_manager,
            "session_registry": session_registry,
            "instance_tracker": create_mock_instance_tracker(),
        }
    )


class TestSessionWidgetViewCommunity:
    """Tests for session_widget_view with community sessions."""

    @pytest.mark.asyncio
    async def test_community_anonymous(self):
        mgr = MagicMock(spec=CommunitySessionManager)
        mgr._config = {
            "server": "http://localhost:10000",
            "auth_type": "Anonymous",
        }

        mock_registry = MagicMock()
        mock_registry.get = AsyncMock(return_value=mgr)
        context = _make_context(config_dict={}, session_registry=mock_registry)

        result = await session_widget_view(
            context,
            session_id="community:config:local",
            widget_name="my_table",
        )

        assert result["success"] is True
        assert result["widget_url"] == (
            "http://localhost:10000/iframe/widget/?name=my_table"
        )
        assert result["session_id"] == "community:config:local"
        assert result["widget_name"] == "my_table"

    @pytest.mark.asyncio
    async def test_community_with_psk_all_mode(self):
        mgr = MagicMock(spec=CommunitySessionManager)
        mgr._config = {
            "server": "http://localhost:10000",
            "auth_type": "PSK",
            "auth_token": "my_token",
        }

        mock_registry = MagicMock()
        mock_registry.get = AsyncMock(return_value=mgr)

        config_dict = {
            "security": {
                "community": {
                    "credential_retrieval_mode": "all",
                }
            }
        }
        context = _make_context(config_dict=config_dict, session_registry=mock_registry)

        result = await session_widget_view(
            context,
            session_id="community:config:local",
            widget_name="t",
        )

        assert result["success"] is True
        assert "&psk=my_token" in result["widget_url"]

    @pytest.mark.asyncio
    async def test_community_psk_suppressed_by_none_mode(self):
        mgr = MagicMock(spec=CommunitySessionManager)
        mgr._config = {
            "server": "http://localhost:10000",
            "auth_type": "PSK",
            "auth_token": "my_token",
        }

        mock_registry = MagicMock()
        mock_registry.get = AsyncMock(return_value=mgr)

        config_dict = {
            "security": {
                "community": {
                    "credential_retrieval_mode": "none",
                }
            }
        }
        context = _make_context(config_dict=config_dict, session_registry=mock_registry)

        result = await session_widget_view(
            context,
            session_id="community:config:local",
            widget_name="t",
        )

        assert result["success"] is True
        assert "psk=" not in result["widget_url"]

    @pytest.mark.asyncio
    async def test_community_dynamic(self):
        mgr = MagicMock(spec=DynamicCommunitySessionManager)
        mgr.connection_url = "http://dynamic:11111"
        mgr.launched_session = MagicMock()
        mgr.launched_session.auth_type = "anonymous"
        mgr.launched_session.auth_token = ""

        mock_registry = MagicMock()
        mock_registry.get = AsyncMock(return_value=mgr)
        context = _make_context(config_dict={}, session_registry=mock_registry)

        result = await session_widget_view(
            context,
            session_id="community:dynamic:dyn1",
            widget_name="chart",
        )

        assert result["success"] is True
        assert result["widget_url"] == (
            "http://dynamic:11111/iframe/widget/?name=chart"
        )

    @pytest.mark.asyncio
    async def test_community_with_ui_url_base_override(self):
        mgr = MagicMock(spec=CommunitySessionManager)
        mgr._config = {
            "server": "http://localhost:10000",
            "auth_type": "Anonymous",
            "ui_url_base": "http://localhost:4000",
        }

        mock_registry = MagicMock()
        mock_registry.get = AsyncMock(return_value=mgr)
        context = _make_context(config_dict={}, session_registry=mock_registry)

        result = await session_widget_view(
            context,
            session_id="community:config:local",
            widget_name="my_table",
        )

        assert result["success"] is True
        assert result["widget_url"] == (
            "http://localhost:4000/iframe/widget/?name=my_table"
        )


class TestSessionWidgetViewEnterprise:
    """Tests for session_widget_view with enterprise sessions."""

    @pytest.mark.asyncio
    async def test_enterprise_success(self):
        mgr = MagicMock(spec=EnterpriseSessionManager)
        mgr.source = "prod_system"
        mgr.name = "MyPQ"

        mock_registry = MagicMock()
        mock_registry.get = AsyncMock(return_value=mgr)

        config_dict = {
            "enterprise": {
                "systems": {
                    "prod_system": {
                        "connection_json_url": "https://dh.example.com/iris/connection.json",
                    }
                }
            }
        }
        context = _make_context(config_dict=config_dict, session_registry=mock_registry)

        result = await session_widget_view(
            context,
            session_id="enterprise:prod_system:MyPQ",
            widget_name="revenue_chart",
            pq_name="MyPQ",
        )

        assert result["success"] is True
        assert result["widget_url"] == (
            "https://dh.example.com/iriside/embed/widget/MyPQ/revenue_chart"
        )
        assert result["session_id"] == "enterprise:prod_system:MyPQ"
        assert result["widget_name"] == "revenue_chart"

    @pytest.mark.asyncio
    async def test_enterprise_missing_pq_name(self):
        mgr = MagicMock(spec=EnterpriseSessionManager)
        mgr.source = "prod"

        mock_registry = MagicMock()
        mock_registry.get = AsyncMock(return_value=mgr)
        context = _make_context(session_registry=mock_registry)

        result = await session_widget_view(
            context,
            session_id="enterprise:prod:MyPQ",
            widget_name="chart",
        )

        assert result["success"] is False
        assert "pq_name" in result["error"]

    @pytest.mark.asyncio
    async def test_enterprise_missing_connection_json_url(self):
        mgr = MagicMock(spec=EnterpriseSessionManager)
        mgr.source = "prod"

        mock_registry = MagicMock()
        mock_registry.get = AsyncMock(return_value=mgr)

        config_dict = {
            "enterprise": {
                "systems": {
                    "prod": {},
                }
            }
        }
        context = _make_context(config_dict=config_dict, session_registry=mock_registry)

        result = await session_widget_view(
            context,
            session_id="enterprise:prod:MyPQ",
            widget_name="chart",
            pq_name="MyPQ",
        )

        assert result["success"] is False
        assert "connection_json_url" in result["error"]


class TestSessionWidgetViewErrors:
    """Tests for session_widget_view error cases."""

    @pytest.mark.asyncio
    async def test_session_not_found(self):
        mock_registry = MagicMock()
        mock_registry.get = AsyncMock(
            side_effect=RegistryItemNotFoundError("no such session")
        )
        context = _make_context(session_registry=mock_registry)

        result = await session_widget_view(
            context,
            session_id="community:config:nonexistent",
            widget_name="t",
        )

        assert result["success"] is False
        assert "not found" in result["error"].lower()

    @pytest.mark.asyncio
    async def test_unsupported_manager_type(self):
        mgr = MagicMock()
        # Plain MagicMock is neither CommunitySessionManager nor EnterpriseSessionManager
        mock_registry = MagicMock()
        mock_registry.get = AsyncMock(return_value=mgr)
        context = _make_context(session_registry=mock_registry)

        result = await session_widget_view(
            context,
            session_id="unknown:something:else",
            widget_name="t",
        )

        assert result["success"] is False
        assert "unsupported" in result["error"].lower()


# ---------------------------------------------------------------------------
# Tool: session_widgets_list
# ---------------------------------------------------------------------------


class TestSessionWidgetsList:
    """Tests for session_widgets_list."""

    @pytest.mark.asyncio
    async def test_success_multiple_widgets(self):
        """Test session_widgets_list returns multiple widgets with types."""

        class DummySession:
            async def widgets(self):
                return [
                    {"name": "trades", "type": "Table"},
                    {"name": "my_plot", "type": "Figure"},
                    {"name": "dashboard", "type": "PartitionedTable"},
                ]

        mock_session_manager = MagicMock()
        mock_session_manager.get = AsyncMock(return_value=DummySession())

        session_registry = MagicMock()
        session_registry.get = AsyncMock(return_value=mock_session_manager)

        context = MockContext({"session_registry": session_registry})
        result = await session_widgets_list(context, session_id="test-session")

        session_registry.get.assert_awaited_once_with("test-session")
        mock_session_manager.get.assert_awaited_once()

        assert result["success"] is True
        assert result["session_id"] == "test-session"
        assert result["count"] == 3
        assert result["widgets"] == [
            {"name": "trades", "type": "Table"},
            {"name": "my_plot", "type": "Figure"},
            {"name": "dashboard", "type": "PartitionedTable"},
        ]

    @pytest.mark.asyncio
    async def test_success_empty_session(self):
        """Test session_widgets_list with no widgets."""

        class DummySession:
            async def widgets(self):
                return []

        mock_session_manager = MagicMock()
        mock_session_manager.get = AsyncMock(return_value=DummySession())

        session_registry = MagicMock()
        session_registry.get = AsyncMock(return_value=mock_session_manager)

        context = MockContext({"session_registry": session_registry})
        result = await session_widgets_list(context, session_id="empty-session")

        assert result["success"] is True
        assert result["session_id"] == "empty-session"
        assert result["widgets"] == []
        assert result["count"] == 0

    @pytest.mark.asyncio
    async def test_invalid_session_id(self):
        """Test session_widgets_list with invalid session_id."""
        session_registry = MagicMock()
        session_registry.get = AsyncMock(
            side_effect=Exception("Session not found: invalid-session")
        )

        context = MockContext({"session_registry": session_registry})
        result = await session_widgets_list(context, session_id="invalid-session")

        assert result["success"] is False
        assert result["isError"] is True
        assert "Session not found" in result["error"]

    @pytest.mark.asyncio
    async def test_session_connection_failure(self):
        """Test session_widgets_list when session connection fails."""
        mock_session_manager = MagicMock()
        mock_session_manager.get = AsyncMock(side_effect=Exception("Connection failed"))

        session_registry = MagicMock()
        session_registry.get = AsyncMock(return_value=mock_session_manager)

        context = MockContext({"session_registry": session_registry})
        result = await session_widgets_list(context, session_id="test-session")

        assert result["success"] is False
        assert result["isError"] is True
        assert "Connection failed" in result["error"]

    @pytest.mark.asyncio
    async def test_widgets_method_failure(self):
        """Test session_widgets_list when session.widgets() raises."""

        class DummySession:
            async def widgets(self):
                raise Exception("Failed to retrieve widget list")

        mock_session_manager = MagicMock()
        mock_session_manager.get = AsyncMock(return_value=DummySession())

        session_registry = MagicMock()
        session_registry.get = AsyncMock(return_value=mock_session_manager)

        context = MockContext({"session_registry": session_registry})
        result = await session_widgets_list(context, session_id="test-session")

        assert result["success"] is False
        assert result["isError"] is True
        assert "Failed to retrieve widget list" in result["error"]

    @pytest.mark.asyncio
    async def test_tables_only_session(self):
        """Test session_widgets_list with only tables (no other widget types)."""

        class DummySession:
            async def widgets(self):
                return [
                    {"name": "table1", "type": "Table"},
                    {"name": "table2", "type": "Table"},
                ]

        mock_session_manager = MagicMock()
        mock_session_manager.get = AsyncMock(return_value=DummySession())

        session_registry = MagicMock()
        session_registry.get = AsyncMock(return_value=mock_session_manager)

        context = MockContext({"session_registry": session_registry})
        result = await session_widgets_list(context, session_id="community:local:test")

        assert result["success"] is True
        assert result["session_id"] == "community:local:test"
        assert result["count"] == 2
        assert all(w["type"] == "Table" for w in result["widgets"])
