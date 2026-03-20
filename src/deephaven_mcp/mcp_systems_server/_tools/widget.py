"""
Widget View MCP Tools - Open Deephaven Widgets as Interactive MCP Apps.

Provides MCP tools and resources for rendering Deephaven widgets (tables, charts,
dashboards, etc.) as interactive UI views inside MCP Apps-capable chat clients.

Tools:
- session_widget_view: Open a Deephaven widget in an interactive inline iframe

Resources:
- ui://deephaven-mcp/widget-view: HTML view resource for rendering Deephaven widget iframes
"""

import logging
import os
from typing import Any
from urllib.parse import quote, urlparse

from mcp.server.fastmcp import Context

from deephaven_mcp.config import ConfigManager, get_all_config_names, get_config_section
from deephaven_mcp.resource_manager import (
    CombinedSessionRegistry,
    CommunitySessionManager,
    DynamicCommunitySessionManager,
    EnterpriseSessionManager,
)

from .mcp_server import mcp_server

_LOGGER = logging.getLogger(__name__)

VIEW_URI = "ui://deephaven-mcp/widget-view"


def _build_community_base_url(mgr: CommunitySessionManager) -> str:
    """Build the base HTTP(S) URL for a community session.

    For dynamic sessions, uses the pre-built ``connection_url``.
    For static (config-based) sessions, constructs the URL from the
    ``server`` field if present, otherwise from ``host``, ``port``, and
    ``use_tls`` config fields.

    Args:
        mgr: A community session manager.

    Returns:
        The base URL, e.g. ``"http://localhost:10000"``.
    """
    if isinstance(mgr, DynamicCommunitySessionManager):
        return mgr.connection_url

    # Static session – prefer pre-built "server" URL, fall back to host/port
    server = mgr._config.get("server")
    if server:
        return str(server)

    host = mgr._config.get("host", "localhost")
    port = mgr._config.get("port", 10000)
    use_tls = mgr._config.get("use_tls", False)
    scheme = "https" if use_tls else "http"
    return f"{scheme}://{host}:{port}"


def _get_community_psk_token(
    mgr: CommunitySessionManager,
    credential_retrieval_mode: str,
) -> str | None:
    """Return the PSK auth token for a community session if allowed.

    Respects the ``security.community.credential_retrieval_mode`` setting:
    - ``"none"``: never return a token
    - ``"dynamic_only"``: only for dynamic sessions
    - ``"static_only"``: only for static (config-based) sessions
    - ``"all"``: always return a token when available

    Returns ``None`` when no token should be included.
    """
    is_dynamic = isinstance(mgr, DynamicCommunitySessionManager)
    is_static = not is_dynamic

    if credential_retrieval_mode == "none":
        return None
    if credential_retrieval_mode == "dynamic_only" and is_static:
        return None
    if credential_retrieval_mode == "static_only" and is_dynamic:
        return None

    # Retrieve auth_type and token
    if isinstance(mgr, DynamicCommunitySessionManager):
        auth_type = mgr.launched_session.auth_type.upper()
        token = mgr.launched_session.auth_token or ""
    else:
        auth_type = mgr._config.get("auth_type", "Anonymous").upper()
        token = mgr._config.get("auth_token", "")
        if not token:
            # Try resolving from environment variable
            env_var = mgr._config.get("auth_token_env_var", "")
            if env_var:
                token = os.getenv(env_var, "")

    if auth_type == "PSK" and token:
        return token
    return None


def _build_community_widget_url(
    mgr: CommunitySessionManager,
    widget_name: str,
    credential_retrieval_mode: str,
) -> str:
    """Build a full Deephaven Community iframe widget URL.

    URL format: ``http[s]://host:port/iframe/widget/?name=<widget_name>[&psk=<token>]``
    """
    base = _build_community_base_url(mgr)
    url = f"{base}/iframe/widget/?name={quote(widget_name, safe='')}"
    token = _get_community_psk_token(mgr, credential_retrieval_mode)
    if token:
        url += f"&psk={quote(token, safe='')}"
    return url


def _build_enterprise_widget_url(
    connection_json_url: str,
    pq_name: str,
    widget_name: str,
) -> str:
    """Build a Deephaven Enterprise iframe widget URL.

    URL format: ``https://host[:port]/iriside/embed/widget/<pq_name>/<widget_name>``

    The base URL is derived from ``connection_json_url`` by stripping the path
    (e.g. ``/iris/connection.json``).
    """
    parsed = urlparse(connection_json_url)
    base = f"{parsed.scheme}://{parsed.netloc}"
    return f"{base}/iriside/embed/widget/{quote(pq_name, safe='')}/{quote(widget_name, safe='')}"


# ---------------------------------------------------------------------------
# MCP App Resource
# ---------------------------------------------------------------------------

# View HTML served as a ui:// resource for MCP Apps-capable hosts.
# The View creates an iframe pointing at the Deephaven widget URL
# received via the tool result.
_WIDGET_VIEW_HTML = """\
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <meta name="color-scheme" content="light dark">
  <title>Deephaven Widget</title>
  <style>
    :root {
      --font-sans: system-ui, -apple-system, sans-serif;
      --color-text-primary: light-dark(#171717, #fafafa);
      --color-text-secondary: light-dark(#666, #999);
      --color-background-primary: light-dark(#ffffff, #171717);
      --color-border-primary: light-dark(#e0e0e0, #333);
    }
    * { box-sizing: border-box; margin: 0; padding: 0; }
    html, body { height: 100%; overflow: hidden; background: transparent; }
    body {
      font-family: var(--font-sans);
      color: var(--color-text-primary);
      display: flex;
      flex-direction: column;
    }
    .container { flex: 1; display: flex; flex-direction: column; min-height: 400px; }
    .container.fullscreen { min-height: 100vh; }
    iframe {
      flex: 1;
      width: 100%;
      border: 1px solid var(--color-border-primary);
      border-radius: 6px;
      background: var(--color-background-primary);
    }
    .container.fullscreen iframe { border-radius: 0; border: none; }
    .status {
      display: flex;
      align-items: center;
      justify-content: center;
      height: 400px;
      color: var(--color-text-secondary);
      font-size: 14px;
    }
    .error { color: #e53e3e; }
    .toolbar {
      position: absolute; top: 8px; right: 8px;
      display: flex; gap: 4px;
      opacity: 0; transition: opacity 0.2s; z-index: 10;
    }
    .container:hover .toolbar { opacity: 0.8; }
    .toolbar:hover { opacity: 1; }
    .controlBtn {
      width: 32px; height: 32px;
      border: none; border-radius: 6px;
      background: rgba(0, 0, 0, 0.5); color: white; cursor: pointer;
      display: flex; align-items: center; justify-content: center;
      transition: background 0.2s; font-size: 14px;
    }
    .controlBtn:hover { background: rgba(0, 0, 0, 0.8); }
    .controlBtn svg { width: 16px; height: 16px; }
    .fullscreenBtn { display: none; }
    .fullscreenBtn.available { display: flex; }
    .fullscreenBtn .collapseIcon { display: none; }
    .container.fullscreen .fullscreenBtn .expandIcon { display: none; }
    .container.fullscreen .fullscreenBtn .collapseIcon { display: block; }
  </style>
</head>
<body>
  <div id="root"></div>
  <script type="module">
    import { App, applyDocumentTheme, applyHostStyleVariables }
      from "https://unpkg.com/@modelcontextprotocol/ext-apps@0.4.1/app-with-deps";

    const root = document.getElementById("root");

    // State
    let displayMode = "inline";
    let fullscreenAvailable = false;

    const app = new App({ name: "Deephaven Widget Viewer", version: "1.0.0" });

    // --- rendering helpers ---
    function renderLoading() {
      root.innerHTML = '<div class="container"><div class="status">Loading widget\u2026</div></div>';
    }

    function renderError(msg) {
      root.innerHTML = '<div class="container"><div class="status error">' + escapeHtml(msg) + '</div></div>';
    }

    function escapeHtml(s) {
      const d = document.createElement("div");
      d.textContent = s;
      return d.innerHTML;
    }

    function renderWidget(url) {
      const isFS = displayMode === "fullscreen";
      root.innerHTML = "";

      const container = document.createElement("div");
      container.className = "container" + (isFS ? " fullscreen" : "");
      container.style.position = "relative";

      // Toolbar
      const toolbar = document.createElement("div");
      toolbar.className = "toolbar";

      // Fullscreen button
      const fsBtn = document.createElement("button");
      fsBtn.className = "controlBtn fullscreenBtn" + (fullscreenAvailable ? " available" : "");
      fsBtn.title = "Toggle fullscreen";
      fsBtn.innerHTML =
        '<svg class="expandIcon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">' +
          '<path d="M8 3H5a2 2 0 0 0-2 2v3m18 0V5a2 2 0 0 0-2-2h-3m0 18h3a2 2 0 0 0 2-2v-3M3 16v3a2 2 0 0 0 2 2h3"/>' +
        '</svg>' +
        '<svg class="collapseIcon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">' +
          '<path d="M8 3v3a2 2 0 0 1-2 2H3m18 0h-3a2 2 0 0 1-2-2V3m0 18v-3a2 2 0 0 1 2-2h3M3 16h3a2 2 0 0 1 2 2v3"/>' +
        '</svg>';
      fsBtn.addEventListener("click", toggleFullscreen);
      toolbar.appendChild(fsBtn);
      container.appendChild(toolbar);

      // Iframe
      const iframe = document.createElement("iframe");
      iframe.src = url;
      iframe.setAttribute("sandbox", "allow-scripts allow-same-origin allow-forms allow-popups");
      iframe.setAttribute("allow", "clipboard-write");
      container.appendChild(iframe);

      root.appendChild(container);
    }

    // Current widget URL for re-rendering
    let currentWidgetUrl = null;

    async function toggleFullscreen() {
      const newMode = displayMode === "fullscreen" ? "inline" : "fullscreen";
      try {
        const result = await app.requestDisplayMode({ mode: newMode });
        displayMode = result.mode;
      } catch (_) {}
      if (currentWidgetUrl) renderWidget(currentWidgetUrl);
    }

    // --- host context ---
    function applyHostContext(ctx) {
      if (ctx.theme) applyDocumentTheme(ctx.theme);
      if (ctx.styles && ctx.styles.variables) applyHostStyleVariables(ctx.styles.variables);
      if (ctx.availableDisplayModes && ctx.availableDisplayModes.includes("fullscreen")) {
        fullscreenAvailable = true;
        const btn = document.querySelector(".fullscreenBtn");
        if (btn) btn.classList.add("available");
      }
      if (ctx.displayMode) {
        displayMode = ctx.displayMode;
        if (currentWidgetUrl) renderWidget(currentWidgetUrl);
      }
    }

    app.onhostcontextchanged = applyHostContext;

    // --- tool result handling ---
    app.ontoolresult = (result) => {
      try {
        const textBlock = result.content && result.content.find(c => c.type === "text");
        if (!textBlock) { renderError("No result data received"); return; }
        const data = JSON.parse(textBlock.text);
        if (!data.success) { renderError(data.error || "Tool returned an error"); return; }
        if (!data.widget_url) { renderError("No widget URL in result"); return; }
        currentWidgetUrl = data.widget_url;
        renderWidget(currentWidgetUrl);
      } catch (e) {
        renderError("Failed to process tool result: " + e.message);
      }
    };

    app.onteardown = async () => {
      currentWidgetUrl = null;
      root.innerHTML = "";
      return {};
    };

    // --- initialise ---
    renderLoading();
    await app.connect();
    const ctx = app.getHostContext();
    if (ctx) applyHostContext(ctx);
  </script>
</body>
</html>"""


def _compute_frame_domains_from_config(config: dict[str, Any]) -> list[str]:
    """Compute CSP ``frameDomains`` from the Deephaven MCP configuration.

    Extracts the scheme + host + port origin for every configured community
    session and enterprise system so that the widget-view resource advertises
    only the domains that the server can actually reach.

    If dynamic session creation (``community.session_creation``) is enabled,
    ``http://localhost:*`` and ``http://127.0.0.1:*`` are included as
    catch-alls since dynamic sessions use ephemeral ports on localhost.

    Returns:
        A deduplicated, sorted list of origin strings
        (e.g. ``["http://localhost:10000", "https://dh.example.com"]``).
    """
    origins: set[str] = set()

    # --- Community static sessions ---
    session_names = get_all_config_names(config, ["community", "sessions"])
    for name in session_names:
        try:
            sess = get_config_section(config, ["community", "sessions", name])
        except KeyError:
            continue

        server_url = sess.get("server")
        if server_url:
            parsed = urlparse(str(server_url))
            origins.add(f"{parsed.scheme}://{parsed.netloc}")
        else:
            host = sess.get("host", "localhost")
            port = sess.get("port", 10000)
            use_tls = sess.get("use_tls", False)
            scheme = "https" if use_tls else "http"
            origins.add(f"{scheme}://{host}:{port}")

    # --- Community dynamic session creation ---
    try:
        get_config_section(config, ["community", "session_creation"])
        # Dynamic sessions run on localhost with ephemeral ports
        origins.add("http://localhost:*")
        origins.add("http://127.0.0.1:*")
    except KeyError:
        pass

    # --- Enterprise systems ---
    system_names = get_all_config_names(config, ["enterprise", "systems"])
    for name in system_names:
        try:
            sys_cfg = get_config_section(config, ["enterprise", "systems", name])
        except KeyError:
            continue

        conn_url = sys_cfg.get("connection_json_url", "")
        if conn_url:
            parsed = urlparse(conn_url)
            origins.add(f"{parsed.scheme}://{parsed.netloc}")

    return sorted(origins)


async def update_widget_view_frame_domains(config_manager: ConfigManager) -> None:
    """Update the widget-view resource CSP ``frameDomains`` from the live config.

    Called during server lifespan startup (and on config reload) to replace
    the initial empty ``frameDomains`` list with origins derived from the
    configured community sessions and enterprise systems.

    Args:
        config_manager: The active :class:`ConfigManager` instance.
    """
    config = await config_manager.get_config()
    domains = _compute_frame_domains_from_config(config)

    resource = mcp_server._resource_manager._resources.get(VIEW_URI)
    if resource is None:
        _LOGGER.warning(
            "[widget] Cannot update frameDomains: resource %r not registered.",
            VIEW_URI,
        )
        return

    resource.meta["ui"]["csp"]["frameDomains"] = domains
    _LOGGER.info(
        "[widget] Updated frameDomains to %s",
        domains,
    )


@mcp_server.resource(
    VIEW_URI,
    mime_type="text/html;profile=mcp-app",
    meta={
        "ui": {
            "csp": {
                "resourceDomains": ["https://unpkg.com"],
                "frameDomains": [],
            },
            "prefersBorder": False,
        }
    },
)
def widget_view_resource() -> str:
    """Deephaven widget viewer — renders a Deephaven table, chart, or widget in an iframe."""
    return _WIDGET_VIEW_HTML


# ---------------------------------------------------------------------------
# MCP Tool
# ---------------------------------------------------------------------------


@mcp_server.tool(
    meta={
        "ui": {"resourceUri": VIEW_URI},
        "ui/resourceUri": VIEW_URI,  # legacy format for older hosts
    },
)
async def session_widget_view(
    context: Context,
    session_id: str,
    widget_name: str,
    pq_name: str | None = None,
) -> dict[str, Any]:
    """Open a Deephaven widget (table, chart, dashboard, etc.) as an interactive view.

    Generates a URL to embed the specified widget from a Deephaven session in an
    inline iframe.  MCP Apps-capable hosts (Claude, ChatGPT, VS Code, etc.) will
    render the widget directly in the conversation.

    **Terminology Note:**
    - ``session_id``: A fully qualified session identifier in format
      ``"community:config:<name>"``, ``"community:dynamic:<name>"``, or
      ``"enterprise:<system>:<session>"``.
    - ``widget_name``: The variable name assigned to a table, chart, or other
      exportable widget in the Deephaven session.
    - ``pq_name``: (Enterprise only) The name of the Persistent Query or
      Code Studio containing the widget.

    **AI Agent Usage:**
    Use this tool after creating a table or chart via ``session_script_run`` to
    display it visually.  For example, after running a script that creates a
    variable ``my_table``, call this tool with ``widget_name="my_table"`` to
    render it inline.

    For Community sessions, ``pq_name`` is ignored.  For Enterprise sessions,
    ``pq_name`` is required and should be the Persistent Query or Code Studio
    name that contains the widget.

    Args:
        session_id: Fully qualified session identifier
            (e.g. ``"community:config:local_test"``).
        widget_name: Name of the widget variable to display
            (e.g. ``"my_table"``).
        pq_name: (Enterprise only) Name of the Persistent Query or Code Studio.
            Required for Enterprise sessions; ignored for Community sessions.

    Returns:
        A dict with these fields:

        Success Response::

            {
                "success": True,
                "widget_url": "http://localhost:10000/iframe/widget/?name=my_table",
                "session_id": "community:config:local_test",
                "widget_name": "my_table"
            }

        Error Response::

            {
                "success": False,
                "error": "Session 'community:config:xyz' not found: ...",
                "isError": True
            }

    Example Success Response (Community)::

        {
            "success": true,
            "widget_url": "http://localhost:10000/iframe/widget/?name=sin_table&psk=my_token",
            "session_id": "community:config:local_test",
            "widget_name": "sin_table"
        }

    Example Success Response (Enterprise)::

        {
            "success": true,
            "widget_url": "https://dh-enterprise.example.com/iriside/embed/widget/MyQuery/revenue_chart",
            "session_id": "enterprise:prod:MyQuery",
            "widget_name": "revenue_chart"
        }

    Error Scenarios:
        - Session not found in registry
        - Enterprise session missing required ``pq_name`` parameter
        - Enterprise system configuration missing ``connection_json_url``
    """
    _LOGGER.info(
        f"[mcp_systems_server:session_widget_view] Invoked for session_id={session_id!r}, "
        f"widget_name={widget_name!r}, pq_name={pq_name!r}"
    )

    try:
        # Retrieve session registry and config from context
        session_registry: CombinedSessionRegistry = (
            context.request_context.lifespan_context["session_registry"]
        )
        config_manager: ConfigManager = context.request_context.lifespan_context[
            "config_manager"
        ]

        # Look up the session manager
        try:
            mgr = await session_registry.get(session_id)
        except Exception as e:
            return {
                "success": False,
                "error": f"Session '{session_id}' not found: {e}",
                "isError": True,
            }

        # --- Community sessions ---
        if isinstance(mgr, CommunitySessionManager):
            # Get credential retrieval mode for PSK gating
            config = await config_manager.get_config()
            security_config = config.get("security", {})
            security_community = security_config.get("community", {})
            credential_retrieval_mode = security_community.get(
                "credential_retrieval_mode", "none"
            )

            widget_url = _build_community_widget_url(
                mgr, widget_name, credential_retrieval_mode
            )

            _LOGGER.info(
                f"[mcp_systems_server:session_widget_view] Built community widget URL for "
                f"session_id={session_id!r}, widget_name={widget_name!r}"
            )

            return {
                "success": True,
                "widget_url": widget_url,
                "session_id": session_id,
                "widget_name": widget_name,
            }

        # --- Enterprise sessions ---
        if isinstance(mgr, EnterpriseSessionManager):
            if not pq_name:
                return {
                    "success": False,
                    "error": (
                        "Enterprise sessions require the 'pq_name' parameter — "
                        "the name of the Persistent Query or Code Studio that "
                        "contains the widget."
                    ),
                    "isError": True,
                }

            # Derive the system name from the manager's source
            system_name = mgr.source

            # Get the enterprise system config to find connection_json_url
            try:
                enterprise_systems = get_config_section(
                    await config_manager.get_config(),
                    ["enterprise", "systems"],
                )
            except KeyError:
                enterprise_systems = {}

            system_config = enterprise_systems.get(system_name, {})
            connection_json_url = system_config.get("connection_json_url", "")

            if not connection_json_url:
                return {
                    "success": False,
                    "error": (
                        f"Enterprise system '{system_name}' does not have a "
                        f"'connection_json_url' configured. Cannot build widget URL."
                    ),
                    "isError": True,
                }

            widget_url = _build_enterprise_widget_url(
                connection_json_url, pq_name, widget_name
            )

            _LOGGER.info(
                f"[mcp_systems_server:session_widget_view] Built enterprise widget URL for "
                f"session_id={session_id!r}, pq_name={pq_name!r}, widget_name={widget_name!r}"
            )

            return {
                "success": True,
                "widget_url": widget_url,
                "session_id": session_id,
                "widget_name": widget_name,
            }

        # --- Unknown session type ---
        return {
            "success": False,
            "error": (
                f"Session '{session_id}' has an unsupported manager type "
                f"({type(mgr).__name__}). Widget view is supported for "
                f"Community and Enterprise sessions."
            ),
            "isError": True,
        }

    except Exception as e:
        _LOGGER.error(
            f"[mcp_systems_server:session_widget_view] Failed: {e!r}",
            exc_info=True,
        )
        return {"success": False, "error": str(e), "isError": True}
