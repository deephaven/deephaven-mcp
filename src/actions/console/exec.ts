import type { dh } from "@deephaven/jsapi-types";
import { getQueryInfoForNameOrSerial } from "@deephaven-enterprise/jsapi-nodejs";
import {
  type CommandLifecycle,
  runLifecycleAction,
} from "../../cli/lifecycle.ts";
import {
  asDhError,
  cleanupError,
  DhError,
  timeoutMs,
  withTimeout,
} from "../../errors.ts";
import { note } from "../../output.ts";
import { requireAuth } from "../auth/session.ts";
import type { Session } from "../auth/signin.ts";
import type { Store } from "../auth/store.ts";
import {
  communitySharedConsole,
  createConsole,
  identityOf,
  sessionForTarget,
} from "./actions.ts";
import { connectCorePlusWorker } from "./core.ts";
import {
  type ConsoleInfo,
  consoleFailureCode,
  findEnterpriseConsole,
  isRunning,
  listEnterpriseConsoles,
} from "./enterprise.ts";
import {
  COMMUNITY_SHARED_ID,
  EMPTY_CONSOLE_REGISTRY,
  identityKey,
  rememberConsole,
} from "./store.ts";

export interface ExecChange {
  /** Variable name in the script session. */
  name: string;
  /** API variable type string, such as `Table`. */
  type: string;
}

/** Variable changes reported by one `runCode` call. */
export interface ExecChanges {
  created: ExecChange[];
  updated: ExecChange[];
  removed: ExecChange[];
}

/** Console used for execution and whether this call created it. */
export interface ExecConsole {
  shortcode: string;
  id: string;
  created: boolean;
}

/** Stable result shape shared by human and JSON output. */
export interface ExecResult {
  console: ExecConsole;
  changes: ExecChanges;
}

/** Source text already validated at the command boundary. */
export interface ExecSource {
  /** Sent to Deephaven verbatim. */
  code: string;
}

interface ExecTarget {
  /** Authenticated server and selected console. */
  session: Session;
  /** Resolved console, including the synthetic Community shared console. */
  console: ConsoleInfo;
  /** Whether this invocation created a new Enterprise console. */
  created: boolean;
}

/** Resolve an explicit console, the profile selection, or a new Enterprise console. */
export async function resolveExecTarget(
  store: Store,
  target: string | undefined,
  profileFlag: string | undefined,
  lifecycle: CommandLifecycle,
): Promise<ExecTarget> {
  if (target)
    return await resolveExplicitExecTarget(
      store,
      target,
      profileFlag,
      lifecycle,
    );
  return await resolveDefaultExecTarget(store, profileFlag, lifecycle);
}

/** Resolve and validate an explicitly named Community or Enterprise console. */
async function resolveExplicitExecTarget(
  store: Store,
  target: string,
  profileFlag: string | undefined,
  lifecycle: CommandLifecycle,
): Promise<ExecTarget> {
  const session = await sessionForTarget(store, target, profileFlag, lifecycle);
  if (session.community) {
    if (target !== COMMUNITY_SHARED_ID && target !== session.origin) {
      throw new DhError(
        "console_not_found",
        `Console "${target}" was not found`,
        `Community servers provide one shared console: ${COMMUNITY_SHARED_ID}.`,
      );
    }
    return {
      session,
      console: communitySharedConsole(session.origin),
      created: false,
    };
  }
  const console = findEnterpriseConsole(session, target);
  if (!console) {
    throw new DhError(
      "console_not_found",
      `Console "${target}" was not found`,
      `Run \`dh console\` to list consoles.`,
    );
  }
  if (!isRunning(console.state)) {
    throw new DhError(
      "console_not_ready",
      `Console ${target} is ${console.state}`,
    );
  }
  await rememberConsole(
    store,
    console.shortcode,
    console.id,
    identityOf(session),
  );
  return { session, console, created: false };
}

/** Use the current selection, one running console, or a newly created console. */
async function resolveDefaultExecTarget(
  store: Store,
  profileFlag: string | undefined,
  lifecycle: CommandLifecycle,
): Promise<ExecTarget> {
  const session = await (profileFlag
    ? sessionForTarget(store, "", profileFlag, lifecycle)
    : requireAuth(undefined, store, lifecycle));
  if (session.community) {
    return {
      session,
      console: communitySharedConsole(session.origin),
      created: false,
    };
  }
  return await resolveEnterpriseDefaultExecTarget(
    store,
    session,
    profileFlag,
    lifecycle,
  );
}

/** Reuse a running Enterprise selection or create when no unambiguous console exists. */
async function resolveEnterpriseDefaultExecTarget(
  store: Store,
  session: Session,
  profileFlag: string | undefined,
  lifecycle: CommandLifecycle,
): Promise<ExecTarget> {
  const identity = identityOf(session);
  const registry = await store.readConsoleData(EMPTY_CONSOLE_REGISTRY);
  const selected = registry.selected[identityKey(identity)];
  if (selected) {
    const console = findEnterpriseConsole(session, selected);
    if (console && isRunning(console.state)) {
      await rememberConsole(store, console.shortcode, console.id, identity);
      return { session, console, created: false };
    }
  }
  const running = listEnterpriseConsoles(session).filter((console) =>
    isRunning(console.state),
  );
  if (running.length === 1) {
    const console = running.at(0);
    if (console) {
      await rememberConsole(store, console.shortcode, console.id, identity);
      return { session, console, created: false };
    }
  }
  if (running.length > 1) {
    throw new DhError(
      "usage",
      "Multiple consoles are running",
      "Use `--console <shortcode-or-id>` to choose one.",
    );
  }
  const console = await createConsole(store, profileFlag, {}, false, lifecycle);
  return { session, console, created: true };
}

/** Execute one source and normalize variable changes for human and JSON output. */
export async function execInConsole(
  store: Store,
  source: ExecSource,
  target: string | undefined,
  profileFlag: string | undefined,
  lifecycle: CommandLifecycle,
): Promise<ExecResult> {
  const result = await runLifecycleAction(
    lifecycle,
    { command: "console exec", operation: "console execution" },
    execInConsoleAction,
    store,
    source,
    target,
    profileFlag,
    lifecycle,
  );
  lifecycle.setFailureReport({
    command: "console exec",
    target: result.console.shortcode,
    profile: profileFlag,
    operation: "script execution",
    phase: "completed",
    outcome: "succeeded",
    console: result.console,
    changes: result.changes,
    human: {
      summary: `Interrupted after the script succeeded on console ${result.console.shortcode}.`,
      context: `The script succeeded on console ${result.console.shortcode}.`,
    },
  });
  return result;
}

/** Resolve the target, open a session, run the source, and retain partial changes on failure. */
async function execInConsoleAction(
  store: Store,
  source: ExecSource,
  target: string | undefined,
  profileFlag: string | undefined,
  lifecycle: CommandLifecycle,
): Promise<ExecResult> {
  const resolved = await resolveExecTarget(
    store,
    target,
    profileFlag,
    lifecycle,
  );
  lifecycle.setFailureReport({
    command: "console exec",
    target: resolved.console.shortcode,
    profile: profileFlag,
    operation: "script execution",
    phase: "opening",
    outcome: "not_started",
    console: {
      shortcode: resolved.console.shortcode,
      id: resolved.console.id,
      created: resolved.created,
    },
    human: {
      summary: `Interrupted while opening console ${resolved.console.shortcode}.`,
      context: `Console ${resolved.console.shortcode}.`,
    },
  });

  const ideSession = await openTimedIdeSession(
    resolved.session,
    resolved.console,
    lifecycle,
  );
  lifecycle.setFailureReport({
    phase: "running",
    outcome: "unknown",
    human: {
      summary: `Interrupted while running a script on console ${resolved.console.shortcode}.`,
      context: `Console ${resolved.console.shortcode}.`,
      details: [
        "The console and script outcome are unknown; dh exiting does not stop the remote script.",
      ],
    },
  });
  let result: dh.ide.CommandResult;
  try {
    // The JS API resolves successfully even for script failures; `error`
    // carries the server traceback and must be handled after reading changes.
    result = await ideSession.runCode(source.code);
  } catch (e) {
    lifecycle.signal.throwIfAborted();
    if (e instanceof DhError) throw e;
    const code = consoleFailureCode(e);
    const messages = {
      console_forbidden: "The server denied permission to run the script",
      console_unsupported: "The server cannot run scripts in this console",
      server_unreachable: "Lost connection while running the script",
      console_failed: "Could not run the script",
    };
    throw new DhError(code, messages[code]);
  }
  const changes = changesOf(result.changes);
  if (result.error) {
    lifecycle.setFailureReport({
      outcome: "failed",
      human: {
        summary: "Script execution failed.",
        context: `Console ${resolved.console.shortcode}.`,
      },
      changes,
      console: {
        shortcode: resolved.console.shortcode,
        id: resolved.console.id,
        created: resolved.created,
      },
    });
    throw new DhError(
      "exec_failed",
      "Script execution failed",
      undefined,
      result.error,
    );
  }
  lifecycle.setFailureReport({
    outcome: "succeeded",
  });
  return {
    console: {
      shortcode: resolved.console.shortcode,
      id: resolved.console.id,
      created: resolved.created,
    },
    changes,
  };
}

/** Bound session opening without imposing a deadline on script execution. */
async function openTimedIdeSession(
  session: Session,
  console: ConsoleInfo,
  lifecycle: CommandLifecycle,
): Promise<dh.IdeSession> {
  const deadline = new AbortController();
  const signal = AbortSignal.any([lifecycle.signal, deadline.signal]);
  return await withTimeout(
    acquireIdeSession(session, console, lifecycle, signal, deadline.signal),
    timeoutMs(),
    () => {
      const error = new DhError(
        "server_unreachable",
        "Timed out while opening the console session",
      );
      deadline.abort(error);
      return error;
    },
  );
}

/** Register an opened session, or close it if opening outlived its caller. */
async function acquireIdeSession(
  session: Session,
  console: ConsoleInfo,
  lifecycle: CommandLifecycle,
  signal: AbortSignal,
  deadline: AbortSignal,
): Promise<dh.IdeSession> {
  const ideSession = await openIdeSession(session, console, lifecycle, signal);
  try {
    signal.throwIfAborted();
    lifecycle.onCleanup(() => ideSession.close());
  } catch (error) {
    try {
      ideSession.close();
    } catch (cleanup) {
      if (deadline.aborted) {
        note(
          "! Could not close a console session opened after timeout.",
          "warning",
        );
      }
      throw cleanupError([cleanup], asDhError(error));
    }
    throw error;
  }
  return ideSession;
}

/** Open the JS API session used to run code on the resolved console. */
async function openIdeSession(
  session: Session,
  console: ConsoleInfo,
  lifecycle: CommandLifecycle,
  signal: AbortSignal,
): Promise<dh.IdeSession> {
  try {
    if (session.community)
      return await openCommunityIdeSession(session, signal);
    return await openEnterpriseIdeSession(session, console, lifecycle, signal);
  } catch (e) {
    signal.throwIfAborted();
    if (e instanceof DhError) throw e;
    const code = consoleFailureCode(e);
    const messages = {
      console_forbidden: "The server denied permission to open the console",
      console_unsupported: "The server cannot run scripts in this console",
      server_unreachable: "Could not reach the console server",
      console_failed: "Could not open the console session",
    };
    throw new DhError(code, messages[code]);
  }
}

/** Open and start the Community shared-console IDE session. */
async function openCommunityIdeSession(
  session: Session,
  signal: AbortSignal,
): Promise<dh.IdeSession> {
  if (!session.community) {
    throw new DhError(
      "console_unsupported",
      "The server cannot run scripts in this console",
    );
  }
  const ide = await session.community.client.getAsIdeConnection();
  signal.throwIfAborted();
  await ide.running();
  signal.throwIfAborted();
  const languages = await ide.getConsoleTypes();
  signal.throwIfAborted();
  const language =
    languages.find((type) => type.toLowerCase() === "python") ??
    languages.find((type) => type.toLowerCase() === "groovy");
  if (!language) {
    throw new DhError(
      "console_unsupported",
      "The Community server has no supported console language",
    );
  }
  return await ide.startSession(language);
}

/** Resolve an Enterprise query and open its worker IDE session. */
async function openEnterpriseIdeSession(
  session: Session,
  console: ConsoleInfo,
  lifecycle: CommandLifecycle,
  signal: AbortSignal,
): Promise<dh.IdeSession> {
  if (!session.enterprise) {
    throw new DhError(
      "console_unsupported",
      "The server cannot run scripts in this console",
    );
  }
  const language = sessionLanguage(console.language);
  const query = await getQueryInfoForNameOrSerial({
    client: session.enterprise.client,
    dh: session.enterprise.dhe,
    queryNameOrSerial: console.id,
  });
  signal.throwIfAborted();
  const ide = await connectCorePlusWorker(session, query, lifecycle, signal);
  signal.throwIfAborted();
  return await ide.startSession(language);
}

/** Convert JS API change collections into the CLI's JSON shape. */
function changesOf(changes: dh.ide.VariableChanges): ExecChanges {
  const map = (items: dh.ide.VariableDefinition[]) =>
    items.map((item) => ({ name: item.name, type: String(item.type) }));
  return {
    created: map(changes.created),
    updated: map(changes.updated),
    removed: map(changes.removed),
  };
}

/** Normalize server labels to the JS API's Python/Groovy session names. */
function sessionLanguage(language: string | null): "python" | "groovy" {
  switch (language?.toLowerCase()) {
    case "python":
      return "python";
    case "groovy":
      return "groovy";
    default:
      throw new DhError(
        "console_unsupported",
        "The console has no supported script language",
      );
  }
}
