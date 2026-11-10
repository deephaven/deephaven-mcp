import type { AuthenticatedEnterpriseClient } from "@deephaven-enterprise/jsapi-nodejs";
import type { CommandLifecycle } from "../../cli/lifecycle.ts";
import { DhError, reasonOf, timeoutMs, withTimeout } from "../../errors.ts";
import type { Session } from "../auth/signin.ts";

export interface ConsoleInfo {
  id: string;
  shortcode: string;
  name: string;
  state: string;
  language: string | null;
  heapSizeGb: number | null;
  server: string | null;
  engine: string | null;
  jvmArgs: string | null;
  environment: Record<string, string>;
  autoDeleteTimeoutSeconds: number | null;
}

export interface EffectiveConsoleSettings {
  language: "python" | "groovy";
  heapSizeGb: number;
  server: string;
  engine: string;
  jvmArgs: string[];
  environment: Record<string, string>;
  autoDeleteTimeoutSeconds: number;
}

export const CLI_CONSOLE_PREFIX = "IC - dh CLI - ";
const QUERY_TYPE = "InteractiveConsole";
const TEMPORARY_QUEUE = "InteractiveConsoleTemporaryQueue";
const DEFAULT_AUTO_DELETE_SECONDS = 600;
const POLL_INTERVAL_MS = 1_000;

interface ApiQuery {
  name?: string;
  serial?: string | null;
  type?: string;
  status?: string;
  designated?: { status?: string | null; shortCauses?: string | null } | null;
  replicas?: { status?: string | null; shortCauses?: string | null }[];
  scriptLanguage?: string | null;
  heapSize?: number;
  dbServerName?: string;
  workerKind?: string;
  jvmArgs?: string | null;
  jvmArguments?: string[];
  envVars?: string | null;
  envVarEntries?: string[];
  timeout?: number;
}

interface ServerConfig {
  defaultWorkerKind?: string;
  workerKinds?: { name: string; protocols?: string[] }[];
  scriptSessionProviders?: string[];
}

interface ConsoleServer {
  name: string;
}

interface QueryConstants {
  consoleDefaultHeap?: number;
}

interface EnvironmentVariables {
  entries: string[];
  text: string;
}

/**
 * Return the Enterprise API client for a session, rejecting Community sessions.
 */
function enterpriseClient(session: Session): AuthenticatedEnterpriseClient {
  if (!session.enterprise) {
    throw new DhError(
      "console_unsupported",
      "Console management is only available on Enterprise servers",
    );
  }
  return session.enterprise.client;
}

/** Read the server's known queries and keep only consoles created by this CLI. */
function queries(session: Session): ApiQuery[] {
  const client = enterpriseClient(session);
  const values: ApiQuery[] | undefined = client.getKnownConfigs?.();
  if (!Array.isArray(values)) {
    throw new DhError("console_unsupported", "The server cannot list consoles");
  }
  return values.filter(isCliConsole);
}

/** Check the type, name, and stable ID used to identify a CLI-created console. */
function isCliConsole(query: ApiQuery): boolean {
  return (
    query.type === QUERY_TYPE &&
    typeof query.name === "string" &&
    /^IC - dh CLI - [a-z0-9]{6}$/.test(query.name) &&
    typeof query.serial === "string" &&
    query.serial.length > 0
  );
}

/** Extract the user-facing shortcode from a CLI console name. */
export function shortcodeFromName(name: string): string {
  return name.slice(CLI_CONSOLE_PREFIX.length);
}

/** Whether the server reports a console state that can be used. */
export function isRunning(state: string): boolean {
  return ["running", "connected"].includes(state.toLowerCase());
}

/** Whether the server reports a final state that prevents startup. */
export function isTerminal(state: string): boolean {
  return [
    "failed",
    "error",
    "stopped",
    "terminated",
    "deleted",
    "cancelled",
  ].includes(state.toLowerCase());
}

/** Resolve the most useful status exposed by the query API. */
export function consoleState(query: ApiQuery): string {
  return (
    query.designated?.status ??
    query.replicas?.[0]?.status ??
    query.status ??
    "Unknown"
  );
}

/** Convert an Enterprise query into the stable console shape used by actions. */
export function consoleInfo(query: ApiQuery): ConsoleInfo {
  const name = query.name ?? "";
  return {
    id: query.serial ?? "",
    shortcode: shortcodeFromName(name),
    name,
    state: consoleState(query),
    language: query.scriptLanguage ?? null,
    heapSizeGb: query.heapSize ?? null,
    server: query.dbServerName ?? null,
    engine: query.workerKind ?? null,
    jvmArgs: query.jvmArgs ?? query.jvmArguments?.join(" ") ?? null,
    environment: environmentOf(query),
    autoDeleteTimeoutSeconds:
      typeof query.timeout === "number" ? query.timeout / 1000 : null,
  };
}

/** Read environment variables from either supported Enterprise API shape. */
function environmentOf(query: ApiQuery): Record<string, string> {
  const entries = query.envVarEntries ?? [];
  const environment: Record<string, string> = {};
  for (let i = 0; i + 1 < entries.length; i += 2) {
    const key = entries[i];
    const value = entries[i + 1];
    if (key !== undefined && value !== undefined) environment[key] = value;
  }
  if (Object.keys(environment).length > 0 || !query.envVars) return environment;
  for (const line of query.envVars.split(/\r?\n/)) {
    const at = line.indexOf("=");
    if (at > 0) environment[line.slice(0, at)] = line.slice(at + 1);
  }
  return environment;
}

/** List the CLI-owned InteractiveConsole queries on a server. */
export function listEnterpriseConsoles(session: Session): ConsoleInfo[] {
  return queries(session).map(consoleInfo);
}

/** Find a CLI console by its server ID or its user-facing shortcode. */
export function findEnterpriseConsole(
  session: Session,
  target: string,
): ConsoleInfo | undefined {
  const found = queries(session).find(
    (query) =>
      query.serial === target ||
      query.name === `${CLI_CONSOLE_PREFIX}${target}`,
  );
  return found ? consoleInfo(found) : undefined;
}

/** Read server capabilities and derive defaults for a new console. */
export async function serverDefaults(session: Session): Promise<{
  settings: EffectiveConsoleSettings;
  servers: string[];
  engines: string[];
  languages: string[];
}> {
  const client = enterpriseClient(session);
  const config = await call<ServerConfig>(
    client.getServerConfigValues?.(),
    "read console server settings",
  );
  const serverList = await call<ConsoleServer[]>(
    client.getDbServers?.(),
    "list console servers",
  );
  const constants = await call<QueryConstants>(
    client.getQueryConstants?.(),
    "read console defaults",
  );
  if (!Array.isArray(serverList) || serverList.length === 0) {
    throw new DhError(
      "console_unsupported",
      "The server has no console servers available",
    );
  }
  const servers = serverList
    .map((server) => server.name)
    .filter((name): name is string => typeof name === "string");
  const engines = (config.workerKinds ?? [])
    .filter((engine) => engine.protocols?.includes("Community"))
    .map((engine) => engine.name)
    .filter((name): name is string => typeof name === "string");
  const providerNames = (config.scriptSessionProviders ?? []).map((name) =>
    name.toLowerCase(),
  );
  const languages = (["python", "groovy"] as const).filter((language) =>
    providerNames.some((name) => name.includes(language)),
  );
  if (providerNames.length > 0 && languages.length === 0) {
    throw new DhError(
      "console_unsupported",
      "The server has no supported console languages",
    );
  }
  const engine =
    config.defaultWorkerKind && engines.includes(config.defaultWorkerKind)
      ? config.defaultWorkerKind
      : engines[0];
  if (!engine) {
    throw new DhError(
      "console_unsupported",
      "The server has no console engines available",
    );
  }
  if (
    typeof constants.consoleDefaultHeap !== "number" ||
    !Number.isFinite(constants.consoleDefaultHeap)
  ) {
    throw new DhError(
      "console_unsupported",
      "The server did not provide a console heap default",
    );
  }
  return {
    settings: {
      language: languages.includes("python")
        ? "python"
        : (languages[0] ?? "python"),
      heapSizeGb: constants.consoleDefaultHeap,
      server: servers[0] ?? "",
      engine,
      jvmArgs: [],
      environment: {},
      autoDeleteTimeoutSeconds: DEFAULT_AUTO_DELETE_SECONDS,
    },
    servers,
    engines,
    languages,
  };
}

/** Validate capabilities and create a CLI-owned console on Enterprise. */
export async function createEnterpriseConsole(
  session: Session,
  shortcode: string,
  settings: EffectiveConsoleSettings,
  lifecycle: CommandLifecycle,
): Promise<string> {
  const client = enterpriseClient(session);
  const capabilities = await serverDefaults(session);
  const types = await call<Map<string, { supportsCommunity?: boolean }>>(
    client.getQueryConfigurationTypes?.(),
    "read console query types",
  );
  const consoleType = types.get(QUERY_TYPE);
  if (!consoleType?.supportsCommunity) {
    throw new DhError(
      "console_unsupported",
      "The server does not support interactive consoles",
    );
  }
  validateConsoleSettings(settings, capabilities);
  const name = `${CLI_CONSOLE_PREFIX}${shortcode}`;
  const userInfo = await call<{ operateAs?: string }>(
    client.getUserInfo?.(),
    "read console owner",
  );
  const environment = serializeEnvironment(settings.environment);
  const serial = await call<string>(
    client.createQuery?.(
      consoleQuery(
        name,
        userInfo.operateAs ?? session.operateAs ?? session.user,
        settings,
        environment,
      ),
    ),
    "create console",
  );
  lifecycle.setFailureReport(consoleStartupAcceptedReport(shortcode));
  return serial;
}

/** Reject settings that are not supported by this server before creating. */
function validateConsoleSettings(
  settings: EffectiveConsoleSettings,
  capabilities: Awaited<ReturnType<typeof serverDefaults>>,
): void {
  if (!capabilities.servers.includes(settings.server)) {
    throw new DhError(
      "console_unsupported",
      `Console server "${settings.server}" is not available`,
      `Available servers: ${capabilities.servers.join(", ")}`,
    );
  }
  if (!capabilities.engines.includes(settings.engine)) {
    throw new DhError(
      "console_unsupported",
      `Console engine "${settings.engine}" is not available`,
      `Available engines: ${capabilities.engines.join(", ")}`,
    );
  }
  if (
    capabilities.languages.length > 0 &&
    !capabilities.languages.includes(settings.language)
  ) {
    throw new DhError(
      "console_unsupported",
      `Console language "${settings.language}" is not available`,
      `Available languages: ${capabilities.languages.join(", ")}`,
    );
  }
}

/** Build the parallel list and line-based forms accepted by the API. */
function serializeEnvironment(
  environment: Record<string, string>,
): EnvironmentVariables {
  const entries = Object.entries(environment);
  return {
    entries: entries.flat(),
    text: entries.map(([key, value]) => `${key}=${value}`).join("\n"),
  };
}

/** Build the full query payload, keeping server-specific defaults in one place. */
function consoleQuery(
  name: string,
  owner: string,
  settings: EffectiveConsoleSettings,
  environment: EnvironmentVariables,
) {
  return {
    additionalMemory: 0,
    adminGroups: [],
    assignmentPolicy: null,
    assignmentPolicyParams: null,
    classpathAdditions: [],
    dataMemoryRatio: 0,
    dbServerName: settings.server,
    enabled: true,
    enableGcLogs: false,
    envVarEntries: environment.entries,
    envVars: environment.text,
    genericWorkerControl: null,
    heapSize: settings.heapSizeGb,
    jvmArgs: settings.jvmArgs.join(" "),
    jvmArguments: settings.jvmArgs,
    jvmProfile: "",
    kubernetesControl: null,
    name,
    owner,
    pythonControl: null,
    replicaCount: 1,
    restartUsers: 0,
    scheduling: ["AutoDelete=true", `QueueName=${TEMPORARY_QUEUE}`],
    scriptCode: null,
    scriptLanguage: settings.language === "python" ? "Python" : "Groovy",
    scriptPath: null,
    serial: null,
    spareCount: 0,
    timeout: settings.autoDeleteTimeoutSeconds * 1000,
    type: QUERY_TYPE,
    typeSpecificFields: null,
    viewerGroups: [],
    workerKind: settings.engine,
  };
}

/** Report that creation was accepted but startup has not yet been confirmed. */
function consoleStartupAcceptedReport(shortcode: string) {
  return {
    target: shortcode,
    phase: "starting",
    outcome: "unknown" as const,
    continuation: "continues" as const,
    progress:
      "The server accepted the create request; startup is not confirmed.",
  };
}

/** Submit the deletion request for a known console. */
export async function deleteEnterpriseConsole(
  session: Session,
  console: ConsoleInfo,
): Promise<void> {
  const client = enterpriseClient(session);
  await call<void>(client.deleteQueries?.([console.id]), "delete console");
}

/** Poll until a console is running, fails, or the invocation is cancelled. */
export async function waitForConsole(
  session: Session,
  target: string,
  lifecycle: CommandLifecycle,
): Promise<ConsoleInfo> {
  for (;;) {
    lifecycle.signal.throwIfAborted();
    const console = findEnterpriseConsole(session, target);
    if (console) {
      lifecycle.setFailureReport({
        phase: "starting",
        outcome: "unknown",
        continuation: "continues",
        observation: {
          state: console.state,
          observedAt: new Date().toISOString(),
          ageSeconds: 0,
        },
      });
    }
    if (console && isRunning(console.state)) {
      lifecycle.setFailureReport({
        outcome: "succeeded",
        continuation: "stopped",
        progress: "The console is ready.",
      });
      return console;
    }
    if (console && isTerminal(console.state)) {
      lifecycle.setFailureReport({
        phase: "starting",
        outcome: "failed",
        continuation: "stopped",
        observation: {
          state: console.state,
          observedAt: new Date().toISOString(),
          ageSeconds: 0,
        },
      });
      throw new DhError(
        "console_failed",
        `Console ${target} failed to start`,
        undefined,
        `Last state: ${console.state}`,
      );
    }
    await sleep(POLL_INTERVAL_MS, lifecycle.signal);
  }
}

/** Poll until the server no longer reports the deleted console. */
export async function waitForConsoleDeletion(
  session: Session,
  console: ConsoleInfo,
  lifecycle: CommandLifecycle,
): Promise<void> {
  for (;;) {
    lifecycle.signal.throwIfAborted();
    const current = findEnterpriseConsole(session, console.id);
    if (!current) {
      lifecycle.setFailureReport({
        phase: "deleting",
        outcome: "succeeded",
        continuation: "stopped",
        progress: "The console was deleted.",
      });
      return;
    }
    lifecycle.setFailureReport({
      phase: "deleting",
      outcome: "unknown",
      continuation: "continues",
      observation: {
        state: current.state,
        observedAt: new Date().toISOString(),
        ageSeconds: 0,
      },
    });
    await sleep(POLL_INTERVAL_MS, lifecycle.signal);
  }
}

/** Apply a timeout and translate API failures into console-specific errors. */
async function call<T>(
  value: Promise<T> | undefined,
  operation: string,
): Promise<T> {
  if (!value) {
    throw new DhError("console_unsupported", `The server cannot ${operation}`);
  }
  try {
    return await withTimeout(
      value,
      timeoutMs(),
      () =>
        new DhError(
          "server_unreachable",
          `Timed out while trying to ${operation}`,
        ),
    );
  } catch (error) {
    if (error instanceof DhError) throw error;
    const detail = reasonOf(error);
    if (
      /forbidden|permission|not authorized|unauthorized|403/i.test(detail ?? "")
    ) {
      throw new DhError(
        "console_forbidden",
        `The server denied permission to ${operation}`,
        undefined,
        detail,
      );
    }
    throw new DhError(
      "console_failed",
      `The server could not ${operation}`,
      undefined,
      detail,
    );
  }
}

/** Wait for the polling interval while still reacting promptly to cancellation. */
function sleep(ms: number, signal: AbortSignal): Promise<void> {
  return new Promise((resolve, reject) => {
    const cleanup = () => signal.removeEventListener("abort", onAbort);
    const timer = setTimeout(() => {
      cleanup();
      resolve();
    }, ms);
    const onAbort = () => {
      clearTimeout(timer);
      cleanup();
      reject(signal.reason);
    };
    signal.addEventListener("abort", onAbort, { once: true });
    if (signal.aborted) onAbort();
  });
}
