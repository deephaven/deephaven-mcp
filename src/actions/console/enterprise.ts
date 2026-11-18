import type { AuthenticatedEnterpriseClient } from "@deephaven-enterprise/jsapi-nodejs";
import type {
  ConsoleServerAddress,
  PlainEditableQueryInfo,
  QueryConstants,
  QueryInfo,
  ServerConfigValues,
} from "@deephaven-enterprise/jsapi-types";
import type { CommandLifecycle } from "../../cli/lifecycle.ts";
import { DhError, reasonOf, timeoutMs, withTimeout } from "../../errors.ts";
import type { Session } from "../auth/signin.ts";
import {
  consoleDeletedReport,
  consoleObservationReport,
  consoleReadyReport,
  consoleStartupAcceptedReport,
  consoleStartupFailedReport,
} from "./reporting.ts";
import type { EffectiveConsoleSettings } from "./settings.ts";

/** Server-reported state and settings exposed by console list and detail. */
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

/** Enterprise settings are available; only Community's shared console omits them. */
export interface EnterpriseConsoleInfo extends ConsoleInfo {
  heapSizeGb: number;
  server: string;
  engine: string;
  jvmArgs: string;
}

/** A snapshot of available settings and derived defaults for one server. */
export interface ConsoleServerSettings {
  settings: EffectiveConsoleSettings;
  dataMemoryRatio: number;
  jvmProfile: string;
  scheduling: string[];
  servers: string[];
  engines: string[];
  languages: string[];
}

export const CLI_CONSOLE_PREFIX = "IC - dh CLI - ";
const QUERY_TYPE = "InteractiveConsole";
const DEFAULT_AUTO_DELETE_SECONDS = 600;
const STARTUP_TIMEOUT_MS = 600_000;
const TEMPORARY_EXPIRATION_MS = 2 * 24 * 60 * 60 * 1000;
const POLL_INTERVAL_MS = 1_000;

type ConsoleQuery = Pick<
  QueryInfo,
  | "name"
  | "serial"
  | "type"
  | "designated"
  | "replicas"
  | "scriptLanguage"
  | "heapSize"
  | "dbServerName"
  | "workerKind"
  | "jvmArguments"
  | "envVarEntries"
  | "typeSpecificFields"
>;

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
function queries(session: Session): QueryInfo[] {
  const client = enterpriseClient(session);
  return client.getKnownConfigs().filter(isCliConsole);
}

/** Identify CLI-created consoles by their query type and reserved name. */
function isCliConsole(query: QueryInfo): boolean {
  return (
    query.type === QUERY_TYPE && /^IC - dh CLI - [a-z0-9]{6}$/.test(query.name)
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
export function consoleState(
  query: Pick<QueryInfo, "designated" | "replicas">,
): string {
  return query.designated?.status ?? query.replicas[0]?.status ?? "Unknown";
}

/** Convert an Enterprise query into the stable console shape used by actions. */
export function consoleInfo(query: ConsoleQuery): EnterpriseConsoleInfo {
  const name = query.name;
  return {
    id: query.serial,
    shortcode: shortcodeFromName(name),
    name,
    state: consoleState(query),
    language: query.scriptLanguage,
    heapSizeGb: query.heapSize,
    server: query.dbServerName,
    engine: query.workerKind,
    jvmArgs: query.jvmArguments.join(" "),
    environment: environmentOf(query),
    autoDeleteTimeoutSeconds: autoDeleteTimeoutOf(query),
  };
}

/** Read the idle grace period; query.timeout instead controls initialization. */
function autoDeleteTimeoutOf(query: ConsoleQuery): number | null {
  const field = query.typeSpecificFields?.TerminationDelay;
  if (!field) return null;
  const milliseconds = Number(field.value);
  if (
    field.type !== "long" ||
    (typeof field.value !== "string" && typeof field.value !== "number") ||
    !Number.isSafeInteger(milliseconds) ||
    milliseconds <= 0
  ) {
    throw new DhError(
      "console_failed",
      "The server returned an invalid console idle timeout",
    );
  }
  return milliseconds / 1000;
}

/** Decode the API's alternating environment variable names and values. */
function environmentOf(query: ConsoleQuery): Record<string, string> {
  const entries = query.envVarEntries;
  const environment: Record<string, string> = {};
  for (let i = 0; i + 1 < entries.length; i += 2) {
    const key = entries[i];
    const value = entries[i + 1];
    if (key !== undefined && value !== undefined) environment[key] = value;
  }
  return environment;
}

/** List the CLI-owned InteractiveConsole queries on a server. */
export function listEnterpriseConsoles(
  session: Session,
): EnterpriseConsoleInfo[] {
  return queries(session).map(consoleInfo);
}

/** Find a CLI console by its server ID or its user-facing shortcode. */
export function findEnterpriseConsole(
  session: Session,
  target: string,
): EnterpriseConsoleInfo | undefined {
  const found = queries(session).find(
    (query) =>
      query.serial === target ||
      query.name === `${CLI_CONSOLE_PREFIX}${target}`,
  );
  return found ? consoleInfo(found) : undefined;
}

/** Read server capabilities and derive defaults for a new console. */
export async function serverDefaults(
  session: Session,
): Promise<ConsoleServerSettings> {
  const client = enterpriseClient(session);
  const config = await call(
    client.getServerConfigValues(),
    "read console server settings",
  );
  const serverList = await call(client.getDbServers(), "list console servers");
  const constants = await call(
    client.getQueryConstants(),
    "read console defaults",
  );
  const queues = await call(
    client.getTemporaryQueueNames(),
    "list temporary console queues",
  );
  const queue = queues[0];
  if (!queue) {
    throw new DhError(
      "console_unsupported",
      "The server has no temporary console queues",
    );
  }
  return deriveConsoleServerSettings(config, serverList, constants, queue);
}

/** Derive CLI defaults from raw server metadata without making network calls. */
function deriveConsoleServerSettings(
  config: ServerConfigValues,
  serverList: ConsoleServerAddress[],
  constants: QueryConstants,
  queue: string,
): ConsoleServerSettings {
  const firstServer = serverList[0];
  if (!firstServer) {
    throw new DhError(
      "console_unsupported",
      "The server has no console servers available",
    );
  }
  const servers = serverList.map((server) => server.name);
  const engines = config.workerKinds
    .filter((engine) => engine.protocols.includes("Community"))
    .map((engine) => engine.name);
  const languages = supportedConsoleLanguages(config.scriptSessionProviders);
  const [firstLanguage] = languages;
  if (!firstLanguage) {
    throw new DhError(
      "console_unsupported",
      "The server has no supported console languages",
    );
  }
  const engine = defaultConsoleEngine(config.defaultWorkerKind, engines);
  if (!config.jvmProfileDefault) {
    throw new DhError(
      "console_unsupported",
      "The server did not provide a default console JVM profile",
    );
  }
  if (!Number.isFinite(constants.consoleDefaultHeap)) {
    throw new DhError(
      "console_unsupported",
      "The server did not provide a console heap default",
    );
  }
  if (
    !Number.isFinite(constants.minDataRatio) ||
    !Number.isFinite(constants.maxDataRatio) ||
    constants.minDataRatio < 0 ||
    constants.minDataRatio > constants.maxDataRatio
  ) {
    throw new DhError(
      "console_unsupported",
      "The server did not provide valid console data-memory ratio limits",
    );
  }
  return {
    dataMemoryRatio: constants.minDataRatio,
    jvmProfile: config.jvmProfileDefault,
    scheduling: temporaryConsoleScheduling(
      constants.defaultSchedulingDetails,
      queue,
    ),
    settings: {
      language: languages.includes("python") ? "python" : firstLanguage,
      heapSizeGb: constants.consoleDefaultHeap,
      server: firstServer.name,
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

/** Retain required scheduler initialization fields while selecting temporary-console lifecycle. */
function temporaryConsoleScheduling(
  defaults: readonly string[],
  queue: string,
): string[] {
  return [
    "SchedulerType=com.illumon.iris.controller.IrisQuerySchedulerTemporary",
    ...defaults.filter(
      (entry) =>
        !/^(SchedulerType|AutoDelete|TemporaryAutoDelete|QueueName|TemporaryQueueName|TemporaryExpirationTimeMillis)=/.test(
          entry,
        ),
    ),
    "TemporaryAutoDelete=true",
    `TemporaryQueueName=${queue}`,
    `TemporaryExpirationTimeMillis=${TEMPORARY_EXPIRATION_MS}`,
  ];
}

/** Detect console languages from the server's script-session providers. */
function supportedConsoleLanguages(
  providers: string[],
): ("python" | "groovy")[] {
  const names = providers.map((name) => name.toLowerCase());
  const languages = (["python", "groovy"] as const).filter((language) =>
    names.some((name) => name.includes(language)),
  );
  return languages;
}

/** Prefer the configured engine when it supports consoles, otherwise the first available. */
function defaultConsoleEngine(preferred: string, engines: string[]): string {
  if (preferred && engines.includes(preferred)) return preferred;
  const first = engines[0];
  if (first) return first;
  throw new DhError(
    "console_unsupported",
    "The server has no console engines available",
  );
}

/** Validate capabilities and create a CLI-owned console on Enterprise. */
export async function createEnterpriseConsole(
  session: Session,
  shortcode: string,
  settings: EffectiveConsoleSettings,
  lifecycle: CommandLifecycle,
  serverSettings?: ConsoleServerSettings,
): Promise<string> {
  const client = enterpriseClient(session);
  const capabilities = serverSettings ?? (await serverDefaults(session));
  const types = await call(
    client.getQueryConfigurationTypes(),
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
  const userInfo = await call(client.getUserInfo(), "read console owner");
  const serial = await call(
    client.createQuery(
      consoleQuery(
        name,
        userInfo.operateAs,
        settings,
        capabilities.dataMemoryRatio,
        capabilities.jvmProfile,
        capabilities.scheduling,
      ),
    ),
    "create console",
  );
  lifecycle.setFailureReport(
    consoleStartupAcceptedReport(shortcode, session.profileName),
  );
  return serial;
}

/** Reject settings that are not supported by this server before creating. */
function validateConsoleSettings(
  settings: EffectiveConsoleSettings,
  capabilities: ConsoleServerSettings,
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
  if (!capabilities.languages.includes(settings.language)) {
    throw new DhError(
      "console_unsupported",
      `Console language "${settings.language}" is not available`,
      `Available languages: ${capabilities.languages.join(", ")}`,
    );
  }
}

/** Build the full query payload, keeping server-specific defaults in one place. */
function consoleQuery(
  name: string,
  owner: string,
  settings: EffectiveConsoleSettings,
  dataMemoryRatio: number,
  jvmProfile: string,
  scheduling: string[],
): PlainEditableQueryInfo {
  return {
    additionalMemory: 0,
    adminGroups: [],
    assignmentPolicy: null,
    assignmentPolicyParams: null,
    classpathAdditions: [],
    dataMemoryRatio,
    dbServerName: settings.server,
    enabled: true,
    enableGcLogs: false,
    envVarEntries: Object.entries(settings.environment).flat(),
    genericWorkerControl: null,
    heapSize: settings.heapSizeGb,
    jvmArguments: settings.jvmArgs,
    jvmProfile,
    kubernetesControl: null,
    name,
    owner,
    pythonControl: null,
    replicaCount: 1,
    restartUsers: 1,
    scheduling,
    scriptCode: null,
    scriptLanguage: settings.language === "python" ? "Python" : "Groovy",
    scriptPath: null,
    serial: null,
    spareCount: 0,
    timeout: STARTUP_TIMEOUT_MS,
    type: QUERY_TYPE,
    typeSpecificFields: {
      TerminationDelay: {
        type: "long",
        value: String(settings.autoDeleteTimeoutSeconds * 1000),
      },
    },
    viewerGroups: [],
    workerKind: settings.engine,
  };
}

/** Submit the deletion request for a known console. */
export async function deleteEnterpriseConsole(
  session: Session,
  console: ConsoleInfo,
): Promise<void> {
  const client = enterpriseClient(session);
  await call(client.deleteQueries([console.id]), "delete console");
}

/** Poll until a console is running, fails, or the invocation is cancelled. */
export async function waitForConsole(
  session: Session,
  target: string,
  lifecycle: CommandLifecycle,
): Promise<EnterpriseConsoleInfo> {
  let observed = false;
  const discoveryDeadline = performance.now() + timeoutMs();
  for (;;) {
    lifecycle.signal.throwIfAborted();
    const console = findEnterpriseConsole(session, target);
    if (!console && observed) {
      lifecycle.setFailureReport(consoleStartupFailedReport("Deleted"));
      throw new DhError(
        "console_not_found",
        `Console ${target} disappeared before startup completed`,
      );
    }
    if (!console && performance.now() >= discoveryDeadline) {
      throw new DhError(
        "console_failed",
        `Console ${target} was not discovered before the wait timed out`,
        "The create outcome is unconfirmed. Check the console before retrying.",
      );
    }
    if (console) {
      observed = true;
      lifecycle.setFailureReport(
        consoleObservationReport(
          "starting",
          console.state,
          lifecycle.failureReport.human,
        ),
      );
    }
    if (console && isRunning(console.state)) {
      lifecycle.setFailureReport(consoleReadyReport());
      return console;
    }
    if (console && isTerminal(console.state)) {
      lifecycle.setFailureReport(consoleStartupFailedReport(console.state));
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
      lifecycle.setFailureReport(consoleDeletedReport());
      return;
    }
    lifecycle.setFailureReport(
      consoleObservationReport(
        "deleting",
        current.state,
        lifecycle.failureReport.human,
      ),
    );
    await sleep(POLL_INTERVAL_MS, lifecycle.signal);
  }
}

/** Apply a timeout and translate API failures into console-specific errors. */
async function call<T>(value: Promise<T>, operation: string): Promise<T> {
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
