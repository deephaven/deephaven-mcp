/**
 * Tests for client/session module.
 */
import { describe, it, expect, vi, beforeEach } from "vitest";
import {
  BaseSession,
  CoreSession,
  CorePlusSession,
  DhcSession,
  DheSession,
  _isConnectionError,
  _isKeyError,
} from "./session.js";
import { CorePlusQueryInfo } from "./protobuf.js";
import {
  DeephavenConnectionError,
  QueryError,
  ResourceError,
  SessionCreationError,
  SessionError,
} from "../exceptions.js";
import { ConfigurationError } from "../config/index.js";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// eslint-disable-next-line @typescript-eslint/no-explicit-any
type AnyFn = (...args: any[]) => any;

function makeSession(overrides: Partial<DhcSession> = {}): DhcSession {
  return {
    close: vi.fn(),
    isAlive: true,
    tables: ["foo", "bar"],
    emptyTable: vi.fn().mockReturnValue({}),
    importTable: vi.fn().mockReturnValue({}),
    timeTable: vi.fn().mockReturnValue({}),
    mergeTables: vi.fn().mockReturnValue({}),
    query: vi.fn().mockReturnValue({}),
    inputTable: vi.fn().mockReturnValue({}),
    openTable: vi.fn().mockReturnValue({}),
    bindTable: vi.fn(),
    runScript: vi.fn(),
    ...overrides,
  } as unknown as DhcSession;
}

function makeDheSession(overrides: Partial<DheSession> = {}): DheSession {
  return {
    ...makeSession(),
    pqinfo: vi.fn().mockReturnValue({ config: {}, replicas: [], spares: [] }),
    historicalTable: vi.fn().mockReturnValue({}),
    liveTable: vi.fn().mockReturnValue({}),
    catalogTable: vi.fn().mockReturnValue({}),
    ...overrides,
  } as unknown as DheSession;
}

function connectionError(): Error {
  const e = new Error("fail");
  e.name = "ConnectionError";
  return e;
}

function keyError(): Error {
  const e = new Error("not found");
  e.name = "KeyError";
  return e;
}

// ---------------------------------------------------------------------------
// _isConnectionError
// ---------------------------------------------------------------------------

describe("_isConnectionError", () => {
  it("detects_named_connection_error", () => {
    expect(_isConnectionError(connectionError())).toBe(true);
  });

  it("detects_by_constructor_name", () => {
    class ConnectionError extends Error {}
    expect(_isConnectionError(new ConnectionError("fail"))).toBe(true);
  });

  it("false_for_generic", () => {
    expect(_isConnectionError(new Error("boom"))).toBe(false);
  });
});

// ---------------------------------------------------------------------------
// _isKeyError
// ---------------------------------------------------------------------------

describe("_isKeyError", () => {
  it("detects_named_key_error", () => {
    expect(_isKeyError(keyError())).toBe(true);
  });

  it("false_for_generic", () => {
    expect(_isKeyError(new Error("boom"))).toBe(false);
  });
});

// ---------------------------------------------------------------------------
// BaseSession
// ---------------------------------------------------------------------------

describe("BaseSession", () => {
  it("programming_language_property", () => {
    const session = makeSession();
    const wrapper = new BaseSession(session, false, "python");
    expect(wrapper.programmingLanguage).toBe("python");
  });

  it("to_string_returns_string_rep", () => {
    const session = makeSession();
    const wrapper = new BaseSession(session, false, "python");
    expect(typeof wrapper.toString()).toBe("string");
  });
});

// ---------------------------------------------------------------------------
// CoreSession — basic method delegation
// ---------------------------------------------------------------------------

describe("CoreSession", () => {
  let session: DhcSession;
  let wrapper: CoreSession;

  beforeEach(() => {
    session = makeSession();
    wrapper = new CoreSession(session, "python");
  });

  it("programming_language", () => {
    expect(wrapper.programmingLanguage).toBe("python");
  });

  it("close_success", async () => {
    await wrapper.close();
    expect(session.close).toHaveBeenCalled();
  });

  it("close_connection_error", async () => {
    (session.close as AnyFn).mockImplementation(() => { throw connectionError(); });
    await expect(wrapper.close()).rejects.toThrow(DeephavenConnectionError);
  });

  it("close_other_error", async () => {
    (session.close as AnyFn).mockImplementation(() => { throw new Error("fail"); });
    await expect(wrapper.close()).rejects.toThrow(SessionError);
  });

  it("is_alive_success", async () => {
    Object.defineProperty(session, "isAlive", { get: () => true, configurable: true });
    expect(await wrapper.isAlive()).toBe(true);
  });

  it("is_alive_connection_error", async () => {
    Object.defineProperty(session, "isAlive", { get: () => { throw connectionError(); }, configurable: true });
    await expect(wrapper.isAlive()).rejects.toThrow(DeephavenConnectionError);
  });

  it("is_alive_other_error", async () => {
    Object.defineProperty(session, "isAlive", { get: () => { throw new Error("fail"); }, configurable: true });
    await expect(wrapper.isAlive()).rejects.toThrow(SessionError);
  });

  it("tables_success", async () => {
    Object.defineProperty(session, "tables", { get: () => ["foo", "bar"], configurable: true });
    expect(await wrapper.tables()).toEqual(["foo", "bar"]);
  });

  it("tables_connection_error", async () => {
    Object.defineProperty(session, "tables", { get: () => { throw connectionError(); }, configurable: true });
    await expect(wrapper.tables()).rejects.toThrow(DeephavenConnectionError);
  });

  it("tables_other_error", async () => {
    Object.defineProperty(session, "tables", { get: () => { throw new Error("fail"); }, configurable: true });
    await expect(wrapper.tables()).rejects.toThrow(QueryError);
  });

  it("empty_table_success", async () => {
    (session.emptyTable as AnyFn).mockReturnValue({ type: "table" });
    const result = await wrapper.emptyTable(10);
    expect(result).toBeDefined();
  });

  it("empty_table_connection_error", async () => {
    (session.emptyTable as AnyFn).mockImplementation(() => { throw connectionError(); });
    await expect(wrapper.emptyTable(10)).rejects.toThrow(DeephavenConnectionError);
  });

  it("empty_table_other_error", async () => {
    (session.emptyTable as AnyFn).mockImplementation(() => { throw new Error("fail"); });
    await expect(wrapper.emptyTable(-1)).rejects.toThrow(QueryError);
  });

  it("open_table_success", async () => {
    (session.openTable as AnyFn).mockReturnValue({ type: "table" });
    expect(await wrapper.openTable("foo")).toBeDefined();
  });

  it("open_table_key_error", async () => {
    (session.openTable as AnyFn).mockImplementation(() => { throw keyError(); });
    await expect(wrapper.openTable("missing")).rejects.toThrow(ResourceError);
  });

  it("open_table_connection_error", async () => {
    (session.openTable as AnyFn).mockImplementation(() => { throw connectionError(); });
    await expect(wrapper.openTable("conn")).rejects.toThrow(DeephavenConnectionError);
  });

  it("open_table_other_error", async () => {
    (session.openTable as AnyFn).mockImplementation(() => { throw new Error("fail"); });
    await expect(wrapper.openTable("exc")).rejects.toThrow(QueryError);
  });

  it("bind_table_success", async () => {
    await wrapper.bindTable("foo", {} as unknown as import("@deephaven/jsapi-types").Table);
    expect(session.bindTable).toHaveBeenCalled();
  });

  it("bind_table_connection_error", async () => {
    (session.bindTable as AnyFn).mockImplementation(() => { throw connectionError(); });
    await expect(wrapper.bindTable("conn", {} as unknown as import("@deephaven/jsapi-types").Table)).rejects.toThrow(DeephavenConnectionError);
  });

  it("bind_table_other_error", async () => {
    (session.bindTable as AnyFn).mockImplementation(() => { throw new Error("fail"); });
    await expect(wrapper.bindTable("exc", {} as unknown as import("@deephaven/jsapi-types").Table)).rejects.toThrow(QueryError);
  });

  it("query_success", async () => {
    (session.query as AnyFn).mockReturnValue({ type: "query" });
    expect(await wrapper.query({} as unknown as import("@deephaven/jsapi-types").Table)).toBeDefined();
  });

  it("query_connection_error", async () => {
    (session.query as AnyFn).mockImplementation(() => { throw connectionError(); });
    await expect(wrapper.query({} as unknown as import("@deephaven/jsapi-types").Table)).rejects.toThrow(DeephavenConnectionError);
  });

  it("query_other_error", async () => {
    (session.query as AnyFn).mockImplementation(() => { throw new Error("fail"); });
    await expect(wrapper.query({} as unknown as import("@deephaven/jsapi-types").Table)).rejects.toThrow(QueryError);
  });

  it("run_script_success", async () => {
    await wrapper.runScript("print('hi')");
    expect(session.runScript).toHaveBeenCalledWith("print('hi')", undefined);
  });

  it("run_script_connection_error", async () => {
    (session.runScript as AnyFn).mockImplementation(() => { throw connectionError(); });
    await expect(wrapper.runScript("print('hi')")).rejects.toThrow(DeephavenConnectionError);
  });

  it("run_script_other_error", async () => {
    (session.runScript as AnyFn).mockImplementation(() => { throw new Error("fail"); });
    await expect(wrapper.runScript("print('hi')")).rejects.toThrow(QueryError);
  });

  it("time_table_success", async () => {
    (session.timeTable as AnyFn).mockReturnValue({ type: "table" });
    expect(await wrapper.timeTable("PT1S")).toBeDefined();
  });

  it("time_table_connection_error", async () => {
    (session.timeTable as AnyFn).mockImplementation(() => { throw connectionError(); });
    await expect(wrapper.timeTable("PT1S")).rejects.toThrow(DeephavenConnectionError);
  });

  it("time_table_other_error", async () => {
    (session.timeTable as AnyFn).mockImplementation(() => { throw new Error("fail"); });
    await expect(wrapper.timeTable("PT1S")).rejects.toThrow(QueryError);
  });

  it("merge_tables_success", async () => {
    (session.mergeTables as AnyFn).mockReturnValue({ type: "table" });
    expect(await wrapper.mergeTables([])).toBeDefined();
  });

  it("merge_tables_connection_error", async () => {
    (session.mergeTables as AnyFn).mockImplementation(() => { throw connectionError(); });
    await expect(wrapper.mergeTables([])).rejects.toThrow(DeephavenConnectionError);
  });

  it("merge_tables_other_error", async () => {
    (session.mergeTables as AnyFn).mockImplementation(() => { throw new Error("fail"); });
    await expect(wrapper.mergeTables([])).rejects.toThrow(QueryError);
  });

  it("import_table_success", async () => {
    (session.importTable as AnyFn).mockReturnValue({ type: "table" });
    expect(await wrapper.importTable({})).toBeDefined();
  });

  it("import_table_connection_error", async () => {
    (session.importTable as AnyFn).mockImplementation(() => { throw connectionError(); });
    await expect(wrapper.importTable("conn")).rejects.toThrow(DeephavenConnectionError);
  });

  it("import_table_other_error", async () => {
    (session.importTable as AnyFn).mockImplementation(() => { throw new Error("fail"); });
    await expect(wrapper.importTable("bad")).rejects.toThrow(QueryError);
  });

  it("input_table_success", async () => {
    (session.inputTable as AnyFn).mockReturnValue({ type: "input_table" });
    expect(await wrapper.inputTable()).toBeDefined();
  });

  it("input_table_type_error_propagates", async () => {
    (session.inputTable as AnyFn).mockImplementation(() => { throw new TypeError("bad schema"); });
    await expect(wrapper.inputTable("bad")).rejects.toThrow(TypeError);
  });

  it("input_table_connection_error", async () => {
    (session.inputTable as AnyFn).mockImplementation(() => { throw connectionError(); });
    await expect(wrapper.inputTable("conn")).rejects.toThrow(DeephavenConnectionError);
  });

  it("input_table_other_error", async () => {
    (session.inputTable as AnyFn).mockImplementation(() => { throw new Error("fail"); });
    await expect(wrapper.inputTable("exc")).rejects.toThrow(QueryError);
  });
});

// ---------------------------------------------------------------------------
// CoreSession._resolveAuthToken
// ---------------------------------------------------------------------------

describe("CoreSession._resolveAuthToken", () => {
  it("returns_inline_token", () => {
    expect(CoreSession._resolveAuthToken({ auth_token: "tok" })).toBe("tok");
  });

  it("returns_empty_when_neither_set", () => {
    expect(CoreSession._resolveAuthToken({})).toBe("");
  });

  it("raises_when_env_var_not_set", () => {
    delete process.env["MY_MISSING_TOKEN"];
    expect(() => CoreSession._resolveAuthToken({ auth_token_env_var: "MY_MISSING_TOKEN" })).toThrow(ConfigurationError);
  });

  it("resolves_from_env_var", () => {
    process.env["MY_TEST_TOKEN"] = "from_env";
    try {
      expect(CoreSession._resolveAuthToken({ auth_token_env_var: "MY_TEST_TOKEN" })).toBe("from_env");
    } finally {
      delete process.env["MY_TEST_TOKEN"];
    }
  });
});

// ---------------------------------------------------------------------------
// CoreSession.fromConfig
// ---------------------------------------------------------------------------

describe("CoreSession.fromConfig", () => {
  it("creates_session_successfully", async () => {
    const mockSession = makeSession();
    const factory = vi.fn().mockReturnValue(mockSession);
    const result = await CoreSession.fromConfig({ host: "localhost" }, undefined, factory);
    expect(result).toBeInstanceOf(CoreSession);
    expect(factory).toHaveBeenCalled();
  });

  it("programming_language_from_session_type", async () => {
    const mockSession = makeSession();
    const factory = vi.fn().mockReturnValue(mockSession);
    const result = await CoreSession.fromConfig({ host: "localhost", session_type: "groovy" }, undefined, factory);
    expect(result.programmingLanguage).toBe("groovy");
  });

  it("default_programming_language_is_python", async () => {
    const mockSession = makeSession();
    const factory = vi.fn().mockReturnValue(mockSession);
    const result = await CoreSession.fromConfig({ host: "localhost" }, undefined, factory);
    expect(result.programmingLanguage).toBe("python");
  });

  it("session_creation_error_raises_session_creation_error", async () => {
    const factory = vi.fn().mockImplementation(() => { throw new Error("session creation failed"); });
    await expect(CoreSession.fromConfig({ host: "localhost" }, undefined, factory)).rejects.toThrow(SessionCreationError);
    await expect(CoreSession.fromConfig({ host: "localhost" }, undefined, factory)).rejects.toThrow(/Failed to create Deephaven Community/);
  });

  it("timeout_raises_deephaven_connection_error", async () => {
    const factory = vi.fn().mockReturnValue(new Promise<DhcSession>(() => { /* never resolves */ }));
    await expect(
      CoreSession.fromConfig({ host: "localhost" }, 0.001, factory)
    ).rejects.toThrow(DeephavenConnectionError);
    await expect(
      CoreSession.fromConfig({ host: "localhost" }, 0.001, factory)
    ).rejects.toThrow(/timed out/);
  }, 5000);

  it("invalid_config_raises", async () => {
    await expect(CoreSession.fromConfig({ host: "localhost", bad_field: 123 })).rejects.toThrow(/Unknown field 'bad_field'/);
  });

  it("mutually_exclusive_auth_raises", async () => {
    await expect(
      CoreSession.fromConfig({ host: "localhost", auth_token: "tok", auth_token_env_var: "ENV" })
    ).rejects.toThrow(/mutually exclusive/);
  });

  it("auth_token_from_env_var", async () => {
    process.env["MY_TOKEN"] = "from_env";
    const mockSession = makeSession();
    const factory = vi.fn().mockReturnValue(mockSession);
    try {
      const result = await CoreSession.fromConfig({ auth_token_env_var: "MY_TOKEN" }, undefined, factory);
      expect(result).toBeInstanceOf(CoreSession);
    } finally {
      delete process.env["MY_TOKEN"];
    }
  });

  it("missing_auth_token_env_var_raises", async () => {
    delete process.env["MY_MISSING_TOKEN"];
    await expect(
      CoreSession.fromConfig({ auth_token_env_var: "MY_MISSING_TOKEN" })
    ).rejects.toThrow(ConfigurationError);
  });

  it("tls_error_propagates", async () => {
    // When loadBytes fails (e.g., file not found), error propagates out of fromConfig
    await expect(
      CoreSession.fromConfig({ tls_root_certs: "/no/such/file/cert.pem" })
    ).rejects.toThrow(); // file not found
  });
});

// ---------------------------------------------------------------------------
// CorePlusSession
// ---------------------------------------------------------------------------

describe("CorePlusSession", () => {
  let session: DheSession;
  let wrapper: CorePlusSession;

  beforeEach(() => {
    session = makeDheSession();
    wrapper = new CorePlusSession(session, "python");
  });

  it("programming_language", () => {
    expect(wrapper.programmingLanguage).toBe("python");
  });

  it("pqinfo_success", async () => {
    (session.pqinfo as AnyFn).mockReturnValue({ config: { name: "q1" }, state: null, replicas: [], spares: [] });
    const result = await wrapper.pqinfo();
    expect(result).toBeInstanceOf(CorePlusQueryInfo);
  });

  it("pqinfo_connection_error", async () => {
    (session.pqinfo as AnyFn).mockImplementation(() => { throw connectionError(); });
    await expect(wrapper.pqinfo()).rejects.toThrow(DeephavenConnectionError);
  });

  it("pqinfo_other_error", async () => {
    (session.pqinfo as AnyFn).mockImplementation(() => { throw new Error("fail"); });
    await expect(wrapper.pqinfo()).rejects.toThrow(QueryError);
  });

  it("historical_table_success", async () => {
    (session.historicalTable as AnyFn).mockReturnValue({ type: "table" });
    expect(await wrapper.historicalTable("ns", "tbl")).toBeDefined();
  });

  it("historical_table_connection_error", async () => {
    (session.historicalTable as AnyFn).mockImplementation(() => { throw connectionError(); });
    await expect(wrapper.historicalTable("conn", "tbl")).rejects.toThrow(DeephavenConnectionError);
  });

  it("historical_table_key_error", async () => {
    (session.historicalTable as AnyFn).mockImplementation(() => { throw keyError(); });
    await expect(wrapper.historicalTable("missing", "tbl")).rejects.toThrow(ResourceError);
  });

  it("historical_table_other_error", async () => {
    (session.historicalTable as AnyFn).mockImplementation(() => { throw new Error("fail"); });
    await expect(wrapper.historicalTable("exc", "tbl")).rejects.toThrow(QueryError);
  });

  it("live_table_success", async () => {
    (session.liveTable as AnyFn).mockReturnValue({ type: "table" });
    expect(await wrapper.liveTable("ns", "tbl")).toBeDefined();
  });

  it("live_table_connection_error", async () => {
    (session.liveTable as AnyFn).mockImplementation(() => { throw connectionError(); });
    await expect(wrapper.liveTable("conn", "tbl")).rejects.toThrow(DeephavenConnectionError);
  });

  it("live_table_key_error", async () => {
    (session.liveTable as AnyFn).mockImplementation(() => { throw keyError(); });
    await expect(wrapper.liveTable("missing", "tbl")).rejects.toThrow(ResourceError);
  });

  it("live_table_other_error", async () => {
    (session.liveTable as AnyFn).mockImplementation(() => { throw new Error("fail"); });
    await expect(wrapper.liveTable("exc", "tbl")).rejects.toThrow(QueryError);
  });

  it("catalog_table_success", async () => {
    (session.catalogTable as AnyFn).mockReturnValue({ type: "table" });
    expect(await wrapper.catalogTable()).toBeDefined();
  });

  it("catalog_table_connection_error", async () => {
    (session.catalogTable as AnyFn).mockImplementation(() => { throw connectionError(); });
    await expect(wrapper.catalogTable()).rejects.toThrow(DeephavenConnectionError);
  });

  it("catalog_table_other_error", async () => {
    (session.catalogTable as AnyFn).mockImplementation(() => { throw new Error("fail"); });
    await expect(wrapper.catalogTable()).rejects.toThrow(QueryError);
  });
});

// ---------------------------------------------------------------------------
// CoreSession._logSessionCreationErrorDetails (smoke test - shouldn't throw)
// ---------------------------------------------------------------------------

describe("CoreSession._logSessionCreationErrorDetails", () => {
  it("does_not_throw_for_any_error", () => {
    const cases = [
      "failed to get the configuration constants",
      "SSL certificate error",
      "authentication failed",
      "connection timeout",
      "address already in use",
      "name resolution failed",
      "some unknown error",
    ];
    for (const msg of cases) {
      expect(() => CoreSession._logSessionCreationErrorDetails(new Error(msg))).not.toThrow();
    }
  });
});
