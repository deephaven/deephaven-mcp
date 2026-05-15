/**
 * Tests for client/controller-client module.
 */
import { describe, it, expect, vi, beforeEach } from "vitest";
import {
  CorePlusControllerClient,
  DheControllerClient,
  _validateTimeout,
  _DEFAULT_PERMANENT_CONTINUOUS_SCHEDULING,
} from "./controller-client.js";
import { CorePlusQueryConfig, CorePlusQueryInfo, CorePlusQuerySerial } from "./protobuf.js";
import {
  DeephavenConnectionError,
  InternalError,
  QueryError,
} from "../exceptions.js";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function makeSerial(n: number): CorePlusQuerySerial {
  return n as CorePlusQuerySerial;
}

function makeDheControllerClient(
  overrides: Partial<DheControllerClient> = {},
): DheControllerClient {
  return {
    ping: vi.fn().mockResolvedValue(true),
    subscribe: vi.fn().mockResolvedValue(undefined),
    map: vi.fn().mockResolvedValue(new Map()),
    mapAndVersion: vi.fn().mockResolvedValue([new Map(), 0]),
    getSerialForName: vi.fn().mockResolvedValue(42),
    waitForChange: vi.fn().mockResolvedValue(undefined),
    waitForChangeFromVersion: vi.fn().mockResolvedValue(true),
    get: vi.fn().mockResolvedValue({ config: {}, replicas: [], spares: [] }),
    addQuery: vi.fn().mockResolvedValue(99),
    makeTemporaryConfig: vi.fn().mockResolvedValue({ scheduling: [], name: "test" }),
    deleteQuery: vi.fn().mockResolvedValue(undefined),
    modifyQuery: vi.fn().mockResolvedValue(undefined),
    restartQuery: vi.fn().mockResolvedValue(undefined),
    startAndWait: vi.fn().mockResolvedValue(undefined),
    stopQuery: vi.fn().mockResolvedValue(undefined),
    stopAndWait: vi.fn().mockResolvedValue(undefined),
    ...overrides,
  };
}

async function makeSubscribedClient(
  overrides: Partial<DheControllerClient> = {},
): Promise<CorePlusControllerClient> {
  const dhe = makeDheControllerClient(overrides);
  const client = new CorePlusControllerClient(dhe);
  await client.subscribe();
  return client;
}

// ---------------------------------------------------------------------------
// _validateTimeout
// ---------------------------------------------------------------------------

describe("_validateTimeout", () => {
  it("accepts_null", () => {
    expect(() => _validateTimeout(null)).not.toThrow();
  });

  it("accepts_undefined", () => {
    expect(() => _validateTimeout(undefined)).not.toThrow();
  });

  it("accepts_zero", () => {
    expect(() => _validateTimeout(0)).not.toThrow();
  });

  it("accepts_positive", () => {
    expect(() => _validateTimeout(10)).not.toThrow();
  });

  it("rejects_negative", () => {
    expect(() => _validateTimeout(-1)).toThrow(RangeError);
    expect(() => _validateTimeout(-1)).toThrow(/timeout_seconds must be non-negative/);
  });
});

// ---------------------------------------------------------------------------
// _DEFAULT_PERMANENT_CONTINUOUS_SCHEDULING
// ---------------------------------------------------------------------------

describe("_DEFAULT_PERMANENT_CONTINUOUS_SCHEDULING", () => {
  it("contains_scheduler_type", () => {
    expect(_DEFAULT_PERMANENT_CONTINUOUS_SCHEDULING.some((s) => s.startsWith("SchedulerType="))).toBe(true);
  });

  it("contains_restart_when_running", () => {
    expect(_DEFAULT_PERMANENT_CONTINUOUS_SCHEDULING).toContain("RestartWhenRunning=Yes");
  });

  it("contains_scheduling_not_disabled", () => {
    expect(_DEFAULT_PERMANENT_CONTINUOUS_SCHEDULING).toContain("SchedulingDisabled=false");
  });

  it("has_expected_length", () => {
    expect(_DEFAULT_PERMANENT_CONTINUOUS_SCHEDULING.length).toBe(9);
  });
});

// ---------------------------------------------------------------------------
// CorePlusControllerClient constructor
// ---------------------------------------------------------------------------

describe("CorePlusControllerClient constructor", () => {
  it("wraps_dhe_client", () => {
    const dhe = makeDheControllerClient();
    const client = new CorePlusControllerClient(dhe);
    expect(client.wrapped).toBe(dhe);
  });

  it("starts_unsubscribed", async () => {
    const dhe = makeDheControllerClient();
    const client = new CorePlusControllerClient(dhe);
    // map() should raise InternalError when not subscribed
    await expect(client.map()).rejects.toThrow(InternalError);
  });
});

// ---------------------------------------------------------------------------
// ping
// ---------------------------------------------------------------------------

describe("ping", () => {
  it("returns_true_on_success", async () => {
    const dhe = makeDheControllerClient({ ping: vi.fn().mockResolvedValue(true) });
    const client = new CorePlusControllerClient(dhe);
    const result = await client.ping();
    expect(result).toBe(true);
    expect(dhe.ping).toHaveBeenCalled();
  });

  it("returns_false_when_no_cookie", async () => {
    const dhe = makeDheControllerClient({ ping: vi.fn().mockResolvedValue(false) });
    const client = new CorePlusControllerClient(dhe);
    const result = await client.ping();
    expect(result).toBe(false);
  });

  it("connection_error_raises_deephaven_connection_error", async () => {
    const connError = new Error("no connection");
    connError.name = "ConnectionError";
    const dhe = makeDheControllerClient({ ping: vi.fn().mockRejectedValue(connError) });
    const client = new CorePlusControllerClient(dhe);
    await expect(client.ping()).rejects.toThrow(DeephavenConnectionError);
    await expect(client.ping()).rejects.toThrow(/Failed to ping controller/);
  });

  it("other_error_raises_deephaven_connection_error", async () => {
    const dhe = makeDheControllerClient({ ping: vi.fn().mockRejectedValue(new Error("boom")) });
    const client = new CorePlusControllerClient(dhe);
    await expect(client.ping()).rejects.toThrow(DeephavenConnectionError);
    await expect(client.ping()).rejects.toThrow(/Connection error during ping/);
  });
});

// ---------------------------------------------------------------------------
// subscribe
// ---------------------------------------------------------------------------

describe("subscribe", () => {
  it("subscribes_successfully", async () => {
    const dhe = makeDheControllerClient();
    const client = new CorePlusControllerClient(dhe);
    await expect(client.subscribe()).resolves.toBeUndefined();
    expect(dhe.subscribe).toHaveBeenCalledOnce();
  });

  it("is_idempotent", async () => {
    const dhe = makeDheControllerClient();
    const client = new CorePlusControllerClient(dhe);
    await client.subscribe();
    await client.subscribe();
    expect(dhe.subscribe).toHaveBeenCalledOnce();
  });

  it("connection_error_raises_deephaven_connection_error", async () => {
    const connError = new Error("refused");
    connError.name = "ConnectionError";
    const dhe = makeDheControllerClient({ subscribe: vi.fn().mockRejectedValue(connError) });
    const client = new CorePlusControllerClient(dhe);
    await expect(client.subscribe()).rejects.toThrow(DeephavenConnectionError);
    await expect(client.subscribe()).rejects.toThrow(/Unable to connect to controller service/);
  });

  it("other_error_raises_query_error", async () => {
    const dhe = makeDheControllerClient({
      subscribe: vi.fn().mockRejectedValue(new Error("subscription failed")),
    });
    const client = new CorePlusControllerClient(dhe);
    await expect(client.subscribe()).rejects.toThrow(QueryError);
    await expect(client.subscribe()).rejects.toThrow(/Failed to subscribe to persistent query state/);
  });
});

// ---------------------------------------------------------------------------
// map
// ---------------------------------------------------------------------------

describe("map", () => {
  it("raises_internal_error_when_not_subscribed", async () => {
    const dhe = makeDheControllerClient();
    const client = new CorePlusControllerClient(dhe);
    await expect(client.map()).rejects.toThrow(InternalError);
    await expect(client.map()).rejects.toThrow(/subscribe\(\) must be called before map\(\)/);
  });

  it("returns_empty_map_when_no_queries", async () => {
    const client = await makeSubscribedClient();
    const result = await client.map();
    expect(result).toBeInstanceOf(Map);
    expect(result.size).toBe(0);
  });

  it("returns_wrapped_query_info", async () => {
    const serial = 42;
    const rawMap = new Map([[serial, { config: { name: "q1" }, replicas: [], spares: [] }]]);
    const dhe = makeDheControllerClient({ map: vi.fn().mockResolvedValue(rawMap) });
    const client = await makeSubscribedClient();
    // replace subscribe call (already done), inject the map mock
    (client.wrapped as DheControllerClient & { map: ReturnType<typeof vi.fn> }).map =
      vi.fn().mockResolvedValue(rawMap);
    const result = await client.map();
    expect(result.size).toBe(1);
    const info = result.get(makeSerial(serial));
    expect(info).toBeInstanceOf(CorePlusQueryInfo);
  });

  it("connection_error_raises_deephaven_connection_error", async () => {
    const connError = new Error("net error");
    connError.name = "ConnectionError";
    const client = await makeSubscribedClient({ map: vi.fn().mockRejectedValue(connError) });
    // need to re-apply mock since subscribe was already called
    (client.wrapped as { map: ReturnType<typeof vi.fn> }).map = vi.fn().mockRejectedValue(connError);
    await expect(client.map()).rejects.toThrow(DeephavenConnectionError);
  });

  it("other_error_raises_query_error", async () => {
    const client = await makeSubscribedClient();
    (client.wrapped as { map: ReturnType<typeof vi.fn> }).map =
      vi.fn().mockRejectedValue(new Error("boom"));
    await expect(client.map()).rejects.toThrow(QueryError);
    await expect(client.map()).rejects.toThrow(/Failed to retrieve query state/);
  });
});

// ---------------------------------------------------------------------------
// mapAndVersion
// ---------------------------------------------------------------------------

describe("mapAndVersion", () => {
  it("raises_internal_error_when_not_subscribed", async () => {
    const dhe = makeDheControllerClient();
    const client = new CorePlusControllerClient(dhe);
    await expect(client.mapAndVersion()).rejects.toThrow(InternalError);
  });

  it("returns_map_and_version", async () => {
    const serial = 7;
    const rawMap = new Map([[serial, { config: {}, replicas: [], spares: [] }]]);
    const dhe = makeDheControllerClient({
      subscribe: vi.fn().mockResolvedValue(undefined),
      mapAndVersion: vi.fn().mockResolvedValue([rawMap, 5]),
    });
    const client = new CorePlusControllerClient(dhe);
    await client.subscribe();
    const [queryMap, version] = await client.mapAndVersion();
    expect(version).toBe(5);
    expect(queryMap.size).toBe(1);
    expect(queryMap.get(makeSerial(serial))).toBeInstanceOf(CorePlusQueryInfo);
  });
});

// ---------------------------------------------------------------------------
// getSerialForName
// ---------------------------------------------------------------------------

describe("getSerialForName", () => {
  it("raises_internal_error_when_not_subscribed", async () => {
    const dhe = makeDheControllerClient();
    const client = new CorePlusControllerClient(dhe);
    await expect(client.getSerialForName("my-query")).rejects.toThrow(InternalError);
  });

  it("returns_serial", async () => {
    const client = await makeSubscribedClient({
      getSerialForName: vi.fn().mockResolvedValue(99),
    });
    // Need to replace on already-subscribed client's wrapped object
    (client.wrapped as { getSerialForName: ReturnType<typeof vi.fn> }).getSerialForName =
      vi.fn().mockResolvedValue(99);
    const result = await client.getSerialForName("my-query");
    expect(result).toBe(99);
  });

  it("connection_error_raises_deephaven_connection_error", async () => {
    const connError = new Error("lost");
    connError.name = "ConnectionError";
    const client = await makeSubscribedClient();
    (client.wrapped as { getSerialForName: ReturnType<typeof vi.fn> }).getSerialForName =
      vi.fn().mockRejectedValue(connError);
    await expect(client.getSerialForName("my-query")).rejects.toThrow(DeephavenConnectionError);
  });

  it("other_error_raises_query_error", async () => {
    const client = await makeSubscribedClient();
    (client.wrapped as { getSerialForName: ReturnType<typeof vi.fn> }).getSerialForName =
      vi.fn().mockRejectedValue(new Error("not found"));
    await expect(client.getSerialForName("my-query")).rejects.toThrow(QueryError);
    await expect(client.getSerialForName("my-query")).rejects.toThrow(/Failed to find query with name 'my-query'/);
  });
});

// ---------------------------------------------------------------------------
// waitForChange
// ---------------------------------------------------------------------------

describe("waitForChange", () => {
  it("resolves_without_error", async () => {
    const dhe = makeDheControllerClient({ waitForChange: vi.fn().mockResolvedValue(undefined) });
    const client = new CorePlusControllerClient(dhe);
    await expect(client.waitForChange(5)).resolves.toBeUndefined();
  });

  it("connection_error_raises_deephaven_connection_error", async () => {
    const connError = new Error("dropped");
    connError.name = "ConnectionError";
    const dhe = makeDheControllerClient({ waitForChange: vi.fn().mockRejectedValue(connError) });
    const client = new CorePlusControllerClient(dhe);
    await expect(client.waitForChange(5)).rejects.toThrow(DeephavenConnectionError);
  });

  it("other_error_raises_query_error", async () => {
    const dhe = makeDheControllerClient({
      waitForChange: vi.fn().mockRejectedValue(new Error("state error")),
    });
    const client = new CorePlusControllerClient(dhe);
    await expect(client.waitForChange(5)).rejects.toThrow(QueryError);
    await expect(client.waitForChange(5)).rejects.toThrow(/Failed to wait for query state change/);
  });
});

// ---------------------------------------------------------------------------
// waitForChangeFromVersion
// ---------------------------------------------------------------------------

describe("waitForChangeFromVersion", () => {
  it("raises_range_error_for_non_positive_timeout", async () => {
    const dhe = makeDheControllerClient();
    const client = new CorePlusControllerClient(dhe);
    await expect(client.waitForChangeFromVersion(0, 0)).rejects.toThrow(RangeError);
    await expect(client.waitForChangeFromVersion(0, -1)).rejects.toThrow(RangeError);
  });

  it("returns_true_when_version_changed", async () => {
    const dhe = makeDheControllerClient({
      waitForChangeFromVersion: vi.fn().mockResolvedValue(true),
    });
    const client = new CorePlusControllerClient(dhe);
    const result = await client.waitForChangeFromVersion(3, 10);
    expect(result).toBe(true);
    expect(dhe.waitForChangeFromVersion).toHaveBeenCalledWith(3, 10);
  });

  it("returns_false_on_timeout", async () => {
    const dhe = makeDheControllerClient({
      waitForChangeFromVersion: vi.fn().mockResolvedValue(false),
    });
    const client = new CorePlusControllerClient(dhe);
    const result = await client.waitForChangeFromVersion(3, 10);
    expect(result).toBe(false);
  });

  it("connection_error_raises_deephaven_connection_error", async () => {
    const connError = new Error("dropped");
    connError.name = "ConnectionError";
    const dhe = makeDheControllerClient({
      waitForChangeFromVersion: vi.fn().mockRejectedValue(connError),
    });
    const client = new CorePlusControllerClient(dhe);
    await expect(client.waitForChangeFromVersion(0, 5)).rejects.toThrow(DeephavenConnectionError);
  });

  it("other_error_raises_query_error", async () => {
    const dhe = makeDheControllerClient({
      waitForChangeFromVersion: vi.fn().mockRejectedValue(new Error("boom")),
    });
    const client = new CorePlusControllerClient(dhe);
    await expect(client.waitForChangeFromVersion(0, 5)).rejects.toThrow(QueryError);
  });
});

// ---------------------------------------------------------------------------
// get
// ---------------------------------------------------------------------------

describe("get", () => {
  it("returns_query_info", async () => {
    const rawInfo = { config: { name: "q1" }, state: { status: { name: "RUNNING" } }, replicas: [], spares: [] };
    const dhe = makeDheControllerClient({
      subscribe: vi.fn().mockResolvedValue(undefined),
      get: vi.fn().mockResolvedValue(rawInfo),
    });
    const client = new CorePlusControllerClient(dhe);
    await client.subscribe();
    const info = await client.get(makeSerial(42));
    expect(info).toBeInstanceOf(CorePlusQueryInfo);
    expect(info.state?.status.isRunning).toBe(true);
    expect(dhe.get).toHaveBeenCalledWith(42, 0);
  });

  it("connection_error_raises_deephaven_connection_error", async () => {
    const connError = new Error("net");
    connError.name = "ConnectionError";
    const dhe = makeDheControllerClient({
      subscribe: vi.fn().mockResolvedValue(undefined),
      get: vi.fn().mockRejectedValue(connError),
    });
    const client = new CorePlusControllerClient(dhe);
    await client.subscribe();
    await expect(client.get(makeSerial(1))).rejects.toThrow(DeephavenConnectionError);
  });

  it("other_error_raises_query_error", async () => {
    const dhe = makeDheControllerClient({
      subscribe: vi.fn().mockResolvedValue(undefined),
      get: vi.fn().mockRejectedValue(new Error("boom")),
    });
    const client = new CorePlusControllerClient(dhe);
    await client.subscribe();
    await expect(client.get(makeSerial(1))).rejects.toThrow(QueryError);
    await expect(client.get(makeSerial(1))).rejects.toThrow(/Failed to retrieve query/);
  });
});

// ---------------------------------------------------------------------------
// addQuery
// ---------------------------------------------------------------------------

describe("addQuery", () => {
  it("returns_serial_on_success", async () => {
    const dhe = makeDheControllerClient({ addQuery: vi.fn().mockResolvedValue(55) });
    const client = new CorePlusControllerClient(dhe);
    const config = new CorePlusQueryConfig({ name: "test-q" });
    const serial = await client.addQuery(config);
    expect(serial).toBe(55);
    expect(dhe.addQuery).toHaveBeenCalledWith(config.pb);
  });

  it("connection_error_raises_deephaven_connection_error", async () => {
    const connError = new Error("net");
    connError.name = "ConnectionError";
    const dhe = makeDheControllerClient({ addQuery: vi.fn().mockRejectedValue(connError) });
    const client = new CorePlusControllerClient(dhe);
    const config = new CorePlusQueryConfig({ name: "test-q" });
    await expect(client.addQuery(config)).rejects.toThrow(DeephavenConnectionError);
    await expect(client.addQuery(config)).rejects.toThrow(/Unable to connect to controller/);
  });

  it("other_error_raises_query_error", async () => {
    const dhe = makeDheControllerClient({ addQuery: vi.fn().mockRejectedValue(new Error("fail")) });
    const client = new CorePlusControllerClient(dhe);
    const config = new CorePlusQueryConfig({ name: "test-q" });
    await expect(client.addQuery(config)).rejects.toThrow(QueryError);
    await expect(client.addQuery(config)).rejects.toThrow(/Failed to create query/);
  });
});

// ---------------------------------------------------------------------------
// _applyScheduleConfig
// ---------------------------------------------------------------------------

describe("_applyScheduleConfig", () => {
  it("leaves_scheduling_untouched_when_undefined", () => {
    const dhe = makeDheControllerClient();
    const client = new CorePlusControllerClient(dhe);
    const config = { scheduling: ["existing=entry"] };
    client._applyScheduleConfig(config, undefined);
    expect(config.scheduling).toEqual(["existing=entry"]);
  });

  it("clears_scheduling_with_empty_array", () => {
    const dhe = makeDheControllerClient();
    const client = new CorePlusControllerClient(dhe);
    const config = { scheduling: ["existing=entry"] };
    client._applyScheduleConfig(config, []);
    expect(config.scheduling).toEqual([]);
  });

  it("replaces_scheduling_with_provided_list", () => {
    const dhe = makeDheControllerClient();
    const client = new CorePlusControllerClient(dhe);
    const config = { scheduling: ["old=entry"] };
    client._applyScheduleConfig(config, ["new=entry", "other=value"]);
    expect(config.scheduling).toEqual(["new=entry", "other=value"]);
  });
});

// ---------------------------------------------------------------------------
// makePqConfig
// ---------------------------------------------------------------------------

describe("makePqConfig", () => {
  it("raises_range_error_for_mutual_exclusion", async () => {
    const dhe = makeDheControllerClient();
    const client = new CorePlusControllerClient(dhe);
    await expect(
      client.makePqConfig("test", 2, "body", "path"),
    ).rejects.toThrow(RangeError);
    await expect(
      client.makePqConfig("test", 2, "body", "path"),
    ).rejects.toThrow(/script_body and script_path are mutually exclusive/);
  });

  it("creates_config_with_default_scheduler_for_permanent_pq", async () => {
    const dhe = makeDheControllerClient();
    const client = new CorePlusControllerClient(dhe);
    const config = await client.makePqConfig("my-pq", 4);
    expect(config).toBeInstanceOf(CorePlusQueryConfig);
    // Should have default scheduling installed
    expect(config.pb["scheduling"]).toEqual([..._DEFAULT_PERMANENT_CONTINUOUS_SCHEDULING]);
    expect(dhe.makeTemporaryConfig).toHaveBeenCalledWith(
      "my-pq", 4, undefined, undefined, undefined, "DeephavenCommunity",
      600, undefined, undefined,
    );
  });

  it("uses_caller_schedule_for_permanent_pq", async () => {
    const dhe = makeDheControllerClient();
    const client = new CorePlusControllerClient(dhe);
    const customSchedule = ["SchedulerType=custom", "StartTime=08:00:00"];
    const config = await client.makePqConfig(
      "my-pq", 4, undefined, undefined, undefined, undefined, undefined, customSchedule,
    );
    expect(config.pb["scheduling"]).toEqual(customSchedule);
  });

  it("clears_scheduling_when_empty_array", async () => {
    const dhe = makeDheControllerClient();
    const client = new CorePlusControllerClient(dhe);
    const config = await client.makePqConfig(
      "my-pq", 4, undefined, undefined, undefined, undefined, undefined, [],
    );
    expect(config.pb["scheduling"]).toEqual([]);
  });

  it("uses_auto_delete_timeout_for_temporary_pq", async () => {
    const dhe = makeDheControllerClient();
    const client = new CorePlusControllerClient(dhe);
    await client.makePqConfig(
      "temp-pq", 2, undefined, undefined, undefined, undefined, undefined, undefined,
      undefined, "DeephavenCommunity", undefined, undefined, undefined, undefined, undefined,
      undefined, 3600,
    );
    expect(dhe.makeTemporaryConfig).toHaveBeenCalledWith(
      "temp-pq", 2, undefined, undefined, undefined, "DeephavenCommunity",
      3600, undefined, undefined,
    );
  });

  it("applies_script_body", async () => {
    const dhe = makeDheControllerClient();
    const client = new CorePlusControllerClient(dhe);
    const config = await client.makePqConfig("my-pq", 2, "print('hello')");
    expect(config.pb["scriptCode"]).toBe("print('hello')");
  });

  it("applies_programming_language", async () => {
    const dhe = makeDheControllerClient();
    const client = new CorePlusControllerClient(dhe);
    const config = await client.makePqConfig(
      "my-pq", 2, undefined, undefined, "Python",
    );
    expect(config.pb["scriptLanguage"]).toBe("Python");
  });
});

// ---------------------------------------------------------------------------
// deleteQuery
// ---------------------------------------------------------------------------

describe("deleteQuery", () => {
  it("deletes_successfully", async () => {
    const dhe = makeDheControllerClient();
    const client = new CorePlusControllerClient(dhe);
    await expect(client.deleteQuery(makeSerial(10))).resolves.toBeUndefined();
    expect(dhe.deleteQuery).toHaveBeenCalledWith(10);
  });

  it("connection_error_raises_deephaven_connection_error", async () => {
    const connError = new Error("net");
    connError.name = "ConnectionError";
    const dhe = makeDheControllerClient({ deleteQuery: vi.fn().mockRejectedValue(connError) });
    const client = new CorePlusControllerClient(dhe);
    await expect(client.deleteQuery(makeSerial(10))).rejects.toThrow(DeephavenConnectionError);
    await expect(client.deleteQuery(makeSerial(10))).rejects.toThrow(/Unable to connect to controller service/);
  });

  it("other_error_raises_query_error", async () => {
    const dhe = makeDheControllerClient({
      deleteQuery: vi.fn().mockRejectedValue(new Error("not found")),
    });
    const client = new CorePlusControllerClient(dhe);
    await expect(client.deleteQuery(makeSerial(10))).rejects.toThrow(QueryError);
    await expect(client.deleteQuery(makeSerial(10))).rejects.toThrow(/Failed to delete query/);
  });
});

// ---------------------------------------------------------------------------
// modifyQuery
// ---------------------------------------------------------------------------

describe("modifyQuery", () => {
  it("modifies_without_restart", async () => {
    const dhe = makeDheControllerClient();
    const client = new CorePlusControllerClient(dhe);
    const config = new CorePlusQueryConfig({ serial: 5, name: "q" });
    await expect(client.modifyQuery(config, false)).resolves.toBeUndefined();
    expect(dhe.modifyQuery).toHaveBeenCalledWith(config.pb, false);
  });

  it("modifies_with_restart", async () => {
    const dhe = makeDheControllerClient();
    const client = new CorePlusControllerClient(dhe);
    const config = new CorePlusQueryConfig({ serial: 5 });
    await client.modifyQuery(config, true);
    expect(dhe.modifyQuery).toHaveBeenCalledWith(config.pb, true);
  });

  it("connection_error_raises_deephaven_connection_error", async () => {
    const connError = new Error("net");
    connError.name = "ConnectionError";
    const dhe = makeDheControllerClient({ modifyQuery: vi.fn().mockRejectedValue(connError) });
    const client = new CorePlusControllerClient(dhe);
    const config = new CorePlusQueryConfig({ serial: 5 });
    await expect(client.modifyQuery(config)).rejects.toThrow(DeephavenConnectionError);
  });

  it("other_error_raises_query_error", async () => {
    const dhe = makeDheControllerClient({
      modifyQuery: vi.fn().mockRejectedValue(new Error("bad state")),
    });
    const client = new CorePlusControllerClient(dhe);
    const config = new CorePlusQueryConfig({ serial: 5 });
    await expect(client.modifyQuery(config)).rejects.toThrow(QueryError);
    await expect(client.modifyQuery(config)).rejects.toThrow(/Failed to modify query/);
  });
});

// ---------------------------------------------------------------------------
// restartQuery
// ---------------------------------------------------------------------------

describe("restartQuery", () => {
  it("restarts_single_serial", async () => {
    const dhe = makeDheControllerClient();
    const client = new CorePlusControllerClient(dhe);
    await expect(client.restartQuery(makeSerial(10))).resolves.toBeUndefined();
    expect(dhe.restartQuery).toHaveBeenCalledWith(10, undefined);
  });

  it("restarts_multiple_serials", async () => {
    const dhe = makeDheControllerClient();
    const client = new CorePlusControllerClient(dhe);
    const serials = [makeSerial(1), makeSerial(2)];
    await client.restartQuery(serials);
    expect(dhe.restartQuery).toHaveBeenCalledWith([1, 2], undefined);
  });

  it("passes_timeout_seconds", async () => {
    const dhe = makeDheControllerClient();
    const client = new CorePlusControllerClient(dhe);
    await client.restartQuery(makeSerial(1), 30);
    expect(dhe.restartQuery).toHaveBeenCalledWith(1, 30);
  });

  it("connection_error_raises_deephaven_connection_error", async () => {
    const connError = new Error("net");
    connError.name = "ConnectionError";
    const dhe = makeDheControllerClient({ restartQuery: vi.fn().mockRejectedValue(connError) });
    const client = new CorePlusControllerClient(dhe);
    await expect(client.restartQuery(makeSerial(1))).rejects.toThrow(DeephavenConnectionError);
  });

  it("other_error_raises_query_error", async () => {
    const dhe = makeDheControllerClient({
      restartQuery: vi.fn().mockRejectedValue(new Error("boom")),
    });
    const client = new CorePlusControllerClient(dhe);
    await expect(client.restartQuery(makeSerial(1))).rejects.toThrow(QueryError);
    await expect(client.restartQuery(makeSerial(1))).rejects.toThrow(/Failed to restart query\(s\)/);
  });
});

// ---------------------------------------------------------------------------
// startAndWait
// ---------------------------------------------------------------------------

describe("startAndWait", () => {
  it("starts_successfully", async () => {
    const dhe = makeDheControllerClient();
    const client = new CorePlusControllerClient(dhe);
    await expect(client.startAndWait(makeSerial(5))).resolves.toBeUndefined();
    expect(dhe.startAndWait).toHaveBeenCalledWith(5, expect.any(Number));
  });

  it("rejects_negative_timeout", async () => {
    const dhe = makeDheControllerClient();
    const client = new CorePlusControllerClient(dhe);
    await expect(client.startAndWait(makeSerial(5), -1)).rejects.toThrow(RangeError);
  });

  it("connection_error_raises_deephaven_connection_error", async () => {
    const connError = new Error("net");
    connError.name = "ConnectionError";
    const dhe = makeDheControllerClient({ startAndWait: vi.fn().mockRejectedValue(connError) });
    const client = new CorePlusControllerClient(dhe);
    await expect(client.startAndWait(makeSerial(5))).rejects.toThrow(DeephavenConnectionError);
  });

  it("other_error_raises_query_error", async () => {
    const dhe = makeDheControllerClient({
      startAndWait: vi.fn().mockRejectedValue(new Error("failed to start")),
    });
    const client = new CorePlusControllerClient(dhe);
    await expect(client.startAndWait(makeSerial(5))).rejects.toThrow(QueryError);
    await expect(client.startAndWait(makeSerial(5))).rejects.toThrow(/Failed to start query/);
  });
});

// ---------------------------------------------------------------------------
// stopQuery
// ---------------------------------------------------------------------------

describe("stopQuery", () => {
  it("stops_single_serial", async () => {
    const dhe = makeDheControllerClient();
    const client = new CorePlusControllerClient(dhe);
    await expect(client.stopQuery(makeSerial(10))).resolves.toBeUndefined();
    expect(dhe.stopQuery).toHaveBeenCalledWith(10, undefined);
  });

  it("stops_multiple_serials", async () => {
    const dhe = makeDheControllerClient();
    const client = new CorePlusControllerClient(dhe);
    await client.stopQuery([makeSerial(1), makeSerial(2)]);
    expect(dhe.stopQuery).toHaveBeenCalledWith([1, 2], undefined);
  });

  it("rejects_negative_timeout", async () => {
    const dhe = makeDheControllerClient();
    const client = new CorePlusControllerClient(dhe);
    await expect(client.stopQuery(makeSerial(1), -1)).rejects.toThrow(RangeError);
  });

  it("connection_error_raises_deephaven_connection_error", async () => {
    const connError = new Error("net");
    connError.name = "ConnectionError";
    const dhe = makeDheControllerClient({ stopQuery: vi.fn().mockRejectedValue(connError) });
    const client = new CorePlusControllerClient(dhe);
    await expect(client.stopQuery(makeSerial(1))).rejects.toThrow(DeephavenConnectionError);
  });

  it("other_error_raises_query_error", async () => {
    const dhe = makeDheControllerClient({
      stopQuery: vi.fn().mockRejectedValue(new Error("boom")),
    });
    const client = new CorePlusControllerClient(dhe);
    await expect(client.stopQuery(makeSerial(1))).rejects.toThrow(QueryError);
    await expect(client.stopQuery(makeSerial(1))).rejects.toThrow(/Failed to stop query\(s\)/);
  });
});

// ---------------------------------------------------------------------------
// stopAndWait
// ---------------------------------------------------------------------------

describe("stopAndWait", () => {
  it("stops_successfully", async () => {
    const dhe = makeDheControllerClient();
    const client = new CorePlusControllerClient(dhe);
    await expect(client.stopAndWait(makeSerial(5))).resolves.toBeUndefined();
    expect(dhe.stopAndWait).toHaveBeenCalledWith(5, expect.any(Number));
  });

  it("rejects_negative_timeout", async () => {
    const dhe = makeDheControllerClient();
    const client = new CorePlusControllerClient(dhe);
    await expect(client.stopAndWait(makeSerial(5), -1)).rejects.toThrow(RangeError);
  });

  it("connection_error_raises_deephaven_connection_error", async () => {
    const connError = new Error("net");
    connError.name = "ConnectionError";
    const dhe = makeDheControllerClient({ stopAndWait: vi.fn().mockRejectedValue(connError) });
    const client = new CorePlusControllerClient(dhe);
    await expect(client.stopAndWait(makeSerial(5))).rejects.toThrow(DeephavenConnectionError);
  });

  it("other_error_raises_query_error", async () => {
    const dhe = makeDheControllerClient({
      stopAndWait: vi.fn().mockRejectedValue(new Error("stuck")),
    });
    const client = new CorePlusControllerClient(dhe);
    await expect(client.stopAndWait(makeSerial(5))).rejects.toThrow(QueryError);
    await expect(client.stopAndWait(makeSerial(5))).rejects.toThrow(/Failed to stop query/);
  });
});
