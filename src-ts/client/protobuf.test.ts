/**
 * Tests for client/protobuf module.
 */
import { describe, it, expect } from "vitest";
import {
  PQ_STATES,
  DheWrapper,
  CorePlusQueryStatus,
  CorePlusToken,
  CorePlusQueryConfig,
  CorePlusQueryState,
  CorePlusQueryInfo,
  ProtobufWrapper,
} from "./protobuf.js";

// ---------------------------------------------------------------------------
// PQ_STATES
// ---------------------------------------------------------------------------

describe("PQ_STATES", () => {
  it("running_is_active", () => {
    expect(PQ_STATES["RUNNING"]).toBe("ACTIVE");
  });

  it("executing_is_active", () => {
    expect(PQ_STATES["EXECUTING"]).toBe("ACTIVE");
  });

  it("stopped_is_terminal", () => {
    expect(PQ_STATES["STOPPED"]).toBe("TERMINAL");
  });

  it("failed_is_terminal", () => {
    expect(PQ_STATES["FAILED"]).toBe("TERMINAL");
  });

  it("killed_is_terminal", () => {
    expect(PQ_STATES["KILLED"]).toBe("TERMINAL");
  });

  it("completed_is_terminal", () => {
    expect(PQ_STATES["COMPLETED"]).toBe("TERMINAL");
  });

  it("uninitialized_is_transitional", () => {
    expect(PQ_STATES["UNINITIALIZED"]).toBe("TRANSITIONAL");
  });

  it("initializing_is_transitional", () => {
    expect(PQ_STATES["INITIALIZING"]).toBe("TRANSITIONAL");
  });

  it("unspecified_is_invalid", () => {
    expect(PQ_STATES["UNSPECIFIED"]).toBe("INVALID");
  });
});

// ---------------------------------------------------------------------------
// DheWrapper
// ---------------------------------------------------------------------------

describe("DheWrapper", () => {
  it("raises_on_null", () => {
    expect(() => new DheWrapper(null as unknown as Record<string, unknown>)).toThrow(
      /Protobuf message cannot be None/,
    );
  });

  it("raises_on_undefined", () => {
    expect(() => new DheWrapper(undefined as unknown as Record<string, unknown>)).toThrow(
      /Protobuf message cannot be None/,
    );
  });

  it("pb_property_returns_wrapped_object", () => {
    const obj = { field1: 123, field2: "abc" };
    const wrapper = new DheWrapper(obj);
    expect(wrapper.pb).toBe(obj);
  });

  it("to_dict_returns_copy", () => {
    const obj = { a: 1, b: "x" };
    const wrapper = new DheWrapper(obj);
    const dict = wrapper.toDict();
    expect(dict).toEqual({ a: 1, b: "x" });
    expect(dict).not.toBe(obj);
  });

  it("to_json_returns_string", () => {
    const obj = { a: 1, b: "x" };
    const wrapper = new DheWrapper(obj);
    const json = wrapper.toJson();
    expect(typeof json).toBe("string");
    expect(JSON.parse(json)).toEqual({ a: 1, b: "x" });
  });

  it("to_string_includes_class_name", () => {
    const obj = { a: 1 };
    const wrapper = new DheWrapper(obj);
    expect(wrapper.toString()).toContain("DheWrapper");
  });
});

// ---------------------------------------------------------------------------
// ProtobufWrapper (alias)
// ---------------------------------------------------------------------------

it("protobuf_wrapper_is_alias_for_dhe_wrapper", () => {
  expect(ProtobufWrapper).toBe(DheWrapper);
});

// ---------------------------------------------------------------------------
// CorePlusQueryStatus
// ---------------------------------------------------------------------------

describe("CorePlusQueryStatus", () => {
  it("name_strips_pqs_prefix", () => {
    const status = new CorePlusQueryStatus({ name: "PQS_RUNNING" });
    expect(status.name).toBe("RUNNING");
  });

  it("name_without_prefix", () => {
    const status = new CorePlusQueryStatus({ name: "RUNNING" });
    expect(status.name).toBe("RUNNING");
  });

  it("to_string_returns_name", () => {
    const status = new CorePlusQueryStatus({ name: "RUNNING" });
    expect(status.toString()).toBe("RUNNING");
  });

  it("is_running_true_for_running", () => {
    const status = new CorePlusQueryStatus({ name: "RUNNING" });
    expect(status.isRunning).toBe(true);
  });

  it("is_running_true_for_executing", () => {
    const status = new CorePlusQueryStatus({ name: "EXECUTING" });
    expect(status.isRunning).toBe(true);
  });

  it("is_running_false_for_stopped", () => {
    const status = new CorePlusQueryStatus({ name: "STOPPED" });
    expect(status.isRunning).toBe(false);
  });

  it("is_completed_true", () => {
    const status = new CorePlusQueryStatus({ name: "COMPLETED" });
    expect(status.isCompleted).toBe(true);
  });

  it("is_completed_false_for_running", () => {
    const status = new CorePlusQueryStatus({ name: "RUNNING" });
    expect(status.isCompleted).toBe(false);
  });

  it("is_terminal_true_for_failed", () => {
    const status = new CorePlusQueryStatus({ name: "FAILED" });
    expect(status.isTerminal).toBe(true);
  });

  it("is_terminal_false_for_running", () => {
    const status = new CorePlusQueryStatus({ name: "RUNNING" });
    expect(status.isTerminal).toBe(false);
  });

  it("is_uninitialized_true", () => {
    const status = new CorePlusQueryStatus({ name: "UNINITIALIZED" });
    expect(status.isUninitialized).toBe(true);
  });

  it("is_initializing_true", () => {
    const status = new CorePlusQueryStatus({ name: "INITIALIZING" });
    expect(status.isInitializing).toBe(true);
  });

  it("equals_with_instance", () => {
    const s1 = new CorePlusQueryStatus({ name: "RUNNING" });
    const s2 = new CorePlusQueryStatus({ name: "RUNNING" });
    expect(s1.equals(s2)).toBe(true);
  });

  it("equals_with_string", () => {
    const s = new CorePlusQueryStatus({ name: "RUNNING" });
    expect(s.equals("RUNNING")).toBe(true);
    expect(s.equals("STOPPED")).toBe(false);
  });
});

// ---------------------------------------------------------------------------
// CorePlusToken
// ---------------------------------------------------------------------------

describe("CorePlusToken", () => {
  it("wraps_token_object", () => {
    const token = { type: "service", value: "tok123" };
    const wrapper = new CorePlusToken(token);
    expect(wrapper.pb).toBe(token);
  });

  it("raises_on_null", () => {
    expect(() => new CorePlusToken(null as unknown as Record<string, unknown>)).toThrow(
      /Protobuf message cannot be None/,
    );
  });
});

// ---------------------------------------------------------------------------
// CorePlusQueryConfig
// ---------------------------------------------------------------------------

describe("CorePlusQueryConfig", () => {
  it("wraps_config_object", () => {
    const config = { name: "my_query", heap_size_mb: 4096 };
    const wrapper = new CorePlusQueryConfig(config);
    expect(wrapper.pb).toBe(config);
    expect(wrapper.toDict()["name"]).toBe("my_query");
  });
});

// ---------------------------------------------------------------------------
// CorePlusQueryState
// ---------------------------------------------------------------------------

describe("CorePlusQueryState", () => {
  it("status_property_returns_core_plus_query_status", () => {
    const state = new CorePlusQueryState({ status: { name: "RUNNING" } });
    expect(state.status).toBeInstanceOf(CorePlusQueryStatus);
    expect(state.status.isRunning).toBe(true);
  });

  it("status_defaults_to_unspecified_when_absent", () => {
    const state = new CorePlusQueryState({});
    expect(state.status.name).toBe("UNSPECIFIED");
  });
});

// ---------------------------------------------------------------------------
// CorePlusQueryInfo
// ---------------------------------------------------------------------------

describe("CorePlusQueryInfo", () => {
  it("config_property_returns_core_plus_query_config", () => {
    const info = new CorePlusQueryInfo({
      config: { name: "q1" },
      state: { status: { name: "RUNNING" } },
      replicas: [],
      spares: [],
    });
    expect(info.config).toBeInstanceOf(CorePlusQueryConfig);
    expect(info.config.toDict()["name"]).toBe("q1");
  });

  it("state_property_returns_core_plus_query_state", () => {
    const info = new CorePlusQueryInfo({
      config: {},
      state: { status: { name: "RUNNING" } },
      replicas: [],
      spares: [],
    });
    expect(info.state).toBeInstanceOf(CorePlusQueryState);
    expect(info.state!.status.isRunning).toBe(true);
  });

  it("state_is_undefined_when_absent", () => {
    const info = new CorePlusQueryInfo({ config: {}, replicas: [], spares: [] });
    expect(info.state).toBeUndefined();
  });

  it("replicas_and_spares_are_arrays", () => {
    const info = new CorePlusQueryInfo({
      config: {},
      replicas: [{ status: { name: "RUNNING" } }],
      spares: [{ status: { name: "STOPPED" } }],
    });
    expect(info.replicas).toHaveLength(1);
    expect(info.spares).toHaveLength(1);
    expect(info.replicas[0]).toBeInstanceOf(CorePlusQueryState);
    expect(info.spares[0]).toBeInstanceOf(CorePlusQueryState);
  });

  it("empty_replicas_and_spares", () => {
    const info = new CorePlusQueryInfo({ config: {}, replicas: [], spares: [] });
    expect(info.replicas).toEqual([]);
    expect(info.spares).toEqual([]);
  });

  it("raises_on_null", () => {
    expect(() => new CorePlusQueryInfo(null as unknown as Record<string, unknown>)).toThrow(
      /Protobuf message cannot be None/,
    );
  });
});
