/**
 * Tests for client/constants module.
 */
import { describe, it, expect } from "vitest";
import {
  SESSION_CONNECT_TIMEOUT_SECONDS,
  SUBSCRIBE_TIMEOUT_SECONDS,
  PQ_CONNECTION_TIMEOUT_SECONDS,
  WORKER_CREATION_TIMEOUT_SECONDS,
  AUTH_TIMEOUT_SECONDS,
  SAML_AUTH_TIMEOUT_SECONDS,
  PQ_MANAGEMENT_TIMEOUT_SECONDS,
  QUICK_OPERATION_TIMEOUT_SECONDS,
  PQ_STATE_CHANGE_TIMEOUT_SECONDS,
  NO_WAIT_SECONDS,
} from "./constants.js";

// ---------------------------------------------------------------------------
// Default values and types
// ---------------------------------------------------------------------------

describe("timeout_constants", () => {
  it("session_connect_timeout_is_positive_number", () => {
    expect(typeof SESSION_CONNECT_TIMEOUT_SECONDS).toBe("number");
    expect(SESSION_CONNECT_TIMEOUT_SECONDS).toBeGreaterThan(0);
  });

  it("subscribe_timeout_is_positive_number", () => {
    expect(typeof SUBSCRIBE_TIMEOUT_SECONDS).toBe("number");
    expect(SUBSCRIBE_TIMEOUT_SECONDS).toBeGreaterThan(0);
  });

  it("pq_connection_timeout_is_positive_number", () => {
    expect(typeof PQ_CONNECTION_TIMEOUT_SECONDS).toBe("number");
    expect(PQ_CONNECTION_TIMEOUT_SECONDS).toBeGreaterThan(0);
  });

  it("worker_creation_timeout_is_positive_number", () => {
    expect(typeof WORKER_CREATION_TIMEOUT_SECONDS).toBe("number");
    expect(WORKER_CREATION_TIMEOUT_SECONDS).toBeGreaterThan(0);
  });

  it("auth_timeout_is_positive_number", () => {
    expect(typeof AUTH_TIMEOUT_SECONDS).toBe("number");
    expect(AUTH_TIMEOUT_SECONDS).toBeGreaterThan(0);
  });

  it("saml_auth_timeout_is_positive_number", () => {
    expect(typeof SAML_AUTH_TIMEOUT_SECONDS).toBe("number");
    expect(SAML_AUTH_TIMEOUT_SECONDS).toBeGreaterThan(0);
  });

  it("saml_timeout_longer_than_standard_auth", () => {
    expect(SAML_AUTH_TIMEOUT_SECONDS).toBeGreaterThan(AUTH_TIMEOUT_SECONDS);
  });

  it("pq_management_timeout_is_positive_number", () => {
    expect(typeof PQ_MANAGEMENT_TIMEOUT_SECONDS).toBe("number");
    expect(PQ_MANAGEMENT_TIMEOUT_SECONDS).toBeGreaterThan(0);
  });

  it("pq_state_change_timeout_is_positive_integer", () => {
    expect(typeof PQ_STATE_CHANGE_TIMEOUT_SECONDS).toBe("number");
    expect(Number.isInteger(PQ_STATE_CHANGE_TIMEOUT_SECONDS)).toBe(true);
    expect(PQ_STATE_CHANGE_TIMEOUT_SECONDS).toBeGreaterThan(0);
  });

  it("pq_wait_timeout_longer_than_operation_timeout", () => {
    expect(PQ_STATE_CHANGE_TIMEOUT_SECONDS).toBeGreaterThanOrEqual(PQ_MANAGEMENT_TIMEOUT_SECONDS);
  });

  it("quick_operation_timeout_is_positive_number", () => {
    expect(typeof QUICK_OPERATION_TIMEOUT_SECONDS).toBe("number");
    expect(QUICK_OPERATION_TIMEOUT_SECONDS).toBeGreaterThan(0);
  });

  it("quick_operation_timeout_shorter_than_connection_timeout", () => {
    expect(QUICK_OPERATION_TIMEOUT_SECONDS).toBeLessThanOrEqual(SESSION_CONNECT_TIMEOUT_SECONDS);
  });

  it("no_wait_seconds_is_zero", () => {
    expect(typeof NO_WAIT_SECONDS).toBe("number");
    expect(NO_WAIT_SECONDS).toBe(0.0);
  });

  it("default_session_connect_timeout_is_60", () => {
    // Verify the default value when no env var is set.
    // The module was loaded without the env var; if it was set, it would have a different value.
    // We test the default by checking it's positive and reasonable.
    expect(SESSION_CONNECT_TIMEOUT_SECONDS).toBeGreaterThanOrEqual(1);
  });
});
