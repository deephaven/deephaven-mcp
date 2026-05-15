/**
 * Tests for client/auth-client module.
 */
import { describe, it, expect, vi } from "vitest";
import { CorePlusAuthClient, DheAuthClient } from "./auth-client.js";
import { CorePlusToken } from "./protobuf.js";
import { AuthenticationError, DeephavenConnectionError } from "../exceptions.js";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function makeDheAuthClient(overrides: Partial<DheAuthClient> = {}): DheAuthClient {
  return {
    getToken: vi.fn().mockResolvedValue({ type: "service", value: "tok" }),
    ...overrides,
  };
}

// ---------------------------------------------------------------------------
// CorePlusAuthClient
// ---------------------------------------------------------------------------

describe("CorePlusAuthClient", () => {
  it("get_token_success", async () => {
    const dheClient = makeDheAuthClient({
      getToken: vi.fn().mockResolvedValue({ type: "service", value: "tok3" }),
    });
    const client = new CorePlusAuthClient(dheClient);
    const result = await client.getToken("PersistentQueryController", 123);
    expect(result).toBeInstanceOf(CorePlusToken);
    expect(dheClient.getToken).toHaveBeenCalledWith("PersistentQueryController", 123);
  });

  it("get_token_default_timeout", async () => {
    const dheClient = makeDheAuthClient({
      getToken: vi.fn().mockResolvedValue({ type: "service", value: "tok-default" }),
    });
    const client = new CorePlusAuthClient(dheClient);
    const result = await client.getToken("svc");
    expect(result).toBeInstanceOf(CorePlusToken);
    expect(dheClient.getToken).toHaveBeenCalledWith("svc", undefined);
  });

  it("get_token_null_timeout", async () => {
    const dheClient = makeDheAuthClient({
      getToken: vi.fn().mockResolvedValue({ type: "service" }),
    });
    const client = new CorePlusAuthClient(dheClient);
    await client.getToken("svc", null);
    expect(dheClient.getToken).toHaveBeenCalledWith("svc", undefined);
  });

  it("get_token_connection_error_raises_deephaven_connection_error", async () => {
    const connError = new Error("fail");
    connError.name = "ConnectionError";
    const dheClient = makeDheAuthClient({
      getToken: vi.fn().mockRejectedValue(connError),
    });
    const client = new CorePlusAuthClient(dheClient);
    await expect(client.getToken("svc")).rejects.toThrow(DeephavenConnectionError);
    await expect(client.getToken("svc")).rejects.toThrow(/Unable to connect/);
  });

  it("get_token_other_error_raises_authentication_error", async () => {
    const dheClient = makeDheAuthClient({
      getToken: vi.fn().mockRejectedValue(new Error("boom")),
    });
    const client = new CorePlusAuthClient(dheClient);
    await expect(client.getToken("svc")).rejects.toThrow(AuthenticationError);
    await expect(client.getToken("svc")).rejects.toThrow(/Token retrieval failed/);
  });

  it("wrapped_is_dhe_auth_client", () => {
    const dheClient = makeDheAuthClient();
    const client = new CorePlusAuthClient(dheClient);
    expect(client.wrapped).toBe(dheClient);
  });
});
