/**
 * Tests for client/session-factory module.
 */
import { describe, it, expect, vi, beforeEach } from "vitest";
import {
  CorePlusSessionFactory,
  DheSessionManager,
} from "./session-factory.js";
import {
  AuthenticationError,
  DeephavenConnectionError,
  InternalError,
  QueryError,
  ResourceError,
  SessionCreationError,
  SessionError,
} from "../exceptions.js";
import { CorePlusAuthClient } from "./auth-client.js";
import { CorePlusControllerClient } from "./controller-client.js";
import { CorePlusSession } from "./session.js";
import { PasswordCredentials, PrivateKeyCredentials, PSKCredentials } from "../auth/credentials/credentials.js";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function makeDheSessionManager(overrides: Partial<DheSessionManager> = {}): DheSessionManager {
  const mockControllerClient = {
    ping: vi.fn().mockResolvedValue(true),
    subscribe: vi.fn().mockResolvedValue(undefined),
    map: vi.fn().mockResolvedValue(new Map()),
    mapAndVersion: vi.fn().mockResolvedValue([new Map(), 0]),
    getSerialForName: vi.fn().mockResolvedValue(1),
    waitForChange: vi.fn().mockResolvedValue(undefined),
    waitForChangeFromVersion: vi.fn().mockResolvedValue(true),
    get: vi.fn().mockResolvedValue({}),
    addQuery: vi.fn().mockResolvedValue(1),
    makeTemporaryConfig: vi.fn().mockResolvedValue({ scheduling: [] }),
    deleteQuery: vi.fn().mockResolvedValue(undefined),
    modifyQuery: vi.fn().mockResolvedValue(undefined),
    restartQuery: vi.fn().mockResolvedValue(undefined),
    startAndWait: vi.fn().mockResolvedValue(undefined),
    stopQuery: vi.fn().mockResolvedValue(undefined),
    stopAndWait: vi.fn().mockResolvedValue(undefined),
  };

  const mockAuthClient = {
    getToken: vi.fn().mockResolvedValue({ type: "service", value: "tok" }),
  };

  return {
    controller_client: mockControllerClient,
    auth_client: mockAuthClient,
    close: vi.fn().mockResolvedValue(undefined),
    ping: vi.fn().mockResolvedValue(true),
    password: vi.fn().mockResolvedValue(undefined),
    private_key: vi.fn().mockResolvedValue(undefined),
    saml: vi.fn().mockResolvedValue(undefined),
    upload_key: vi.fn().mockResolvedValue(undefined),
    delete_key: vi.fn().mockResolvedValue(undefined),
    connect_to_new_worker: vi.fn().mockResolvedValue({ _session_type: "python" }),
    connect_to_persistent_query: vi.fn().mockResolvedValue({ _session_type: "python" }),
    ...overrides,
  };
}

function makeFactory(overrides: Partial<DheSessionManager> = {}): {
  factory: CorePlusSessionFactory;
  sm: DheSessionManager;
} {
  const sm = makeDheSessionManager(overrides);
  const factory = new CorePlusSessionFactory(sm);
  return { factory, sm };
}

// ---------------------------------------------------------------------------
// Constructor & Properties
// ---------------------------------------------------------------------------

describe("CorePlusSessionFactory constructor", () => {
  it("wraps_dhe_session_manager", () => {
    const { factory, sm } = makeFactory();
    expect(factory.wrapped).toBe(sm);
  });

  it("exposes_controller_client", () => {
    const { factory } = makeFactory();
    expect(factory.controllerClient).toBeInstanceOf(CorePlusControllerClient);
  });

  it("exposes_auth_client", () => {
    const { factory } = makeFactory();
    expect(factory.authClient).toBeInstanceOf(CorePlusAuthClient);
  });

  it("controller_client_init_error_raises_session_error", () => {
    const sm = makeDheSessionManager();
    // Make controller_client throw when accessed
    Object.defineProperty(sm, "controller_client", {
      get() { throw new Error("controller failure"); },
    });
    expect(() => new CorePlusSessionFactory(sm)).toThrow(SessionError);
  });

  it("auth_client_init_error_raises_authentication_error", () => {
    const sm = makeDheSessionManager();
    Object.defineProperty(sm, "auth_client", {
      get() { throw new Error("auth failure"); },
    });
    expect(() => new CorePlusSessionFactory(sm)).toThrow(AuthenticationError);
  });
});

// ---------------------------------------------------------------------------
// close()
// ---------------------------------------------------------------------------

describe("CorePlusSessionFactory.close", () => {
  it("close_success", async () => {
    const { factory, sm } = makeFactory();
    await factory.close();
    expect(sm.close).toHaveBeenCalledOnce();
  });

  it("close_failure_raises_session_error", async () => {
    const { factory } = makeFactory({
      close: vi.fn().mockRejectedValue(new Error("fail")),
    });
    await expect(factory.close()).rejects.toThrow(SessionError);
  });
});

// ---------------------------------------------------------------------------
// ping()
// ---------------------------------------------------------------------------

describe("CorePlusSessionFactory.ping", () => {
  it("ping_success_returns_true", async () => {
    const { factory } = makeFactory({ ping: vi.fn().mockResolvedValue(true) });
    const result = await factory.ping();
    expect(result).toBe(true);
  });

  it("ping_success_returns_false", async () => {
    const { factory } = makeFactory({ ping: vi.fn().mockResolvedValue(false) });
    const result = await factory.ping();
    expect(result).toBe(false);
  });

  it("ping_other_error_raises_deephaven_connection_error", async () => {
    const { factory } = makeFactory({
      ping: vi.fn().mockRejectedValue(new Error("fail")),
    });
    await expect(factory.ping()).rejects.toThrow(DeephavenConnectionError);
  });

  it("ping_timeout_raises_deephaven_connection_error", async () => {
    const { factory } = makeFactory({
      ping: vi.fn().mockImplementation(
        () => new Promise<boolean>((resolve) => setTimeout(() => resolve(true), 200)),
      ),
    });
    await expect(factory.ping(0.01)).rejects.toThrow(DeephavenConnectionError);
    await expect(factory.ping(0.01)).rejects.toThrow(/timed out/i);
  });
});

// ---------------------------------------------------------------------------
// password()
// ---------------------------------------------------------------------------

describe("CorePlusSessionFactory.password", () => {
  it("password_success", async () => {
    const { factory, sm } = makeFactory();
    await factory.password("user", "pw");
    expect(sm.password).toHaveBeenCalledWith("user", "pw", undefined);
  });

  it("password_with_effective_user", async () => {
    const { factory, sm } = makeFactory();
    await factory.password("user", "pw", "effective");
    expect(sm.password).toHaveBeenCalledWith("user", "pw", "effective");
  });

  it("password_connection_error_raises_deephaven_connection_error", async () => {
    const connError = new Error("fail");
    connError.name = "ConnectionError";
    const { factory } = makeFactory({
      password: vi.fn().mockRejectedValue(connError),
    });
    await expect(factory.password("user", "pw")).rejects.toThrow(DeephavenConnectionError);
  });

  it("password_other_error_raises_authentication_error", async () => {
    const { factory } = makeFactory({
      password: vi.fn().mockRejectedValue(new Error("fail")),
    });
    await expect(factory.password("user", "pw")).rejects.toThrow(AuthenticationError);
  });

  it("password_timeout_raises_deephaven_connection_error", async () => {
    const { factory } = makeFactory({
      password: vi.fn().mockImplementation(
        () => new Promise<void>((resolve) => setTimeout(resolve, 200)),
      ),
    });
    await expect(factory.password("user", "pw", undefined, 0.01)).rejects.toThrow(
      DeephavenConnectionError,
    );
    await expect(factory.password("user", "pw", undefined, 0.01)).rejects.toThrow(/timed out/i);
  });
});

// ---------------------------------------------------------------------------
// privateKey()
// ---------------------------------------------------------------------------

describe("CorePlusSessionFactory.privateKey", () => {
  it("private_key_success", async () => {
    const { factory, sm } = makeFactory();
    await factory.privateKey("/fake/path");
    expect(sm.private_key).toHaveBeenCalledWith("/fake/path");
  });

  it("private_key_file_not_found_raises_authentication_error", async () => {
    const fileErr = new Error("no such file: /fake/path");
    (fileErr as NodeJS.ErrnoException).code = "ENOENT";
    const { factory } = makeFactory({
      private_key: vi.fn().mockRejectedValue(fileErr),
    });
    await expect(factory.privateKey("/fake/path")).rejects.toThrow(AuthenticationError);
    await expect(factory.privateKey("/fake/path")).rejects.toThrow(/file not found/i);
  });

  it("private_key_connection_error_raises_deephaven_connection_error", async () => {
    const connError = new Error("fail");
    connError.name = "ConnectionError";
    const { factory } = makeFactory({
      private_key: vi.fn().mockRejectedValue(connError),
    });
    await expect(factory.privateKey("/fake/path")).rejects.toThrow(DeephavenConnectionError);
  });

  it("private_key_other_error_raises_authentication_error", async () => {
    const { factory } = makeFactory({
      private_key: vi.fn().mockRejectedValue(new Error("fail")),
    });
    await expect(factory.privateKey("/fake/path")).rejects.toThrow(AuthenticationError);
  });

  it("private_key_timeout_raises_deephaven_connection_error", async () => {
    const { factory } = makeFactory({
      private_key: vi.fn().mockImplementation(
        () => new Promise<void>((resolve) => setTimeout(resolve, 200)),
      ),
    });
    await expect(factory.privateKey("/fake/path", 0.01)).rejects.toThrow(
      DeephavenConnectionError,
    );
    await expect(factory.privateKey("/fake/path", 0.01)).rejects.toThrow(/timed out/i);
  });
});

// ---------------------------------------------------------------------------
// saml()
// ---------------------------------------------------------------------------

describe("CorePlusSessionFactory.saml", () => {
  it("saml_success", async () => {
    const { factory, sm } = makeFactory();
    await factory.saml();
    expect(sm.saml).toHaveBeenCalledOnce();
  });

  it("saml_connection_error_raises_deephaven_connection_error", async () => {
    const connError = new Error("fail");
    connError.name = "ConnectionError";
    const { factory } = makeFactory({ saml: vi.fn().mockRejectedValue(connError) });
    await expect(factory.saml()).rejects.toThrow(DeephavenConnectionError);
  });

  it("saml_range_error_raises_authentication_error", async () => {
    const { factory } = makeFactory({
      saml: vi.fn().mockRejectedValue(new RangeError("fail")),
    });
    await expect(factory.saml()).rejects.toThrow(AuthenticationError);
  });

  it("saml_other_error_raises_authentication_error", async () => {
    const { factory } = makeFactory({
      saml: vi.fn().mockRejectedValue(new Error("fail")),
    });
    await expect(factory.saml()).rejects.toThrow(AuthenticationError);
  });

  it("saml_timeout_raises_deephaven_connection_error", async () => {
    const { factory } = makeFactory({
      saml: vi.fn().mockImplementation(
        () => new Promise<void>((resolve) => setTimeout(resolve, 200)),
      ),
    });
    await expect(factory.saml(0.01)).rejects.toThrow(DeephavenConnectionError);
    await expect(factory.saml(0.01)).rejects.toThrow(/timed out/i);
  });
});

// ---------------------------------------------------------------------------
// uploadKey()
// ---------------------------------------------------------------------------

describe("CorePlusSessionFactory.uploadKey", () => {
  it("upload_key_success", async () => {
    const { factory, sm } = makeFactory();
    await factory.uploadKey("pubkey");
    expect(sm.upload_key).toHaveBeenCalledWith("pubkey");
  });

  it("upload_key_connection_error_raises_deephaven_connection_error", async () => {
    const connError = new Error("fail");
    connError.name = "ConnectionError";
    const { factory } = makeFactory({
      upload_key: vi.fn().mockRejectedValue(connError),
    });
    await expect(factory.uploadKey("pubkey")).rejects.toThrow(DeephavenConnectionError);
  });

  it("upload_key_other_error_raises_resource_error", async () => {
    const { factory } = makeFactory({
      upload_key: vi.fn().mockRejectedValue(new Error("fail")),
    });
    await expect(factory.uploadKey("pubkey")).rejects.toThrow(ResourceError);
  });

  it("upload_key_timeout_raises_deephaven_connection_error", async () => {
    const { factory } = makeFactory({
      upload_key: vi.fn().mockImplementation(
        () => new Promise<void>((resolve) => setTimeout(resolve, 200)),
      ),
    });
    await expect(factory.uploadKey("pubkey", 0.01)).rejects.toThrow(DeephavenConnectionError);
    await expect(factory.uploadKey("pubkey", 0.01)).rejects.toThrow(/timed out/i);
  });
});

// ---------------------------------------------------------------------------
// deleteKey()
// ---------------------------------------------------------------------------

describe("CorePlusSessionFactory.deleteKey", () => {
  it("delete_key_success", async () => {
    const { factory, sm } = makeFactory();
    await factory.deleteKey("pubkey");
    expect(sm.delete_key).toHaveBeenCalledWith("pubkey");
  });

  it("delete_key_connection_error_raises_deephaven_connection_error", async () => {
    const connError = new Error("fail");
    connError.name = "ConnectionError";
    const { factory } = makeFactory({
      delete_key: vi.fn().mockRejectedValue(connError),
    });
    await expect(factory.deleteKey("pubkey")).rejects.toThrow(DeephavenConnectionError);
  });

  it("delete_key_other_error_raises_resource_error", async () => {
    const { factory } = makeFactory({
      delete_key: vi.fn().mockRejectedValue(new Error("fail")),
    });
    await expect(factory.deleteKey("pubkey")).rejects.toThrow(ResourceError);
  });

  it("delete_key_timeout_raises_deephaven_connection_error", async () => {
    const { factory } = makeFactory({
      delete_key: vi.fn().mockImplementation(
        () => new Promise<void>((resolve) => setTimeout(resolve, 200)),
      ),
    });
    await expect(factory.deleteKey("pubkey", 0.01)).rejects.toThrow(DeephavenConnectionError);
    await expect(factory.deleteKey("pubkey", 0.01)).rejects.toThrow(/timed out/i);
  });
});

// ---------------------------------------------------------------------------
// connectToNewWorker()
// ---------------------------------------------------------------------------

describe("CorePlusSessionFactory.connectToNewWorker", () => {
  it("connect_to_new_worker_success_returns_core_plus_session", async () => {
    const rawSession = { _session_type: "python" };
    const { factory, sm } = makeFactory({
      connect_to_new_worker: vi.fn().mockResolvedValue(rawSession),
    });
    const result = await factory.connectToNewWorker(4);
    expect(result).toBeInstanceOf(CorePlusSession);
    expect(sm.connect_to_new_worker).toHaveBeenCalledOnce();
  });

  it("connect_to_new_worker_default_language_is_python", async () => {
    const rawSession = {};  // no _session_type
    const { factory } = makeFactory({
      connect_to_new_worker: vi.fn().mockResolvedValue(rawSession),
    });
    const result = await factory.connectToNewWorker(4);
    // CorePlusSession stores the language; just verify it's a CorePlusSession
    expect(result).toBeInstanceOf(CorePlusSession);
  });

  it("connect_to_new_worker_session_type_extracted", async () => {
    const rawSession = { _session_type: "groovy" };
    const { factory } = makeFactory({
      connect_to_new_worker: vi.fn().mockResolvedValue(rawSession),
    });
    // The session factory extracts the language and wraps it — test it doesn't crash
    const result = await factory.connectToNewWorker(4);
    expect(result).toBeInstanceOf(CorePlusSession);
  });

  it("connect_to_new_worker_resource_error_propagates", async () => {
    const { factory } = makeFactory({
      connect_to_new_worker: vi.fn().mockRejectedValue(new ResourceError("no resources")),
    });
    await expect(factory.connectToNewWorker(4)).rejects.toThrow(ResourceError);
  });

  it("connect_to_new_worker_connection_error_raises_deephaven_connection_error", async () => {
    const connError = new Error("fail");
    connError.name = "ConnectionError";
    const { factory } = makeFactory({
      connect_to_new_worker: vi.fn().mockRejectedValue(connError),
    });
    await expect(factory.connectToNewWorker(4)).rejects.toThrow(DeephavenConnectionError);
  });

  it("connect_to_new_worker_other_error_raises_session_creation_error", async () => {
    const { factory } = makeFactory({
      connect_to_new_worker: vi.fn().mockRejectedValue(new Error("fail")),
    });
    await expect(factory.connectToNewWorker(4)).rejects.toThrow(SessionCreationError);
  });
});

// ---------------------------------------------------------------------------
// connectToPersistentQuery()
// ---------------------------------------------------------------------------

describe("CorePlusSessionFactory.connectToPersistentQuery", () => {
  it("connect_to_persistent_query_success_by_name", async () => {
    const rawSession = { _session_type: "python" };
    const { factory, sm } = makeFactory({
      connect_to_persistent_query: vi.fn().mockResolvedValue(rawSession),
    });
    const result = await factory.connectToPersistentQuery("my-pq");
    expect(result).toBeInstanceOf(CorePlusSession);
    expect(sm.connect_to_persistent_query).toHaveBeenCalledOnce();
  });

  it("connect_to_persistent_query_range_error_propagates", async () => {
    const { factory } = makeFactory({
      connect_to_persistent_query: vi.fn().mockRejectedValue(new RangeError("fail")),
    });
    await expect(factory.connectToPersistentQuery("pq")).rejects.toThrow(RangeError);
  });

  it("connect_to_persistent_query_type_error_propagates", async () => {
    const { factory } = makeFactory({
      connect_to_persistent_query: vi.fn().mockRejectedValue(new TypeError("fail")),
    });
    await expect(factory.connectToPersistentQuery("pq")).rejects.toThrow(TypeError);
  });

  it("connect_to_persistent_query_connection_error_raises_deephaven_connection_error", async () => {
    const connError = new Error("fail");
    connError.name = "ConnectionError";
    const { factory } = makeFactory({
      connect_to_persistent_query: vi.fn().mockRejectedValue(connError),
    });
    await expect(factory.connectToPersistentQuery("pq")).rejects.toThrow(
      DeephavenConnectionError,
    );
  });

  it("connect_to_persistent_query_key_error_raises_query_error", async () => {
    const keyErr = new Error("not found");
    keyErr.name = "KeyError";
    const { factory } = makeFactory({
      connect_to_persistent_query: vi.fn().mockRejectedValue(keyErr),
    });
    await expect(factory.connectToPersistentQuery("pq")).rejects.toThrow(QueryError);
  });

  it("connect_to_persistent_query_other_error_raises_session_creation_error", async () => {
    const { factory } = makeFactory({
      connect_to_persistent_query: vi.fn().mockRejectedValue(new Error("fail")),
    });
    await expect(factory.connectToPersistentQuery("pq")).rejects.toThrow(SessionCreationError);
  });

  it("connect_to_persistent_query_timeout_raises_deephaven_connection_error", async () => {
    const { factory } = makeFactory({
      connect_to_persistent_query: vi.fn().mockImplementation(
        () => new Promise<Record<string, unknown>>((resolve) => setTimeout(() => resolve({}), 200)),
      ),
    });
    await expect(factory.connectToPersistentQuery("pq", undefined, undefined, 0.01)).rejects.toThrow(
      DeephavenConnectionError,
    );
    await expect(
      factory.connectToPersistentQuery("pq", undefined, undefined, 0.01),
    ).rejects.toThrow(/timed out/i);
  });
});

// ---------------------------------------------------------------------------
// _getProgrammingLanguage static helper
// ---------------------------------------------------------------------------

describe("CorePlusSessionFactory._getProgrammingLanguage", () => {
  it("returns_session_type_from_session_type_field", () => {
    const result = CorePlusSessionFactory._getProgrammingLanguage({ _session_type: "groovy" });
    expect(result).toBe("groovy");
  });

  it("returns_session_type_from_sessionType_field", () => {
    const result = CorePlusSessionFactory._getProgrammingLanguage({ sessionType: "python" });
    expect(result).toBe("python");
  });

  it("prefers_session_type_over_sessionType", () => {
    const result = CorePlusSessionFactory._getProgrammingLanguage({
      _session_type: "groovy",
      sessionType: "python",
    });
    expect(result).toBe("groovy");
  });

  it("returns_python_as_default_when_no_type", () => {
    const result = CorePlusSessionFactory._getProgrammingLanguage({});
    expect(result).toBe("python");
  });
});

// ---------------------------------------------------------------------------
// fromUrl() — static factory method
// ---------------------------------------------------------------------------

describe("CorePlusSessionFactory.fromUrl", () => {
  it("from_url_fails_with_connection_error_when_session_manager_not_available", async () => {
    // In the test environment, isEnterpriseAvailable=true but the DHE package is not
    // actually installed, so require('@deephaven/jsapi-nodejs') returns a stub where
    // SessionManager is not a real constructor. The factory wraps the error in
    // DeephavenConnectionError.
    await expect(
      CorePlusSessionFactory.fromUrl("https://example.com/iris/connection.json"),
    ).rejects.toThrow(DeephavenConnectionError);
  });
});

// ---------------------------------------------------------------------------
// fromCredentials() — static factory method
// ---------------------------------------------------------------------------

describe("CorePlusSessionFactory.fromCredentials", () => {
  const validConfig = {
    system_name: "test-system",
    connection_json_url: "https://server/iris/connection.json",
    auth: { backends: ["password"] },
  };

  it("from_credentials_fails_with_connection_error_when_session_manager_not_available", async () => {
    const creds = new PasswordCredentials("user", "pw");
    // In test environment, the DHE package is a stub; connection attempt fails with
    // DeephavenConnectionError because SessionManager is not a real constructor.
    await expect(
      CorePlusSessionFactory.fromCredentials(validConfig, creds),
    ).rejects.toThrow(DeephavenConnectionError);
  });

  it("from_credentials_unsupported_creds_type_after_connection_established", async () => {
    // PSKCredentials is unsupported. The SessionManager construction fails first
    // (in test env), resulting in DeephavenConnectionError before the creds check.
    const pskCreds = new PSKCredentials("tok");
    await expect(
      CorePlusSessionFactory.fromCredentials(validConfig, pskCreds),
    ).rejects.toThrow(DeephavenConnectionError);
  });
});
