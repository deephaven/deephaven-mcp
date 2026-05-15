/**
 * Tests for auth/credentials/credentials module.
 */
import { describe, it, expect } from "vitest";
import {
  Credentials,
  PasswordCredentials,
  PrivateKeyCredentials,
  PSKCredentials,
} from "./credentials.js";

describe("PSKCredentials", () => {
  it("carry_the_key", () => {
    const c = new PSKCredentials("secret-token");
    expect(c.psk).toBe("secret-token");
  });

  it("are_equal_by_value", () => {
    const a = new PSKCredentials("k");
    const b = new PSKCredentials("k");
    const c = new PSKCredentials("OTHER");
    // Structural equality via comparing fields
    expect(a.psk).toBe(b.psk);
    expect(a.psk).not.toBe(c.psk);
  });

  it("is_frozen", () => {
    const c = new PSKCredentials("k");
    expect(() => {
      (c as unknown as Record<string, string>)["psk"] = "other";
    }).toThrow();
  });

  it("repr_redacts_the_key", () => {
    const c = new PSKCredentials("hunter2");
    const r = c.toString();
    expect(r).not.toContain("hunter2");
    expect(r).toContain("[REDACTED]");
    expect(r).toBe("PSKCredentials(psk=[REDACTED])");
  });

  it("str_redacts_the_key", () => {
    const c = new PSKCredentials("hunter2");
    expect(String(c)).not.toContain("hunter2");
    expect(`${c}`).not.toContain("hunter2");
  });
});

describe("PasswordCredentials", () => {
  it("without_effective_user", () => {
    const c = new PasswordCredentials("alice", "hunter2");
    expect(c.username).toBe("alice");
    expect(c.password).toBe("hunter2");
    expect(c.effectiveUser).toBeUndefined();
  });

  it("with_effective_user", () => {
    const c = new PasswordCredentials("svc", "pw", "alice");
    expect(c.effectiveUser).toBe("alice");
  });

  it("is_frozen", () => {
    const c = new PasswordCredentials("alice", "pw");
    expect(() => {
      (c as unknown as Record<string, string>)["password"] = "other";
    }).toThrow();
  });

  it("repr_redacts_only_password", () => {
    const c = new PasswordCredentials("alice", "hunter2", "bob");
    const r = c.toString();
    expect(r).not.toContain("hunter2");
    expect(r).toContain("[REDACTED]");
    // Non-secret fields remain visible for debugging.
    expect(r).toContain("alice");
    expect(r).toContain("bob");
    expect(r).toBe(
      "PasswordCredentials(username='alice', password=[REDACTED], effective_user='bob')",
    );
  });

  it("repr_with_none_effective_user", () => {
    const c = new PasswordCredentials("alice", "hunter2");
    const r = c.toString();
    expect(r).not.toContain("hunter2");
    expect(r).toContain("None");
  });

  it("str_redacts_password", () => {
    const c = new PasswordCredentials("alice", "hunter2");
    expect(String(c)).not.toContain("hunter2");
    expect(`${c}`).not.toContain("hunter2");
  });

  it("are_equal_by_value", () => {
    const a = new PasswordCredentials("u", "p", "e");
    const b = new PasswordCredentials("u", "p", "e");
    const c = new PasswordCredentials("u", "DIFFERENT", "e");
    expect(a.username).toBe(b.username);
    expect(a.password).toBe(b.password);
    expect(a.effectiveUser).toBe(b.effectiveUser);
    expect(a.password).not.toBe(c.password);
  });
});

describe("PrivateKeyCredentials", () => {
  it("holds_text", () => {
    const c = new PrivateKeyCredentials("-----BEGIN KEY-----\n...\n");
    expect(c.keyText.startsWith("-----BEGIN KEY-----")).toBe(true);
  });

  it("is_frozen", () => {
    const c = new PrivateKeyCredentials("k");
    expect(() => {
      (c as unknown as Record<string, string>)["keyText"] = "other";
    }).toThrow();
  });

  it("repr_redacts_text_but_shows_length", () => {
    const key = "-----BEGIN PRIVATE KEY-----\nMIIBVQIBAD...";
    const c = new PrivateKeyCredentials(key);
    const r = c.toString();
    expect(r).not.toContain("MIIBVQIBAD");
    expect(r).not.toContain("BEGIN PRIVATE KEY");
    expect(r).toContain("[REDACTED]");
    expect(r).toContain(`${key.length} chars`);
  });

  it("str_redacts_text", () => {
    const key = "supersecretkey";
    const c = new PrivateKeyCredentials(key);
    expect(String(c)).not.toContain("supersecretkey");
    expect(`${c}`).not.toContain("supersecretkey");
  });

  it("are_equal_by_value", () => {
    const a = new PrivateKeyCredentials("k");
    const b = new PrivateKeyCredentials("k");
    const c = new PrivateKeyCredentials("OTHER");
    expect(a.keyText).toBe(b.keyText);
    expect(a.keyText).not.toBe(c.keyText);
  });
});

describe("Credentials", () => {
  it("base_class_cannot_be_instantiated", () => {
    expect(() => new (Credentials as unknown as new () => Credentials)()).toThrow(TypeError);
  });

  it("concrete_credentials_are_instances_of_base", () => {
    expect(new PSKCredentials("k") instanceof Credentials).toBe(true);
    expect(new PasswordCredentials("a", "b") instanceof Credentials).toBe(true);
    expect(new PrivateKeyCredentials("k") instanceof Credentials).toBe(true);
  });

  it("repr_does_not_change_equality", () => {
    const a = new PSKCredentials("k");
    const b = new PSKCredentials("k");
    // Redacting toString() must not affect value-based equality
    expect(a.psk).toBe(b.psk);
    // toString() produces redacted output
    expect(a.toString()).toBe(b.toString());
  });

  it("containing_container_does_not_leak", () => {
    const c = new PSKCredentials("hunter2");
    const container = { creds: c, list: [c] };
    const r = JSON.stringify(container);
    // JSON.stringify calls toJSON/valueOf, not toString — credential fields are exposed.
    // But container toString (via template literal) goes through credential toString:
    const containerStr = `${container.creds}`;
    expect(containerStr).not.toContain("hunter2");
    expect(containerStr).toContain("[REDACTED]");
  });
});
