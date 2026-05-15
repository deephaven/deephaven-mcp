/**
 * Tests for OpenAI client module.
 */
import { describe, it, expect, vi, afterEach } from "vitest";
import { OpenAIClient, OpenAIClientError } from "./openai.js";

afterEach(() => {
  vi.restoreAllMocks();
});

// Dummy OpenAI-compatible client for testing
function makeDummyClient(opts: {
  shouldFail?: boolean;
  responseContent?: string;
  streamContent?: string[];
} = {}) {
  const { shouldFail = false, responseContent = "Hello, world!", streamContent = ["Hello,", " world!"] } = opts;

  return {
    chat: {
      completions: {
        create: vi.fn().mockImplementation(async (params: { stream?: boolean }) => {
          if (params.stream) {
            // Return async iterable
            if (shouldFail) {
              throw Object.assign(new Error("Simulated OpenAI error"), { name: "OpenAIError" });
            }
            const chunks = streamContent.map((c) => ({
              choices: [{ delta: { content: c } }],
            }));
            return (async function* () {
              for (const chunk of chunks) {
                yield chunk;
              }
            })();
          }
          // Non-streaming
          if (shouldFail) {
            throw Object.assign(new Error("Simulated OpenAI error"), { name: "OpenAIError" });
          }
          return {
            choices: [{ message: { content: responseContent } }],
          };
        }),
      },
    },
    close: vi.fn().mockResolvedValue(undefined),
  };
}

describe("OpenAIClient", () => {
  it("chat_success", async () => {
    const dummy = makeDummyClient();
    const client = new OpenAIClient({
      apiKey: "test-key",
      baseUrl: "https://api.test.com/v1",
      model: "gpt-test",
      client: dummy as unknown as Parameters<typeof OpenAIClient.prototype["chat"]>[0] extends never ? never : never,
    } as never);
    const result = await client.chat("hello", { history: [{ role: "user", content: "hi" }] });
    expect(result).toBe("Hello, world!");
  });

  it("chat_failure", async () => {
    const dummy = makeDummyClient({ shouldFail: true });
    const client = new OpenAIClient({
      apiKey: "test-key",
      baseUrl: "https://api.test.com/v1",
      model: "gpt-test",
      client: dummy as never,
    } as never);
    await expect(client.chat("fail")).rejects.toThrow(OpenAIClientError);
  });

  it("stream_chat_success", async () => {
    const dummy = makeDummyClient();
    const client = new OpenAIClient({
      apiKey: "test-key",
      baseUrl: "https://api.test.com/v1",
      model: "gpt-test",
      client: dummy as never,
    } as never);
    const result: string[] = [];
    for await (const token of client.streamChat("hello")) {
      result.push(token);
    }
    expect(result).toEqual(["Hello,", " world!"]);
  });

  it("stream_chat_failure", async () => {
    const dummy = makeDummyClient({ shouldFail: true });
    const client = new OpenAIClient({
      apiKey: "test-key",
      baseUrl: "https://api.test.com/v1",
      model: "gpt-test",
      client: dummy as never,
    } as never);
    await expect(async () => {
      for await (const _ of client.streamChat("fail")) {
        // consume
      }
    }).rejects.toThrow(OpenAIClientError);
  });

  it("build_messages_and_validate_history", () => {
    const client = new OpenAIClient({
      apiKey: "test-key",
      baseUrl: "https://api.test.com/v1",
      model: "gpt-test",
      client: makeDummyClient() as never,
    } as never);
    const prompt = "What's up?";
    const history = [{ role: "user", content: "Hi" }];
    const messages = client._buildMessages(prompt, history);
    expect(messages[messages.length - 1].content).toBe(prompt);
    expect(messages[0].role).toBe("user");

    // With system prompts
    const sysPrompts = ["You are a bot.", "Be concise."];
    const messages2 = client._buildMessages(prompt, history, sysPrompts);
    expect(messages2[0].role).toBe("system");
    expect(messages2[0].content).toBe("You are a bot.");
    expect(messages2[1].role).toBe("system");
    expect(messages2[1].content).toBe("Be concise.");
    expect(messages2[2].role).toBe("user");
    expect(messages2[2].content).toBe("Hi");
    expect(messages2[messages2.length - 1].role).toBe("user");
    expect(messages2[messages2.length - 1].content).toBe(prompt);

    // Invalid history - missing content key
    expect(() => client._validateHistory([{ role: "user" } as never])).toThrow(OpenAIClientError);
    // Invalid history - non-dict
    expect(() => client._validateHistory([123 as never])).toThrow(OpenAIClientError);
    // Accepts None and empty history
    expect(() => client._validateHistory(undefined)).not.toThrow();
    expect(() => client._validateHistory([])).not.toThrow();
    // Non-array
    expect(() => client._validateHistory("notalist" as never)).toThrow(OpenAIClientError);
    // Non-string values
    expect(() => client._validateHistory([{ role: "user", content: 123 } as never])).toThrow(OpenAIClientError);
  });

  it("validate_system_prompts", () => {
    const client = new OpenAIClient({
      apiKey: "x",
      baseUrl: "y",
      model: "z",
    });
    expect(() => client._validateSystemPrompts(undefined)).not.toThrow();
    expect(() => client._validateSystemPrompts([])).not.toThrow();
    expect(() => client._validateSystemPrompts(["a", "b"])).not.toThrow();
    expect(() => client._validateSystemPrompts("notalist" as never)).toThrow(OpenAIClientError);
    expect(() => client._validateSystemPrompts(123 as never)).toThrow(OpenAIClientError);
    expect(() => client._validateSystemPrompts([123 as never])).toThrow(OpenAIClientError);
    expect(() => client._validateSystemPrompts(["ok", null as never])).toThrow(OpenAIClientError);
  });

  it("chat_invalid_response_structure", async () => {
    const dummy = {
      chat: {
        completions: {
          create: vi.fn().mockResolvedValue({}),
        },
      },
      close: vi.fn(),
    };
    const client = new OpenAIClient({ apiKey: "x", baseUrl: "y", model: "z", client: dummy as never } as never);
    await expect(client.chat("prompt")).rejects.toThrow(/Unexpected response structure/);
  });

  it("chat_null_content", async () => {
    const dummy = {
      chat: {
        completions: {
          create: vi.fn().mockResolvedValue({
            choices: [{ message: { content: null } }],
          }),
        },
      },
      close: vi.fn(),
    };
    const client = new OpenAIClient({ apiKey: "x", baseUrl: "y", model: "z", client: dummy as never } as never);
    await expect(client.chat("prompt")).rejects.toThrow(/null content/);
  });

  it("constructor_validation", () => {
    expect(() => new OpenAIClient({ apiKey: "", baseUrl: "x", model: "y" })).toThrow(OpenAIClientError);
    expect(() => new OpenAIClient({ apiKey: "x", baseUrl: "", model: "y" })).toThrow(OpenAIClientError);
    expect(() => new OpenAIClient({ apiKey: "x", baseUrl: "y", model: "" })).toThrow(OpenAIClientError);
    expect(() => new OpenAIClient({ apiKey: 123 as never, baseUrl: "y", model: "z" })).toThrow(OpenAIClientError);
  });

  it("constructor_with_injected_client", () => {
    const injectedClient = makeDummyClient();
    const client = new OpenAIClient({
      apiKey: "test-key",
      baseUrl: "https://api.test.com",
      model: "test-model",
      client: injectedClient as never,
    } as never);
    expect(client["_clientOwned"]).toBe(false);
  });

  it("close_method_injected_client", async () => {
    const mockClient = makeDummyClient();
    const client = new OpenAIClient({
      apiKey: "test-key",
      baseUrl: "https://api.test.com",
      model: "test-model",
      client: mockClient as never,
    } as never);
    expect(client["_clientOwned"]).toBe(false);
    await client.close();
    expect(mockClient.close).not.toHaveBeenCalled();
  });

  it("stream_chat_no_content", async () => {
    const dummy = {
      chat: {
        completions: {
          create: vi.fn().mockResolvedValue(
            (async function* () {
              yield { choices: [{ delta: { content: null } }] };
              yield { choices: [{ delta: { content: null } }] };
            })()
          ),
        },
      },
      close: vi.fn(),
    };
    const client = new OpenAIClient({ apiKey: "x", baseUrl: "y", model: "z", client: dummy as never } as never);
    const tokens: string[] = [];
    for await (const token of client.streamChat("test")) {
      tokens.push(token);
    }
    expect(tokens).toEqual([]);
  });

  it("async_context_manager_success", async () => {
    const dummy = makeDummyClient();
    const client = new OpenAIClient({
      apiKey: "test-key",
      baseUrl: "https://api.openai.com/v1",
      model: "gpt-3.5-turbo",
      client: dummy as never,
    } as never);
    let contextClient: OpenAIClient | undefined;
    await client[Symbol.asyncDispose]?.();
    // Use manual open/close pattern since TypeScript uses Symbol.asyncDispose
    const result = await client.chat("Test prompt");
    expect(result).toBe("Hello, world!");
    await client.close();
  });
});
