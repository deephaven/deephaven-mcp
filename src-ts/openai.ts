/**
 * Generic OpenAI/LLM client utilities for deephaven-mcp.
 *
 * This module provides a robust, production-ready OpenAIClient class for interacting with
 * OpenAI-compatible LLM APIs. The client is designed for high-volume usage scenarios and
 * includes comprehensive error handling, connection pooling, and resource management.
 *
 * Key Features:
 *   - Async context management via `Symbol.asyncDispose` for automatic resource cleanup
 *   - Configurable HTTP connection pooling to prevent resource exhaustion
 *   - Comprehensive timeout and retry configuration
 *   - Support for both streaming and non-streaming chat completions
 *   - Chat history validation and message formatting
 *   - System prompt support for conversation context
 *   - Detailed logging for debugging and monitoring
 *
 * @example
 * ```typescript
 * const client = new OpenAIClient({
 *   apiKey: "sk-...",
 *   baseUrl: "https://api.openai.com/v1",
 *   model: "gpt-3.5-turbo",
 * });
 * try {
 *   const response = await client.chat("Hello, world!");
 * } finally {
 *   await client.close();
 * }
 * ```
 */

import OpenAI from "openai";
import pino from "pino";

const _logger = pino({ name: "deephaven-mcp:openai" });

/**
 * Custom exception for OpenAIClient errors.
 *
 * This exception is raised when the OpenAIClient encounters errors during initialization,
 * API communication, or response processing.
 */
export class OpenAIClientError extends Error {
  constructor(message?: string) {
    super(message);
    this.name = "OpenAIClientError";
    Object.setPrototypeOf(this, new.target.prototype);
  }
}

/**
 * A message in the OpenAI chat format.
 */
export interface ChatMessage {
  role: string;
  content: string;
}

/**
 * Constructor options for OpenAIClient.
 */
export interface OpenAIClientOptions {
  /** The API key for authentication with the LLM service. */
  apiKey: string;
  /** The base URL of the OpenAI-compatible API endpoint. */
  baseUrl: string;
  /** The model name to use for chat completions. */
  model: string;
  /** Optionally inject a custom OpenAI client (for testing). */
  client?: OpenAILikeClient;
  /** Primary request timeout in seconds. Defaults to 60.0. */
  timeout?: number;
  /** Maximum number of automatic retries. Defaults to 2. */
  maxRetries?: number;
  /** Maximum total HTTP connections. Defaults to 10. */
  maxConnections?: number;
}

/**
 * Interface for an OpenAI-compatible client (for dependency injection in tests).
 */
export interface OpenAILikeClient {
  chat: {
    completions: {
      create(params: Record<string, unknown>): Promise<unknown>;
    };
  };
  close(): Promise<void>;
}

/**
 * Options for the `chat` and `streamChat` methods.
 */
export interface ChatOptions {
  /** Previous chat messages for context. */
  history?: ChatMessage[];
  /** System prompt strings to prepend as system messages. */
  systemPrompts?: string[];
  [key: string]: unknown;
}

/**
 * Asynchronous client for OpenAI-compatible chat APIs, supporting chat completion and streaming.
 *
 * This class provides a production-ready wrapper around the OpenAI SDK with enhanced
 * reliability, resource management, and error handling.
 *
 * @example
 * ```typescript
 * const client = new OpenAIClient({
 *   apiKey: "sk-...",
 *   baseUrl: "https://api.openai.com/v1",
 *   model: "gpt-3.5-turbo",
 * });
 * try {
 *   const response = await client.chat("Hello!");
 * } finally {
 *   await client.close();
 * }
 * ```
 */
export class OpenAIClient {
  /** The API key for authentication with the LLM service. */
  readonly apiKey: string;
  /** The base URL of the OpenAI-compatible API endpoint. */
  readonly baseUrl: string;
  /** The model name to use for chat completions. */
  readonly model: string;
  /** The underlying OpenAI-compatible client instance. */
  readonly client: OpenAILikeClient;

  /** Whether this instance owns the client and is responsible for cleanup. */
  readonly #clientOwned: boolean;

  /**
   * @param options - Configuration options for the client.
   * @throws {@link OpenAIClientError} If any required parameter (apiKey, baseUrl, model) is missing or invalid.
   */
  constructor(options: OpenAIClientOptions) {
    const {
      apiKey,
      baseUrl,
      model,
      client,
      timeout = 60.0,
      maxRetries = 2,
    } = options;

    if (!apiKey || typeof apiKey !== "string") {
      throw new OpenAIClientError("api_key must be a non-empty string.");
    }
    if (!baseUrl || typeof baseUrl !== "string") {
      throw new OpenAIClientError("base_url must be a non-empty string.");
    }
    if (!model || typeof model !== "string") {
      throw new OpenAIClientError("model must be a non-empty string.");
    }

    _logger.debug(
      `[OpenAIClient] Initializing client | model=${model}, base_url=${baseUrl}, timeout=${timeout}`
    );

    this.apiKey = apiKey;
    this.baseUrl = baseUrl;
    this.model = model;

    if (client === undefined) {
      // Production path: create a properly configured client
      this.client = new OpenAI({
        apiKey,
        baseURL: baseUrl,
        timeout: timeout * 1000,
        maxRetries,
      }) as unknown as OpenAILikeClient;
      this.#clientOwned = true;
      _logger.debug("[OpenAIClient] Created production client");
    } else {
      // Testing path: use injected client
      this.client = client;
      this.#clientOwned = false;
      _logger.debug("[OpenAIClient] Using injected client for testing");
    }
  }

  /** Whether this instance owns the client (for testing visibility). */
  get ["_clientOwned"](): boolean {
    return this.#clientOwned;
  }

  /**
   * Validate that the chat history is a sequence of dicts with 'role' and 'content' string keys.
   *
   * @param history - The chat history to validate.
   * @throws {@link OpenAIClientError} If history is not valid.
   */
  _validateHistory(history: ChatMessage[] | undefined): void {
    if (history === undefined || history === null) {
      return;
    }
    if (!Array.isArray(history)) {
      throw new OpenAIClientError("history must be a sequence (list or tuple) of dicts");
    }
    for (const msg of history) {
      if (typeof msg !== "object" || msg === null || Array.isArray(msg)) {
        throw new OpenAIClientError("Each message in history must be a dict");
      }
      if (!("role" in msg) || !("content" in msg)) {
        throw new OpenAIClientError(
          "Each message in history must have 'role' and 'content' keys"
        );
      }
      if (typeof (msg as Record<string, unknown>)["role"] !== "string" ||
          typeof (msg as Record<string, unknown>)["content"] !== "string") {
        throw new OpenAIClientError(
          "'role' and 'content' in each message must be strings"
        );
      }
    }
  }

  /**
   * Validate that the system prompts are a sequence of strings.
   *
   * @param systemPrompts - The system prompts to validate.
   * @throws {@link OpenAIClientError} If systemPrompts is not valid.
   */
  _validateSystemPrompts(systemPrompts: string[] | undefined): void {
    if (systemPrompts === undefined || systemPrompts === null) {
      return;
    }
    if (!Array.isArray(systemPrompts)) {
      throw new OpenAIClientError(
        "system_prompts must be a sequence (list or tuple) of strings"
      );
    }
    for (const prompt of systemPrompts) {
      if (typeof prompt !== "string") {
        throw new OpenAIClientError("Each system prompt must be a string");
      }
    }
  }

  /**
   * Construct the messages list for OpenAI chat completion requests.
   *
   * @param prompt - The latest user message to append to the conversation.
   * @param history - Previous chat messages for context.
   * @param systemPrompts - Optional sequence of system prompt strings.
   * @returns The formatted list of messages for the OpenAI API.
   */
  _buildMessages(
    prompt: string,
    history?: ChatMessage[],
    systemPrompts?: string[]
  ): ChatMessage[] {
    this._validateHistory(history);
    this._validateSystemPrompts(systemPrompts);
    const messages: ChatMessage[] = [];
    if (systemPrompts) {
      for (const sysMsg of systemPrompts) {
        messages.push({ role: "system", content: sysMsg });
      }
    }
    if (history) {
      messages.push(...history);
    }
    messages.push({ role: "user", content: prompt });
    return messages;
  }

  /**
   * Asynchronously send a chat completion request to the OpenAI API and return the assistant's response.
   *
   * @param prompt - The prompt to send to the model.
   * @param options - Optional chat options including history and system prompts.
   * @returns The assistant's response message content (stripped of whitespace).
   * @throws {@link OpenAIClientError} If the API call fails.
   *
   * @example
   * ```typescript
   * const response = await client.chat("Hello, who are you?");
   * ```
   */
  async chat(prompt: string, options?: ChatOptions): Promise<string> {
    const { history, systemPrompts, ...kwargs } = options ?? {};
    const messages = this._buildMessages(prompt, history, systemPrompts);
    try {
      _logger.info(
        `[OpenAIClient.chat] Sending chat completion request | model=${this.model}, base_url=${this.baseUrl}, prompt_len=${prompt.length}, history_len=${history?.length ?? 0}`
      );
      const startTime = Date.now();
      const response = await this.client.chat.completions.create({
        model: this.model,
        messages,
        ...kwargs,
      } as Record<string, unknown>);
      const elapsed = (Date.now() - startTime) / 1000;

      // Validate response structure
      const resp = response as Record<string, unknown>;
      if (
        !resp["choices"] ||
        !Array.isArray(resp["choices"]) ||
        resp["choices"].length === 0 ||
        !(resp["choices"][0] as Record<string, unknown>)["message"] ||
        !("content" in (((resp["choices"][0] as Record<string, unknown>)["message"]) as Record<string, unknown>))
      ) {
        _logger.error(`[OpenAIClient.chat] Unexpected response structure: ${JSON.stringify(response)}`);
        throw new OpenAIClientError("Unexpected response structure from OpenAI API");
      }

      _logger.info(`[OpenAIClient.chat] Chat completion succeeded | elapsed=${elapsed.toFixed(3)}s`);
      const content = (((resp["choices"][0] as Record<string, unknown>)["message"]) as Record<string, unknown>)["content"];
      if (content === null || content === undefined) {
        throw new OpenAIClientError("OpenAI API returned a null content message");
      }
      return String(content).trim();
    } catch (e) {
      if (e instanceof OpenAIClientError) {
        throw e;
      }
      // Check if it's an OpenAI SDK error
      const err = e as Error;
      if (err.name === "OpenAIError" || err.constructor?.name?.includes("OpenAI")) {
        _logger.error(`[OpenAIClient.chat] OpenAI API call failed: ${err}`);
        throw new OpenAIClientError(`OpenAI API call failed: ${err.message}`);
      }
      _logger.error(`[OpenAIClient.chat] Unexpected error: ${err}`);
      throw new OpenAIClientError(`Unexpected error: ${err.message}`);
    }
  }

  /**
   * Asynchronously send a streaming chat completion request, yielding tokens as they arrive.
   *
   * @param prompt - The user's question or message to send to the assistant.
   * @param options - Optional chat options including history and system prompts.
   * @yields The next chunk or token from the assistant's response.
   * @throws {@link OpenAIClientError} If the API call fails.
   *
   * @example
   * ```typescript
   * for await (const chunk of client.streamChat("Tell me a joke.")) {
   *   process.stdout.write(chunk);
   * }
   * ```
   */
  async *streamChat(prompt: string, options?: ChatOptions): AsyncGenerator<string> {
    const { history, systemPrompts, ...kwargs } = options ?? {};
    const messages = this._buildMessages(prompt, history, systemPrompts);
    try {
      _logger.info(
        `[OpenAIClient.stream_chat] Sending streaming chat request | model=${this.model}, base_url=${this.baseUrl}, prompt_len=${prompt.length}`
      );
      const startTime = Date.now();
      const response = await this.client.chat.completions.create({
        model: this.model,
        messages,
        stream: true,
        ...kwargs,
      } as Record<string, unknown>);

      // Check if response is async iterable
      if (typeof (response as AsyncIterable<unknown>)[Symbol.asyncIterator] !== "function") {
        _logger.error(`[OpenAIClient.stream_chat] Response is not async iterable: ${typeof response}`);
        throw new OpenAIClientError(
          "OpenAI API did not return an async iterable for streaming chat."
        );
      }

      let yielded = false;
      for await (const chunk of response as AsyncIterable<Record<string, unknown>>) {
        const choices = chunk["choices"] as Array<Record<string, unknown>> | undefined;
        const content = (choices?.[0]?.["delta"] as Record<string, unknown> | undefined)?.["content"];
        if (content) {
          yielded = true;
          yield String(content);
        }
      }

      const elapsed = (Date.now() - startTime) / 1000;
      if (!yielded) {
        _logger.warn(`[OpenAIClient.stream_chat] No content yielded in stream`);
      }
      _logger.info(`[OpenAIClient.stream_chat] Streaming chat completion finished | elapsed=${elapsed.toFixed(3)}s`);
    } catch (e) {
      if (e instanceof OpenAIClientError) {
        throw e;
      }
      const err = e as Error;
      if (err.name === "OpenAIError" || err.constructor?.name?.includes("OpenAI")) {
        _logger.error(`[OpenAIClient.stream_chat] OpenAI API streaming call failed: ${err}`);
        throw new OpenAIClientError(`OpenAI API streaming call failed: ${err.message}`);
      }
      _logger.error(`[OpenAIClient.stream_chat] Unexpected error: ${err}`);
      throw new OpenAIClientError(`Unexpected error: ${err.message}`);
    }
  }

  /**
   * Close the underlying OpenAI client and release HTTP connection resources.
   *
   * The method is designed to be safe and robust:
   * - Only closes resources for clients created by this instance (not injected test clients)
   * - Idempotent: safe to call multiple times without side effects
   * - Graceful error handling: logs cleanup errors without raising exceptions
   *
   * @example
   * ```typescript
   * const client = new OpenAIClient({ ... });
   * try {
   *   const response = await client.chat("Hello!");
   * } finally {
   *   await client.close();
   * }
   * ```
   */
  async close(): Promise<void> {
    if (this.#clientOwned) {
      try {
        await this.client.close();
        _logger.debug("[OpenAIClient.close] HTTP client connections closed");
      } catch (e) {
        _logger.warn(`[OpenAIClient.close] Error closing HTTP client: ${e}`);
      }
    } else {
      _logger.debug(
        "[OpenAIClient.close] Not closing HTTP client since it was not owned by this instance"
      );
    }
  }

  /**
   * Async dispose support for `Symbol.asyncDispose`.
   */
  async [Symbol.asyncDispose](): Promise<void> {
    await this.close();
  }
}
