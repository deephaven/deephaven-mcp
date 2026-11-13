/** Passing a child's stderr through as it arrives, keeping its end, and holding back `dh`'s own marker lines. */
import { errBytes } from "../../output.ts";

/** How much of a run's stderr is kept for classifying a failure. */
const STDERR_TAIL = 16 * 1024;

/**
 * Writes each chunk of `stream` to `dh`'s stderr as it arrives; returns the
 * last {@link STDERR_TAIL} bytes as text. With `marker`, a line starting with
 * it is held back and returned (its rest) instead; so a partial line that
 * might still be one waits for more bytes.
 */
export async function passStderr(
  stream: ReadableStream<Uint8Array>,
  marker?: string,
): Promise<{ tail: string; markers: string[] }> {
  let tail = new Uint8Array(0);
  const keep = (bytes: Uint8Array) => {
    const joined = new Uint8Array(tail.length + bytes.length);
    joined.set(tail);
    joined.set(bytes, tail.length);
    tail = joined.slice(Math.max(0, joined.length - STDERR_TAIL));
  };
  const filter = marker === undefined ? undefined : new MarkerFilter(marker);
  for await (const chunk of stream) {
    const passed = filter ? filter.push(chunk) : chunk;
    if (passed.length > 0) errBytes(passed);
    keep(passed);
  }
  const rest = filter?.end() ?? new Uint8Array(0);
  if (rest.length > 0) errBytes(rest);
  keep(rest);
  return { tail: new TextDecoder().decode(tail), markers: filter?.found ?? [] };
}

/**
 * Splits stderr into what passes through and a line starting with `marker`.
 * Only the start of a line that could still become the marker is held.
 */
class MarkerFilter {
  readonly #marker: Uint8Array;
  /** The current line's bytes, held while it might be the marker. */
  #held: Uint8Array = new Uint8Array(0);
  /** The current line is known not to be the marker, so its bytes pass at once. */
  #passing = false;
  /** The current line is the marker: collect it. */
  #collecting = false;
  /** Each marker line's rest, in order. */
  found: string[] = [];

  constructor(marker: string) {
    this.#marker = new TextEncoder().encode(marker);
  }

  /** The bytes of `chunk` that can be written now. */
  push(chunk: Uint8Array): Uint8Array {
    const out: number[] = [];
    for (const byte of chunk) {
      if (this.#passing) {
        out.push(byte);
        if (byte === 0x0a) this.#passing = false;
        continue;
      }
      if (byte === 0x0a) {
        this.#endLine(out, true);
        continue;
      }
      this.#held = append(this.#held, byte);
      if (this.#collecting) continue;
      if (this.#held.length <= this.#marker.length) {
        const at = this.#held.length - 1;
        if (this.#held[at] !== this.#marker[at]) {
          out.push(...this.#held);
          this.#held = new Uint8Array(0);
          this.#passing = true;
        } else if (this.#held.length === this.#marker.length) {
          this.#collecting = true;
        }
      }
    }
    return Uint8Array.from(out);
  }

  /** What's still held at the end of the stream. */
  end(): Uint8Array {
    const out: number[] = [];
    this.#endLine(out, false);
    return Uint8Array.from(out);
  }

  #endLine(out: number[], newline: boolean): void {
    if (this.#collecting) {
      const text = new TextDecoder().decode(this.#held);
      this.found.push(
        text.slice(new TextDecoder().decode(this.#marker).length),
      );
    } else {
      out.push(...this.#held);
      if (newline) out.push(0x0a);
    }
    this.#held = new Uint8Array(0);
    this.#collecting = false;
    this.#passing = false;
  }
}

function append(bytes: Uint8Array, byte: number): Uint8Array {
  const next = new Uint8Array(bytes.length + 1);
  next.set(bytes);
  next[bytes.length] = byte;
  return next;
}
