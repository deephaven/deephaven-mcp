let buffered: Promise<string> | undefined;

/** Read stdin once so a CA re-execution can replay input already consumed. */
export function readStdin(): Promise<string> {
  buffered ??= Bun.stdin.text();
  return buffered;
}

/** Return consumed stdin for a child process, or undefined if it is untouched. */
export async function stdinForRestart(): Promise<Uint8Array | undefined> {
  if (!buffered) return undefined;
  return new TextEncoder().encode(await buffered);
}
