/** One-time SAML key: 96 CSPRNG bytes, base64. */
export function samlNonce(): string {
  return Buffer.from(crypto.getRandomValues(new Uint8Array(96))).toString(
    "base64",
  );
}

/** The SSO login (or confirm) page `base`, carrying `nonce` as `?key=`. */
export function signInUrl(base: string, nonce: string): string {
  const url = new URL(base);
  url.searchParams.set("key", nonce);
  return url.href;
}

/** Best effort; the URL is always printed too. */
export async function openBrowser(url: string): Promise<boolean> {
  const [cmd, ...args] =
    process.platform === "darwin"
      ? ["open", url]
      : // Not `cmd /c start`: cmd would expand the %XX escapes in the URL.
        process.platform === "win32"
        ? ["rundll32", "url.dll,FileProtocolHandler", url]
        : ["xdg-open", url];
  try {
    const proc = Bun.spawn([cmd, ...args], {
      stdin: "ignore",
      stdout: "ignore",
      stderr: "ignore",
    });
    return (await proc.exited) === 0;
  } catch {
    return false;
  }
}
