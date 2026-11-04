const LOCAL_HOSTS = new Set(["localhost", "127.0.0.1", "[::1]"]);

/** Whether `url` points at this machine. */
export function isLocal(url: URL): boolean {
  return LOCAL_HOSTS.has(url.hostname);
}

/** Whether code may be downloaded from `url`: https, or http to this machine. */
export function isTrustedSource(url: URL): boolean {
  return (
    url.protocol === "https:" || (url.protocol === "http:" && isLocal(url))
  );
}
