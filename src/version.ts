import config from "../deno.json" with { type: "json" };

export const VERSION: string = config.version;
/** Command name; also names release binaries and install targets. */
export const BIN: string = config.bin;
/** GitHub `owner/repo` that releases (and updates) come from. */
export const REPOSITORY: string = config.repository;
