import config from "../deno.json" with { type: "json" };

export const VERSION: string = config.version;
/** GitHub `owner/repo` that releases (and updates) come from. */
export const REPOSITORY: string = config.repository;
