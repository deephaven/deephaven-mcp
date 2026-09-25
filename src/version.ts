import config from "../deno.json" with { type: "json" };

export const VERSION: string = config.version;
