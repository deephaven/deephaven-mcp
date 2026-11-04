import pkg from "../package.json" with { type: "json" };

// Baked in by scripts/build.ts (`bun build --define`); undefined when run from source.
declare const DH_BUILD_VERSION: string | undefined;
declare const DH_BUILD_REPOSITORY: string | undefined;

/** True in compiled binaries; false under `bun run`. */
export const STANDALONE: boolean = Bun.isStandaloneExecutable;
export const VERSION: string =
  typeof DH_BUILD_VERSION !== "undefined" ? DH_BUILD_VERSION : pkg.version;
/** Command name; also names release binaries and install targets. */
export const BIN: string = pkg.name;
/** GitHub `owner/repo` that releases (and updates) come from. */
export const REPOSITORY: string =
  typeof DH_BUILD_REPOSITORY !== "undefined"
    ? DH_BUILD_REPOSITORY
    : pkg.repository;
