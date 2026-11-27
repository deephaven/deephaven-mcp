/**
 * Release channel rules shared by the CLI and the release scripts: channel
 * names, `cli-v*` tags, and how CI numbers the next build. See
 * docs/RELEASE_ARCHITECTURE.md.
 */
import { compare, inc } from "semver";

/** The default channel; its versions have no prerelease suffix. */
export const RELEASE_CHANNEL = "release";
/** Builds run from source or built locally; never published or followed. */
export const DEV_CHANNEL = "dev";

const CHANNEL_NAME = /^[a-z][a-z0-9-]*$/;
const MAX_CHANNEL_LENGTH = 40;
const RELEASE_TAG = /^cli-v(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)$/;
// Greedy channel group, so the build is after the last dot: alpha-1.0 is alpha-1, 0.
const PRERELEASE_TAG =
  /^cli-v((?:0|[1-9]\d*)\.(?:0|[1-9]\d*)\.(?:0|[1-9]\d*))-([a-z][a-z0-9-]*)\.(0|[1-9]\d*)$/;

/** Why `name` can't be published or followed, or undefined if it can. */
export function channelError(name: string): string | undefined {
  if (name === DEV_CHANNEL) {
    return `"${DEV_CHANNEL}" is reserved for builds run from source`;
  }
  if (name.length > MAX_CHANNEL_LENGTH || !CHANNEL_NAME.test(name)) {
    return `Invalid channel "${name}": use lowercase letters, digits and hyphens, starting with a letter, at most ${MAX_CHANNEL_LENGTH} characters`;
  }
  return undefined;
}

/** True if `name` is a channel that can be published and followed. */
export const isChannel = (name: string): boolean => !channelError(name);

/** A parsed `cli-v*` tag. */
export interface CliTag {
  tag: string;
  /** The tag without `cli-v`; valid semver. */
  version: string;
  /** `X.Y.Z` without any prerelease part. */
  base: string;
  channel: string;
  /** The prerelease build number; undefined on the release channel. */
  build?: number;
}

/** Parses a `cli-vX.Y.Z` or `cli-vX.Y.Z-<channel>.<build>` tag; anything else is undefined. */
export function parseTag(tag: string): CliTag | undefined {
  if (RELEASE_TAG.test(tag)) {
    const version = tag.slice("cli-v".length);
    return { tag, version, base: version, channel: RELEASE_CHANNEL };
  }
  const pre = PRERELEASE_TAG.exec(tag);
  if (!pre) return undefined;
  const [, base, channel, build] = pre as unknown as [
    string,
    string,
    string,
    string,
  ];
  if (!isChannel(channel) || channel === RELEASE_CHANNEL) {
    return undefined;
  }
  return {
    tag,
    version: tag.slice("cli-v".length),
    base,
    channel,
    build: Number(build),
  };
}

/** The version for `base` on `channel`: `base`, or `base-<channel>.<build>`. */
export function formatVersion(
  base: string,
  channel: string,
  build: number,
): string {
  return channel === RELEASE_CHANNEL ? base : `${base}-${channel}.${build}`;
}

const parseAll = (tags: Iterable<string>): CliTag[] =>
  [...tags].flatMap((tag) => parseTag(tag) ?? []);

const highest = (tags: CliTag[]): CliTag | undefined =>
  tags.sort((a, b) => compare(b.version, a.version))[0];

/**
 * The version CI publishes next on `channel`, given every tag in the repo and
 * `package.json`'s version. Throws on an invalid channel or version.
 */
export function nextVersion(
  tags: Iterable<string>,
  pkgVersion: string,
  channel: string,
): string {
  const invalid = channelError(channel);
  if (invalid) throw new Error(invalid);
  if (!RELEASE_TAG.test(`cli-v${pkgVersion}`)) {
    throw new Error(
      `package.json version must be plain X.Y.Z, not "${pkgVersion}"`,
    );
  }
  const parsed = parseAll(tags);
  const last =
    highest(parsed.filter((t) => t.channel === RELEASE_CHANNEL))?.version ??
    "0.0.0";
  const bumped = inc(last, "patch") as string;
  const base = compare(bumped, pkgVersion) >= 0 ? bumped : pkgVersion;

  const builds = parsed
    .filter((t) => t.channel === channel && t.base === base)
    .map((t) => t.build ?? 0);
  const build = builds.length ? Math.max(...builds) + 1 : 0;
  return formatVersion(base, channel, build);
}

/**
 * The tag release notes for `tag` start from: for a release, the highest
 * release below it; for a prerelease, the highest tag of its channel below it,
 * else the highest release below its `X.Y.Z`. Undefined if there is none.
 */
export function notesStartTag(
  tags: Iterable<string>,
  tag: string,
): string | undefined {
  const current = parseTag(tag);
  if (!current) throw new Error(`Not a CLI tag: ${tag}`);
  const parsed = parseAll(tags);
  const below = (t: CliTag) => compare(t.version, current.version) < 0;
  const releasesBelow = (version: string) =>
    parsed.filter(
      (t) => t.channel === RELEASE_CHANNEL && compare(t.version, version) < 0,
    );

  if (current.channel === RELEASE_CHANNEL) {
    return highest(releasesBelow(current.version))?.tag;
  }
  const sameChannel = parsed.filter(
    (t) => t.channel === current.channel && below(t),
  );
  return (highest(sameChannel) ?? highest(releasesBelow(current.base)))?.tag;
}
