import { Option } from "commander";
import { findLegacy, type LegacyItem } from "../../auth/legacy.ts";
import {
  type Migrated,
  migrateLegacyEntry,
  recordMigration,
  splitMigrated,
} from "../../auth/migrate.ts";
import type { State } from "../../auth/store.ts";
import { type Context, defineCommand, type Output } from "../../cli/command.ts";
import { timeoutMs } from "../../errors.ts";
import { note } from "../../output.ts";
import { BIN } from "../../version.ts";
import { askLegacyEntriesToMigrate } from "./questions.ts";

/** `dh auth migrate`'s flags; login passes `yes` from its own `--yes`. */
export interface MigrateOptions {
  from?: string;
  yes?: boolean;
}

export const migrate = defineCommand<MigrateOptions>({
  name: "migrate",
  description: "Migrate profiles from a legacy dhcli / Deephaven MCP config.",
  options: [
    new Option("--from <path>", "Legacy config file or directory."),
    new Option("-y, --yes", "Migrate everything without asking."),
  ],

  async run(ctx, _args, opts) {
    await ctx.store.ensureDir();
    const result = await migrateLegacy(ctx, opts, false);
    if (result === undefined) {
      return {
        human: "No legacy config found.",
        json: { migrated: 0, failed: 0 },
      };
    }
    return migratedOutput(result);
  },
});

/** How many legacy entries were migrated, and how many were tried and failed. */
export interface MigrationResult {
  migrated: number;
  failed: number;
}

/** Login offers to migrate until the user has profiles or has answered once. */
export function shouldOfferMigration(state: State): boolean {
  return (
    Object.keys(state.config.profiles).length === 0 &&
    !state.config.legacyImport
  );
}

/**
 * The migration itself, shared with login's first-run offer (`offered`).
 * Undefined if there was no config, or the offer was skipped without a terminal.
 */
export async function migrateLegacy(
  ctx: Context,
  opts: MigrateOptions,
  offered: boolean,
): Promise<MigrationResult | undefined> {
  const legacy = await findLegacy(opts.from);
  if (!legacy || legacy.items.length === 0) return undefined;
  if (offered && !opts.yes && !ctx.ui.interactive) return undefined;
  note(`Found a dhcli config at ${legacy.path}.`);

  const { config } = await ctx.store.read();
  const { done, pending } = splitMigrated(legacy, config);
  for (const item of done) note(`  – ${item.name}: already migrated`);
  if (pending.length === 0) return { migrated: 0, failed: 0 };
  note(
    "Migrating signs in to each server to check its credentials, so the servers must be reachable now (for example, on VPN).",
  );
  const chosen = await askLegacyEntriesToMigrate(ctx.ui, pending, opts.yes);
  for (const n of legacy.notImported) note(`  – ${n}: not used, not migrated`);

  const outcomes: Migrated[] = [];
  for (const item of chosen) {
    outcomes.push(await migrateAndReport(ctx, legacy.path, item));
  }
  const failed = outcomes.filter((o) => !o.ok).length;
  if (failed) {
    note(
      `${failed} not migrated. Fix the problems above (for example, connect to the servers' network), then run \`${BIN} auth migrate\` to retry. Migrated servers are skipped.`,
    );
  }

  await recordMigration(ctx.store, legacy, chosen, outcomes);
  return { migrated: outcomes.length - failed, failed };
}

/** Migrates one entry, printing what it waits on and how it went. */
async function migrateAndReport(
  ctx: Context,
  legacyPath: string,
  item: LegacyItem,
): Promise<Migrated> {
  const seconds = timeoutMs() / 1000;
  const outcome = await migrateLegacyEntry(
    ctx.store,
    legacyPath,
    item,
    (what) =>
      note(
        `  … ${item.name}: ${what} (gives up after ${seconds}s if unreachable)`,
      ),
  );
  note(
    outcome.ok
      ? `  ✔ ${item.name} → ${outcome.profile}  ${outcome.detail}`
      : `  ! ${item.name}: ${outcome.detail}`,
  );
  for (const warning of outcome.warnings) note(`    ${warning}`);
  return outcome;
}

/** "Migrated N profiles.", shared with login's first-run offer. */
export function migratedOutput({ migrated, failed }: MigrationResult): Output {
  const counted = `Migrated ${migrated} profile${migrated === 1 ? "" : "s"}`;
  return {
    human: failed ? `${counted}; ${failed} failed.` : `${counted}.`,
    json: { migrated, failed },
  };
}
