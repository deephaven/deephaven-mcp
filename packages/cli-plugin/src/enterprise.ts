/**
 * `@deephaven/cli-plugin/enterprise`: `definePlugin` with `ctx.enterprise()`
 * typed by `@deephaven-enterprise/jsapi-types`.
 */
import type {
  EnterpriseClient,
  EnterpriseDhType,
} from "@deephaven-enterprise/jsapi-types";
import {
  type DefinedPlugin,
  defineWith,
  type PluginContextOf,
  type PluginDefinitionOf,
} from "./define.js";

export {
  API_VERSION,
  type ArgDef,
  type CommandDef,
  type CoreClient,
  type DhApi,
  type FailCode,
  type FieldDef,
  type GroupDef,
  type LeafDef,
  type OptionDef,
  type OutputDef,
  type Requires,
} from "./define.js";
export type { EnterpriseClient, EnterpriseDhType };

/** The context, with `enterprise()` typed. */
export type PluginContext = PluginContextOf<{
  /** The `iris` global set by the server's `irisapi.nocache.js`. */
  api: EnterpriseDhType;
  client: EnterpriseClient;
}>;

export type PluginDefinition = PluginDefinitionOf<PluginContext>;

/** Declares a plugin; `export default` it from the plugin's module. */
export function definePlugin(
  plugin: PluginDefinition,
): DefinedPlugin<PluginContext> {
  return defineWith(plugin);
}
