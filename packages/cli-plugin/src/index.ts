/**
 * `@deephaven/cli-plugin`: define a TypeScript plugin for `dh`. Typed for
 * Community; for Enterprise's JS API types, import
 * `@deephaven/cli-plugin/enterprise` instead.
 */
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

/** The context, with `enterprise()` untyped. */
export type PluginContext = PluginContextOf<{
  api: unknown;
  client: unknown;
}>;

export type PluginDefinition = PluginDefinitionOf<PluginContext>;

/** Declares a plugin; `export default` it from the plugin's module. */
export function definePlugin(
  plugin: PluginDefinition,
): DefinedPlugin<PluginContext> {
  return defineWith(plugin);
}
