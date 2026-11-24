import type { dh } from "@deephaven/jsapi-types";
import {
  createJsApiFactories,
  EnterpriseCorePlusManager,
  getCoreConnection,
} from "@deephaven-enterprise/jsapi-nodejs";
import type { QueryInfo } from "@deephaven-enterprise/jsapi-types";
import type { CommandLifecycle } from "../../cli/lifecycle.ts";
import { asDhError, cleanupError, DhError, timeoutMs } from "../../errors.ts";
import { loadCorePlusApi } from "../auth/jsapi.ts";
import type { Session } from "../auth/signin.ts";

/** Connect a CLI-owned Core+ worker using the Enterprise library's manager. */
export async function connectCorePlusWorker(
  session: Session,
  query: QueryInfo,
  lifecycle: CommandLifecycle,
  signal: AbortSignal,
): Promise<dh.IdeConnection> {
  if (!session.enterprise) {
    throw new DhError(
      "console_unsupported",
      "The server has no Enterprise connection",
    );
  }
  const { client, dhe } = session.enterprise;
  const { workerKinds } = await client.getServerConfigValues();
  signal.throwIfAborted();
  const factories = createJsApiFactories();
  const manager = new EnterpriseCorePlusManager(
    dhe,
    client,
    workerKinds,
    // The vendor loader does not validate redirect destinations or use DH_TIMEOUT.
    (url) => loadCorePlusApi(url, session.cacheRoot, timeoutMs()),
    (...args) => {
      // The manager can finish fetching a worker token after it is disposed.
      signal.throwIfAborted();
      return factories.createCoreClient(...args);
    },
  );
  try {
    signal.throwIfAborted();
    lifecycle.onCleanup(() => manager.dispose());
  } catch (error) {
    try {
      await manager.dispose();
    } catch (cleanup) {
      throw cleanupError([cleanup], asDhError(error));
    }
    throw error;
  }
  return await getCoreConnection({ query, corePlusManager: manager });
}
