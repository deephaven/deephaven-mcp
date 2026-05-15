/**
 * Deephaven client interface.
 *
 * Provides the main entry point for interacting with Deephaven servers. Exposes all major
 * client wrappers and utilities for both standard and enterprise (Core+) features. All
 * classes and utilities are re-exported from submodules for convenience.
 *
 * Features:
 *   - Async wrappers for sessions, queries, authentication, and controllers
 *   - Base wrapper classes with enhanced and asynchronous interfaces
 *   - Automatic detection of enterprise feature availability
 *   - All logging is handled in submodules; this file does not log
 *
 * @module client
 */

export { CorePlusAuthClient } from "./auth-client.js";
export { ClientObjectWrapper, isEnterpriseAvailable } from "./base.js";
export { CorePlusControllerClient } from "./controller-client.js";
export {
  PQ_STATES,
  CorePlusQueryConfig,
  CorePlusQueryInfo,
  CorePlusQuerySerial,
  CorePlusQueryState,
  CorePlusQueryStatus,
  CorePlusToken,
  ProtobufWrapper,
} from "./protobuf.js";
export { BaseSession, CoreSession, CorePlusSession } from "./session.js";
export { CorePlusSessionFactory } from "./session-factory.js";
