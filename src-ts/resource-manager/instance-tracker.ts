/**
 * Instance tracking and orphaned resource cleanup for MCP server instances.
 *
 * This module provides functionality to track running MCP server instances and clean up
 * orphaned Docker containers and python processes that may be left behind when a server
 * is terminated with SIGKILL or crashes unexpectedly.
 *
 * Key Concepts:
 *   - Each MCP server instance is assigned a unique UUID on startup
 *   - Instance metadata (UUID, PID, start time, python processes) is persisted to disk
 *   - Docker containers are labeled with the instance UUID for identification
 *   - Python processes are tracked in the instance metadata file
 *   - On startup, dead instances are detected and their orphaned resources are cleaned up
 *
 * Architecture:
 *   - Instance metadata stored in: ~/.deephaven-mcp/instances/{uuid}.json
 *   - Docker containers labeled with: deephaven-mcp-server-instance={uuid}
 *   - Python processes tracked in instance file: {"python_processes": {"session": pid}}
 *
 * Usage:
 * ```typescript
 * // On server startup
 * const instance = await InstanceTracker.createAndRegister();
 * await cleanupOrphanedResources();
 *
 * // During operation
 * await instance.trackPythonProcess("my-session", 12345);
 * await instance.untrackPythonProcess("my-session");
 *
 * // On server shutdown
 * await instance.unregister();
 * ```
 *
 * Thread Safety:
 *   - All operations are async-safe
 *   - File operations use atomic writes where possible
 *   - Multiple server instances can safely coexist
 */

import * as fs from "node:fs/promises";
import * as path from "node:path";
import * as os from "node:os";
import { randomUUID } from "node:crypto";
import { spawn } from "node:child_process";
import pino from "pino";

const _logger = pino({ name: "deephaven-mcp:resource-manager/instance-tracker" });

// ---------------------------------------------------------------------------
// InstanceTracker
// ---------------------------------------------------------------------------

/**
 * Tracks a single MCP server instance and its associated resources.
 *
 * This class manages the lifecycle of an MCP server instance, including:
 * - Generating and persisting a unique instance identifier
 * - Tracking python-launched session processes
 * - Registering/unregistering the instance on startup/shutdown
 * - Providing the instance ID for Docker container labeling
 */
export class InstanceTracker {
  /** Unique UUID for this server instance. */
  readonly instanceId: string;
  /** Process ID of this server instance. */
  readonly pid: number;
  /** ISO 8601 timestamp of when this instance started. */
  readonly startedAt: string;
  /** Path to the instance metadata file. */
  readonly instanceFile: string;

  /** @internal Map from session name to PID */
  _pythonProcesses: Record<string, number>;

  /**
   * Initialize an InstanceTracker.
   *
   * This constructor should not be called directly. Use createAndRegister()
   * or loadFromFile() factory methods instead.
   *
   * @param instanceId - Unique UUID for this instance.
   * @param pid - Process ID of this server instance.
   * @param startedAt - ISO 8601 timestamp of when the instance started.
   */
  constructor(instanceId: string, pid: number, startedAt: string) {
    this.instanceId = instanceId;
    this.pid = pid;
    this.startedAt = startedAt;
    this._pythonProcesses = {};

    const instancesDir = path.join(os.homedir(), ".deephaven-mcp", "instances");
    this.instanceFile = path.join(instancesDir, `${instanceId}.json`);
  }

  /**
   * Create a new instance tracker and register it.
   *
   * This factory method creates a new instance with a unique UUID and immediately
   * persists it to disk. Call this on MCP server startup.
   *
   * @returns A new registered instance tracker.
   *
   * @example
   * ```typescript
   * const instance = await InstanceTracker.createAndRegister();
   * ```
   */
  static async createAndRegister(): Promise<InstanceTracker> {
    const instanceId = randomUUID();
    const pid = process.pid;
    const startedAt = new Date().toISOString();

    // Ensure instances directory exists
    const instancesDir = path.join(os.homedir(), ".deephaven-mcp", "instances");
    await fs.mkdir(instancesDir, { recursive: true });

    const tracker = new InstanceTracker(instanceId, pid, startedAt);
    await tracker._save();

    _logger.info(`[InstanceTracker] Registered new instance ${instanceId} (PID: ${pid})`);

    return tracker;
  }

  /**
   * Load an existing instance tracker from a metadata file.
   *
   * This is used during orphan cleanup to load information about other
   * (potentially dead) server instances.
   *
   * @param instanceFile - Path to the instance metadata JSON file.
   * @returns Instance tracker loaded from the file.
   * @throws If the instance file does not exist, contains invalid JSON, or is missing required fields.
   */
  static loadFromFile(instanceFile: string): InstanceTracker {
    // Note: this is intentionally synchronous to match the Python version
    const raw = require("node:fs").readFileSync(instanceFile, "utf-8");
    const data = JSON.parse(raw) as {
      instance_id: string;
      pid: number;
      started_at: string;
      python_processes?: Record<string, number>;
    };

    const tracker = new InstanceTracker(data["instance_id"], data["pid"], data["started_at"]);
    tracker._pythonProcesses = data["python_processes"] ?? {};

    return tracker;
  }

  /**
   * Track a new python-launched session process.
   *
   * Adds the process to the instance metadata so it can be cleaned up
   * if the server crashes or is killed.
   *
   * @param sessionName - Name of the session.
   * @param pid - Process ID of the python-launched deephaven-server process.
   *
   * @example
   * ```typescript
   * await instance.trackPythonProcess("my-session", 12345);
   * ```
   */
  async trackPythonProcess(sessionName: string, pid: number): Promise<void> {
    this._pythonProcesses[sessionName] = pid;
    await this._save();

    _logger.debug(
      `[InstanceTracker] Tracking python process for session '${sessionName}' (PID: ${pid})`,
    );
  }

  /**
   * Stop tracking a python-launched session process.
   *
   * Removes the process from the instance metadata, typically called when
   * the session is stopped normally. If the session name is not currently
   * tracked, this method is a no-op.
   *
   * @param sessionName - Name of the session to stop tracking.
   *
   * @example
   * ```typescript
   * await instance.untrackPythonProcess("my-session");
   * ```
   */
  async untrackPythonProcess(sessionName: string): Promise<void> {
    if (sessionName in this._pythonProcesses) {
      delete this._pythonProcesses[sessionName];
      await this._save();

      _logger.debug(`[InstanceTracker] Stopped tracking python process for session '${sessionName}'`);
    }
  }

  /**
   * Unregister this instance and remove its metadata file.
   *
   * Call this on normal server shutdown to clean up instance tracking.
   * This prevents the cleanup logic from attempting to clean up resources
   * for a server that shut down normally.
   *
   * @example
   * ```typescript
   * // In lifespan finally block
   * await instance.unregister();
   * ```
   */
  async unregister(): Promise<void> {
    try {
      await fs.unlink(this.instanceFile);
      _logger.info(`[InstanceTracker] Unregistered instance ${this.instanceId}`);
    } catch (e: unknown) {
      const err = e as NodeJS.ErrnoException;
      if (err.code !== "ENOENT") {
        _logger.warn(`[InstanceTracker] Error unregistering instance ${this.instanceId}: ${err.message}`);
      }
      // ENOENT means file already gone — treat as success (missing_ok=True)
    }
  }

  /**
   * Save instance metadata to disk using atomic write.
   *
   * Persists the current state of the instance tracker, including tracked
   * python processes, to the instance metadata file. Uses atomic write
   * (temp file + rename) to ensure the file is never left in a corrupted
   * state if the write is interrupted.
   */
  async _save(): Promise<void> {
    const data = {
      instance_id: this.instanceId,
      pid: this.pid,
      started_at: this.startedAt,
      python_processes: this._pythonProcesses,
    };

    const tempFile = this.instanceFile.replace(/\.json$/, ".tmp");
    await fs.writeFile(tempFile, JSON.stringify(data, null, 2), "utf-8");
    await fs.rename(tempFile, this.instanceFile);
  }
}

// ---------------------------------------------------------------------------
// isProcessRunning
// ---------------------------------------------------------------------------

/**
 * Check if a process with the given PID is currently running.
 *
 * Uses `process.kill(pid, 0)` which sends signal 0 (no-op) to probe
 * process existence without terminating it. Works on macOS and Linux;
 * on Windows, throws if the process does not exist.
 *
 * @param pid - Process ID to check.
 * @returns True if the process is running, False otherwise.
 *
 * Note:
 *   Returns false if the process does not exist. Unlike psutil.pid_exists(),
 *   this may return false due to permission errors on some platforms.
 */
export function isProcessRunning(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
}

// ---------------------------------------------------------------------------
// cleanupOrphanedResources
// ---------------------------------------------------------------------------

/**
 * Clean up orphaned Docker containers and python processes from dead server instances.
 *
 * This function should be called on MCP server startup. It:
 * 1. Scans the instances directory for registered server instances
 * 2. Checks if each instance's process is still running
 * 3. For dead instances, cleans up their orphaned resources:
 *    - Stops and removes Docker containers (via instance label)
 *    - Terminates python processes (from instance metadata)
 * 4. Removes instance metadata files for dead instances
 *
 * The cleanup is safe for concurrent server instances - only resources from
 * dead servers are cleaned up. Running servers are left untouched.
 *
 * @example
 * ```typescript
 * // In lifespan, before yielding context
 * await cleanupOrphanedResources();
 * ```
 *
 * Note:
 *   Errors during cleanup are logged but don't raise exceptions, ensuring
 *   that server startup continues even if cleanup partially fails.
 */
export async function cleanupOrphanedResources(
  instancesDir: string = path.join(os.homedir(), ".deephaven-mcp", "instances"),
): Promise<void> {

  let dirEntries: string[];
  try {
    dirEntries = await fs.readdir(instancesDir);
  } catch (e: unknown) {
    const err = e as NodeJS.ErrnoException;
    if (err.code === "ENOENT") {
      _logger.debug("[InstanceTracker] No instances directory, skipping orphan cleanup");
      return;
    }
    throw e;
  }

  const instanceFiles = dirEntries
    .filter((f) => f.endsWith(".json"))
    .map((f) => path.join(instancesDir, f));

  if (instanceFiles.length === 0) {
    _logger.debug("[InstanceTracker] No instance files found, skipping orphan cleanup");
    return;
  }

  _logger.info(
    `[InstanceTracker] Checking ${instanceFiles.length} instance(s) for orphaned resources...`,
  );

  for (const instanceFile of instanceFiles) {
    try {
      const tracker = InstanceTracker.loadFromFile(instanceFile);

      if (isProcessRunning(tracker.pid)) {
        _logger.debug(
          `[InstanceTracker] Instance ${tracker.instanceId} still running (PID ${tracker.pid}), skipping`,
        );
        continue;
      }

      _logger.warn(
        `[InstanceTracker] Found dead instance ${tracker.instanceId} (PID ${tracker.pid}), cleaning up orphans...`,
      );

      await _cleanupDockerContainersForInstance(tracker.instanceId);
      await _cleanupPythonProcessesForInstance(tracker);

      try {
        await fs.unlink(instanceFile);
      } catch {
        // ignore if already gone
      }

      _logger.info(
        `[InstanceTracker] Cleaned up orphaned resources for instance ${tracker.instanceId}`,
      );
    } catch (e: unknown) {
      const err = e as Error;
      _logger.error(
        `[InstanceTracker] Error cleaning up instance ${path.basename(instanceFile)}: ${err.message}`,
      );
    }
  }
}

/**
 * Test-seam alias for `cleanupOrphanedResources` that accepts an explicit instances directory.
 *
 * Because `vi.spyOn(os, "homedir")` cannot work in ESM (Node built-in namespaces are
 * not configurable), tests use this function directly with a temp directory.
 *
 * @param instancesDir - Absolute path to the instances directory to scan.
 */
export const _cleanupInstancesDir = cleanupOrphanedResources;

// ---------------------------------------------------------------------------
// _cleanupDockerContainersForInstance
// ---------------------------------------------------------------------------

/**
 * Clean up Docker containers for a specific server instance.
 *
 * Finds all Docker containers labeled with the instance ID and stops/removes them.
 * Uses the 'deephaven-mcp-server-instance' label to identify containers belonging
 * to the dead instance.
 *
 * @param instanceId - The instance UUID to clean up containers for.
 *
 * Note:
 *   Errors during cleanup are logged but do not raise exceptions.
 */
export async function _cleanupDockerContainersForInstance(instanceId: string): Promise<void> {
  try {
    const stdout = await _runCommand("docker", [
      "ps",
      "-a",
      "--filter",
      `label=deephaven-mcp-server-instance=${instanceId}`,
      "--format",
      "{{.ID}}",
    ]);

    if (stdout === null) {
      return;
    }

    const containerIds = stdout
      .trim()
      .split("\n")
      .map((s) => s.trim())
      .filter((s) => s.length > 0);

    if (containerIds.length === 0) {
      _logger.debug(`[InstanceTracker] No Docker containers found for instance ${instanceId}`);
      return;
    }

    _logger.info(
      `[InstanceTracker] Found ${containerIds.length} orphaned container(s) for instance ${instanceId}`,
    );

    for (const containerId of containerIds) {
      _logger.info(`[InstanceTracker] Stopping orphaned container ${containerId.slice(0, 12)}...`);

      // Try to stop gracefully (ignore errors — container may already be stopped)
      await _runCommand("docker", ["stop", containerId]);

      // Remove the container
      await _runCommand("docker", ["rm", containerId]);

      _logger.info(`[InstanceTracker] Cleaned up orphaned container ${containerId.slice(0, 12)}`);
    }
  } catch (e: unknown) {
    const err = e as Error;
    _logger.error(
      `[InstanceTracker] Error cleaning up Docker containers for instance ${instanceId}: ${err.message}`,
    );
  }
}

// ---------------------------------------------------------------------------
// _cleanupPythonProcessesForInstance
// ---------------------------------------------------------------------------

/**
 * Clean up python processes for a specific server instance.
 *
 * Terminates all python processes tracked in the instance metadata. Sends
 * SIGTERM to each tracked process after verifying it's still running.
 * Processes that are already dead are logged and skipped.
 *
 * @param tracker - The instance tracker with python process information.
 *
 * Note:
 *   Errors during process termination (e.g., permission denied, process already
 *   exited) are logged as warnings but do not raise exceptions.
 */
export async function _cleanupPythonProcessesForInstance(tracker: InstanceTracker): Promise<void> {
  const pythonProcesses = tracker._pythonProcesses;

  if (Object.keys(pythonProcesses).length === 0) {
    _logger.debug(
      `[InstanceTracker] No python processes found for instance ${tracker.instanceId}`,
    );
    return;
  }

  _logger.info(
    `[InstanceTracker] Found ${Object.keys(pythonProcesses).length} orphaned python process(es) for instance ${tracker.instanceId}`,
  );

  for (const [sessionName, pid] of Object.entries(pythonProcesses)) {
    try {
      if (isProcessRunning(pid)) {
        _logger.info(
          `[InstanceTracker] Terminating orphaned python process ${pid} (session: ${sessionName})`,
        );
        process.kill(pid, "SIGTERM");
      } else {
        _logger.debug(
          `[InstanceTracker] Python process ${pid} (session: ${sessionName}) already dead`,
        );
      }
    } catch (e: unknown) {
      const err = e as Error;
      _logger.warn(
        `[InstanceTracker] Error terminating python process ${pid} (session: ${sessionName}): ${err.message}`,
      );
    }
  }
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

/**
 * Run a subprocess command and return its stdout, or null on failure.
 *
 * @param cmd - Command to run.
 * @param args - Arguments for the command.
 * @returns Stdout string on success, null if the command fails.
 */
async function _runCommand(cmd: string, args: string[]): Promise<string | null> {
  return new Promise<string | null>((resolve) => {
    const child = spawn(cmd, args, { stdio: ["ignore", "pipe", "pipe"] });
    let stdout = "";
    let stderr = "";

    child.stdout.on("data", (chunk: Buffer) => {
      stdout += chunk.toString();
    });
    child.stderr.on("data", (chunk: Buffer) => {
      stderr += chunk.toString();
    });

    child.on("close", (code) => {
      if (code !== 0) {
        _logger.debug(`[InstanceTracker] Command ${cmd} ${args.join(" ")} failed (code ${code}): ${stderr.trim()}`);
        resolve(null);
      } else {
        resolve(stdout);
      }
    });

    child.on("error", (err) => {
      _logger.debug(`[InstanceTracker] Command ${cmd} failed to start: ${err.message}`);
      resolve(null);
    });
  });
}
