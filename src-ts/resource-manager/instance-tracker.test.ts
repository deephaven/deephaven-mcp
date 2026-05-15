/**
 * Tests for resource-manager/instance-tracker module.
 */
import { describe, it, expect, beforeEach, afterEach } from "vitest";
import * as fs from "node:fs/promises";
import * as path from "node:path";
import * as os from "node:os";
import {
  InstanceTracker,
  isProcessRunning,
  cleanupOrphanedResources,
  _cleanupDockerContainersForInstance,
  _cleanupPythonProcessesForInstance,
} from "./instance-tracker.js";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

async function makeTempInstancesDir(): Promise<string> {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), "test-instances-"));
  return dir;
}

async function makeTestTracker(instancesDir: string): Promise<InstanceTracker> {
  const instanceId = "test-uuid-1234";
  const pid = process.pid;
  const startedAt = new Date().toISOString();
  const tracker = new InstanceTracker(instanceId, pid, startedAt);
  // Override instanceFile to use temp dir
  (tracker as unknown as { instanceFile: string }).instanceFile = path.join(
    instancesDir,
    `${instanceId}.json`,
  );
  return tracker;
}

// ---------------------------------------------------------------------------
// InstanceTracker constructor
// ---------------------------------------------------------------------------

describe("InstanceTracker constructor", () => {
  it("sets_instance_id", () => {
    const t = new InstanceTracker("abc-123", 999, "2024-01-01T00:00:00.000Z");
    expect(t.instanceId).toBe("abc-123");
  });

  it("sets_pid", () => {
    const t = new InstanceTracker("abc-123", 999, "2024-01-01T00:00:00.000Z");
    expect(t.pid).toBe(999);
  });

  it("sets_started_at", () => {
    const t = new InstanceTracker("abc-123", 999, "2024-01-01T00:00:00.000Z");
    expect(t.startedAt).toBe("2024-01-01T00:00:00.000Z");
  });

  it("initializes_empty_python_processes", () => {
    const t = new InstanceTracker("abc-123", 999, "2024-01-01T00:00:00.000Z");
    expect(t._pythonProcesses).toEqual({});
  });

  it("sets_instance_file_path", () => {
    const t = new InstanceTracker("my-uuid", 999, "2024-01-01T00:00:00.000Z");
    expect(t.instanceFile).toContain("my-uuid.json");
    expect(t.instanceFile).toContain(".deephaven-mcp");
    expect(t.instanceFile).toContain("instances");
  });
});

// ---------------------------------------------------------------------------
// InstanceTracker.createAndRegister
// ---------------------------------------------------------------------------

describe("InstanceTracker.createAndRegister", () => {
  // NOTE: createAndRegister writes to the real ~/.deephaven-mcp/instances/ directory.
  // Each test cleans up the created file. vi.spyOn(os, "homedir") doesn't work in ESM.

  it("creates_tracker_with_valid_uuid", async () => {
    const tracker = await InstanceTracker.createAndRegister();
    try {
      expect(tracker.instanceId).toMatch(
        /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/,
      );
    } finally {
      await fs.unlink(tracker.instanceFile).catch(() => undefined);
    }
  });

  it("sets_current_pid", async () => {
    const tracker = await InstanceTracker.createAndRegister();
    try {
      expect(tracker.pid).toBe(process.pid);
    } finally {
      await fs.unlink(tracker.instanceFile).catch(() => undefined);
    }
  });

  it("creates_instance_file", async () => {
    const tracker = await InstanceTracker.createAndRegister();
    try {
      const content = await fs.readFile(tracker.instanceFile, "utf-8");
      const data = JSON.parse(content) as unknown;
      expect((data as Record<string, unknown>)["instance_id"]).toBe(tracker.instanceId);
    } finally {
      await fs.unlink(tracker.instanceFile).catch(() => undefined);
    }
  });
});

// ---------------------------------------------------------------------------
// InstanceTracker.loadFromFile
// ---------------------------------------------------------------------------

describe("InstanceTracker.loadFromFile", () => {
  let instancesDir: string;

  beforeEach(async () => {
    instancesDir = await makeTempInstancesDir();
  });

  afterEach(async () => {
    await fs.rm(instancesDir, { recursive: true, force: true });
  });

  it("loads_instance_id_pid_started_at", async () => {
    const filePath = path.join(instancesDir, "test.json");
    await fs.writeFile(
      filePath,
      JSON.stringify({ instance_id: "my-id", pid: 1234, started_at: "2024-01-01T00:00:00.000Z" }),
    );
    const tracker = InstanceTracker.loadFromFile(filePath);
    expect(tracker.instanceId).toBe("my-id");
    expect(tracker.pid).toBe(1234);
    expect(tracker.startedAt).toBe("2024-01-01T00:00:00.000Z");
  });

  it("loads_python_processes", async () => {
    const filePath = path.join(instancesDir, "test.json");
    await fs.writeFile(
      filePath,
      JSON.stringify({
        instance_id: "my-id",
        pid: 1234,
        started_at: "2024-01-01T00:00:00.000Z",
        python_processes: { "session-a": 5678 },
      }),
    );
    const tracker = InstanceTracker.loadFromFile(filePath);
    expect(tracker._pythonProcesses).toEqual({ "session-a": 5678 });
  });

  it("defaults_python_processes_to_empty_when_missing", async () => {
    const filePath = path.join(instancesDir, "test.json");
    await fs.writeFile(
      filePath,
      JSON.stringify({ instance_id: "my-id", pid: 1234, started_at: "2024-01-01T00:00:00.000Z" }),
    );
    const tracker = InstanceTracker.loadFromFile(filePath);
    expect(tracker._pythonProcesses).toEqual({});
  });

  it("throws_if_file_missing", () => {
    expect(() => InstanceTracker.loadFromFile("/no/such/file.json")).toThrow();
  });

  it("throws_if_invalid_json", async () => {
    const filePath = path.join(instancesDir, "bad.json");
    await fs.writeFile(filePath, "not json");
    expect(() => InstanceTracker.loadFromFile(filePath)).toThrow();
  });
});

// ---------------------------------------------------------------------------
// InstanceTracker._save
// ---------------------------------------------------------------------------

describe("InstanceTracker._save", () => {
  let instancesDir: string;

  beforeEach(async () => {
    instancesDir = await makeTempInstancesDir();
  });

  afterEach(async () => {
    await fs.rm(instancesDir, { recursive: true, force: true });
  });

  it("writes_json_to_instance_file", async () => {
    const tracker = await makeTestTracker(instancesDir);
    await tracker._save();
    const content = await fs.readFile(tracker.instanceFile, "utf-8");
    const data = JSON.parse(content) as Record<string, unknown>;
    expect(data["instance_id"]).toBe(tracker.instanceId);
    expect(data["pid"]).toBe(tracker.pid);
    expect(data["started_at"]).toBe(tracker.startedAt);
  });

  it("persists_python_processes", async () => {
    const tracker = await makeTestTracker(instancesDir);
    tracker._pythonProcesses["sess"] = 42;
    await tracker._save();
    const content = await fs.readFile(tracker.instanceFile, "utf-8");
    const data = JSON.parse(content) as Record<string, unknown>;
    expect((data["python_processes"] as Record<string, number>)["sess"]).toBe(42);
  });
});

// ---------------------------------------------------------------------------
// InstanceTracker.trackPythonProcess
// ---------------------------------------------------------------------------

describe("InstanceTracker.trackPythonProcess", () => {
  let instancesDir: string;

  beforeEach(async () => {
    instancesDir = await makeTempInstancesDir();
  });

  afterEach(async () => {
    await fs.rm(instancesDir, { recursive: true, force: true });
  });

  it("adds_session_to_python_processes", async () => {
    const tracker = await makeTestTracker(instancesDir);
    await tracker.trackPythonProcess("my-session", 9999);
    expect(tracker._pythonProcesses["my-session"]).toBe(9999);
  });

  it("persists_to_disk", async () => {
    const tracker = await makeTestTracker(instancesDir);
    await tracker.trackPythonProcess("my-session", 9999);
    const content = await fs.readFile(tracker.instanceFile, "utf-8");
    const data = JSON.parse(content) as Record<string, unknown>;
    expect((data["python_processes"] as Record<string, number>)["my-session"]).toBe(9999);
  });
});

// ---------------------------------------------------------------------------
// InstanceTracker.untrackPythonProcess
// ---------------------------------------------------------------------------

describe("InstanceTracker.untrackPythonProcess", () => {
  let instancesDir: string;

  beforeEach(async () => {
    instancesDir = await makeTempInstancesDir();
  });

  afterEach(async () => {
    await fs.rm(instancesDir, { recursive: true, force: true });
  });

  it("removes_session_from_python_processes", async () => {
    const tracker = await makeTestTracker(instancesDir);
    await tracker.trackPythonProcess("my-session", 9999);
    await tracker.untrackPythonProcess("my-session");
    expect("my-session" in tracker._pythonProcesses).toBe(false);
  });

  it("is_no_op_for_missing_session", async () => {
    const tracker = await makeTestTracker(instancesDir);
    // Should not throw
    await expect(tracker.untrackPythonProcess("not-tracked")).resolves.toBeUndefined();
  });

  it("persists_removal_to_disk", async () => {
    const tracker = await makeTestTracker(instancesDir);
    await tracker.trackPythonProcess("my-session", 9999);
    await tracker.untrackPythonProcess("my-session");
    const content = await fs.readFile(tracker.instanceFile, "utf-8");
    const data = JSON.parse(content) as Record<string, unknown>;
    expect("my-session" in (data["python_processes"] as Record<string, number>)).toBe(false);
  });
});

// ---------------------------------------------------------------------------
// InstanceTracker.unregister
// ---------------------------------------------------------------------------

describe("InstanceTracker.unregister", () => {
  let instancesDir: string;

  beforeEach(async () => {
    instancesDir = await makeTempInstancesDir();
  });

  afterEach(async () => {
    await fs.rm(instancesDir, { recursive: true, force: true });
  });

  it("removes_instance_file", async () => {
    const tracker = await makeTestTracker(instancesDir);
    await tracker._save();
    await tracker.unregister();
    await expect(fs.access(tracker.instanceFile)).rejects.toThrow();
  });

  it("is_no_op_if_file_missing", async () => {
    const tracker = await makeTestTracker(instancesDir);
    // File doesn't exist; should not throw
    await expect(tracker.unregister()).resolves.toBeUndefined();
  });
});

// ---------------------------------------------------------------------------
// isProcessRunning
// ---------------------------------------------------------------------------

describe("isProcessRunning", () => {
  it("returns_true_for_current_process", () => {
    expect(isProcessRunning(process.pid)).toBe(true);
  });

  it("returns_false_for_nonexistent_pid", () => {
    // PID 0 is special (process group), use a very high PID unlikely to exist
    // PID 1 exists on Linux but we test with unlikely-to-exist value
    const nonexistentPid = 99999999;
    expect(isProcessRunning(nonexistentPid)).toBe(false);
  });
});

// ---------------------------------------------------------------------------
// cleanupOrphanedResources
// ---------------------------------------------------------------------------
// NOTE: vi.spyOn(os, "homedir") does not work in ESM because Node built-in
// module namespaces are not configurable. cleanupOrphanedResources accepts an
// optional instancesDir parameter for testing.

describe("cleanupOrphanedResources", () => {
  it("returns_without_error_when_dir_missing", async () => {
    await expect(cleanupOrphanedResources("/no/such/dir/at/all")).resolves.toBeUndefined();
  });

  it("returns_without_error_when_instances_dir_is_empty", async () => {
    const tmpDir = await fs.mkdtemp(path.join(os.tmpdir(), "test-instances-"));
    try {
      await expect(cleanupOrphanedResources(tmpDir)).resolves.toBeUndefined();
    } finally {
      await fs.rm(tmpDir, { recursive: true, force: true });
    }
  });

  it("skips_running_instances", async () => {
    const tmpDir = await fs.mkdtemp(path.join(os.tmpdir(), "test-instances-"));
    const data = {
      instance_id: "alive-uuid",
      pid: process.pid,
      started_at: new Date().toISOString(),
      python_processes: {},
    };
    await fs.writeFile(path.join(tmpDir, "alive-uuid.json"), JSON.stringify(data));

    await cleanupOrphanedResources(tmpDir);
    // File should still exist because the process is running
    await expect(fs.access(path.join(tmpDir, "alive-uuid.json"))).resolves.toBeUndefined();
    await fs.rm(tmpDir, { recursive: true, force: true });
  });

  it("removes_dead_instance_file", async () => {
    const tmpDir = await fs.mkdtemp(path.join(os.tmpdir(), "test-instances-"));
    const data = {
      instance_id: "dead-uuid",
      pid: 99999999,
      started_at: new Date().toISOString(),
      python_processes: {},
    };
    await fs.writeFile(path.join(tmpDir, "dead-uuid.json"), JSON.stringify(data));

    await cleanupOrphanedResources(tmpDir);
    // File should be removed
    await expect(fs.access(path.join(tmpDir, "dead-uuid.json"))).rejects.toThrow();
    await fs.rm(tmpDir, { recursive: true, force: true });
  });

  it("handles_corrupt_instance_file_gracefully", async () => {
    const tmpDir = await fs.mkdtemp(path.join(os.tmpdir(), "test-instances-"));
    await fs.writeFile(path.join(tmpDir, "corrupt.json"), "not json at all");

    await expect(cleanupOrphanedResources(tmpDir)).resolves.toBeUndefined();
    await fs.rm(tmpDir, { recursive: true, force: true });
  });
});

// ---------------------------------------------------------------------------
// _cleanupDockerContainersForInstance
// ---------------------------------------------------------------------------

describe("_cleanupDockerContainersForInstance", () => {
  it("does_not_throw_when_docker_is_unavailable", async () => {
    // On a system without Docker or with no containers, should not throw
    await expect(
      _cleanupDockerContainersForInstance("nonexistent-instance-id"),
    ).resolves.toBeUndefined();
  });
});

// ---------------------------------------------------------------------------
// _cleanupPythonProcessesForInstance
// ---------------------------------------------------------------------------

describe("_cleanupPythonProcessesForInstance", () => {
  it("does_nothing_when_no_python_processes", async () => {
    const tracker = new InstanceTracker("test-id", 12345, "2024-01-01T00:00:00.000Z");
    await expect(_cleanupPythonProcessesForInstance(tracker)).resolves.toBeUndefined();
  });

  it("handles_already_dead_processes_gracefully", async () => {
    const tracker = new InstanceTracker("test-id", 12345, "2024-01-01T00:00:00.000Z");
    tracker._pythonProcesses["session-a"] = 99999999; // Very high PID, likely not running
    await expect(_cleanupPythonProcessesForInstance(tracker)).resolves.toBeUndefined();
  });
});
