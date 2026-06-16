# Standalone Binaries (PyApp)

> **Project repository:** [https://github.com/deephaven/deephaven-mcp](https://github.com/deephaven/deephaven-mcp)

Standalone binaries let you run deephaven-mcp **without installing Python**. Each one is a single self-contained executable that bundles its own Python interpreter and every dependency, packaged with [PyApp](https://ofek.dev/pyapp/). It needs **no network access at any point** — not even the first run, which simply unpacks the bundle once into a per-user cache (a few seconds); later launches are fast.

## Install and run

1. **Download** the archive for your platform from the [GitHub Releases page](https://github.com/deephaven/deephaven-mcp/releases):

   | Platform | Archive |
   |----------|---------|
   | Linux (x86_64) | `deephaven-mcp-pyapp-<version>-linux-x86_64.tar.gz` |
   | Linux (arm64) | `deephaven-mcp-pyapp-<version>-linux-aarch64.tar.gz` |
   | macOS (Apple Silicon) | `deephaven-mcp-pyapp-<version>-macos-aarch64.tar.gz` |
   | Windows (x86_64) | `deephaven-mcp-pyapp-<version>-windows-x86_64.zip` |

2. **Extract it onto your `PATH`.** Each archive holds both executables, flat with no leading directory:

   ```bash
   tar -xzf deephaven-mcp-pyapp-*-macos-aarch64.tar.gz -C ~/.local/bin
   ```

   On Windows, unzip the `.zip` and put the `.exe` files in a directory on your `PATH`.

3. **Point your AI tool** at the extracted `dh-mcp-systems-server` (use its absolute path if it is not on your `PATH`). The first launch unpacks the embedded distribution, so it may take a few seconds; later launches are fast.

That is the whole install — there is nothing else to set up. Configuration is identical to any other install: the binary reads the same config directory and accepts the same flags as a `dh-mcp-systems-server` installed via `uv` or `pip`. See [`CONFIGURATION.md`](CONFIGURATION.md).

## What's included

Each archive contains two executables:

| Binary | What it is |
|--------|------------|
| `dh-mcp-systems-server` | The MCP server your AI tool connects to |
| `dh-mcp` | The local CLI |

The docs server (`dh-mcp-docs-server`) is **not** distributed this way — it runs as a hosted service.

> **Note:** Each binary is large (roughly 400+ MB) because it bundles a complete Python runtime plus all dependencies, including Community Core and Enterprise (Core+) support.

## Build it yourself

The published releases cover the platforms listed below. Build your own only when you need a platform that is not published, a different Python version, or your own distribution, using the build script [`scripts/build_pyapp.py`](../scripts/build_pyapp.py).

### Prerequisites

- [`uv`](https://docs.astral.sh/uv/) on your `PATH` — it builds the package and downloads the relocatable CPython that gets embedded.
- A **Rust toolchain** with `cargo` on your `PATH` — PyApp compiles a Rust launcher into each binary. Install it with the one-line script from [rustup.rs](https://rustup.rs).
- Network access **while building**, to download the Python runtime, the project's dependencies, and PyApp. (The finished binaries need no network.)

### Build

```bash
# Build both binaries for the current platform into dist/pyapp/<target>/
uv run scripts/build_pyapp.py

# Build only the systems server
uv run scripts/build_pyapp.py --binaries dh-mcp-systems-server

# Embed a specific CPython version
uv run scripts/build_pyapp.py --python-version 3.13
```

The embedded Python version and the set of binaries are configured in `[tool.pyapp]` of [`pyproject.toml`](../pyproject.toml) (`python-version` and `binaries`); the entry points come from `[project.scripts]`. `--python-version` and `--binaries` override those for a one-off build. Run `uv run scripts/build_pyapp.py --help` for the full option list.

The build writes, under `dist/pyapp/`:

```text
dist/pyapp/
├── <target>/
│   ├── dh-mcp-systems-server[.exe]
│   └── dh-mcp[.exe]
└── deephaven-mcp-pyapp-<version>-<target>.{tar.gz,zip}
```

The flat archive (`.tar.gz` on Unix, `.zip` on Windows) is the thing you distribute — extract it exactly as the [install steps](#install-and-run) describe.

### Supported platforms

Builds run **natively per platform** — there is no cross-compilation, because the build bundles platform-specific compiled packages (numba, deephaven-server, deephaven-coreplus-client, ...). To produce a binary for a platform, run the build on a machine of that architecture.

| Target | Build on |
|--------|----------|
| `linux-x86_64` | Linux x86_64 |
| `linux-aarch64` | Linux arm64 |
| `macos-aarch64` | macOS (Apple Silicon) |
| `windows-x86_64` | Windows x86_64 |

The build always targets the machine it runs on (it auto-detects the platform). Running it on an Intel Mac, for example, produces a `macos-x86_64` build, though that platform is not part of the published set.

## Releasing

Releases are produced by the [`build-pyapp.yml`](../.github/workflows/build-pyapp.yml) GitHub Actions workflow, which builds every supported platform on native runners and verifies each binary starts **with the network blocked** (proving it is self-contained) before publishing:

- **Push a `v*` tag** — the workflow builds all platforms and attaches the distribution archives to the matching GitHub Release.
- **Run it manually** (`workflow_dispatch`) — builds on demand and uploads the archives as workflow artifacts, so you can check a build before tagging.
