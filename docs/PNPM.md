# Using `pnpm` in deephaven-mcp

[`pnpm`](https://pnpm.io/) is the package manager used for the TypeScript port of this project. It provides fast, reproducible installs via a content-addressed store and a strict `pnpm-lock.yaml` lockfile.

> **Note:** Node.js ≥20 and pnpm v9 are required. Only pnpm-based workflows are documented here.

---

## Table of Contents

- [Why pnpm?](#why-pnpm)
- [Installing pnpm and Node.js](#installing-pnpm-and-nodejs)
- [Typical Workflows](#typical-workflows)
  - [Installing Dependencies](#installing-dependencies)
  - [Building](#building)
  - [Code Quality and Testing](#code-quality-and-testing)
- [Lock File Behavior](#lock-file-behavior)
- [Upgrading Dependencies](#upgrading-dependencies)
- [CI/CD Usage](#cicd-usage)
- [Troubleshooting](#troubleshooting)
- [Further Reading](#further-reading)

---

## Why pnpm?

- **Speed**: Significantly faster than npm for installs due to the content-addressed store.
- **Reproducibility**: `pnpm-lock.yaml` pins exact package versions across machines.
- **Disk efficiency**: Packages are stored once in a global store and hard-linked per project.
- **Strict hoisting**: Prevents packages from accidentally importing unlisted dependencies.

---

## Installing pnpm and Node.js

**Node.js ≥20 is required.** Install via [nvm](https://github.com/nvm-sh/nvm), [nodenv](https://github.com/nodenv/nodenv), or directly from [nodejs.org](https://nodejs.org/).

```sh
# Verify Node.js version
node --version  # must be ≥20

# Install pnpm v9
npm install -g pnpm@9
```

Or see the [pnpm installation guide](https://pnpm.io/installation) for other options.

---

## Typical Workflows

### Installing Dependencies

```sh
# Install all dependencies (reads pnpm-lock.yaml for exact versions)
pnpm install
```

### Building

```sh
# Compile TypeScript to dist/
pnpm build

# Type-check only (no output files)
pnpm typecheck
```

### Code Quality and Testing

```sh
# Run all tests
pnpm test

# Run tests with coverage
pnpm test -- --coverage

# Type-check
pnpm typecheck

# Lint
pnpm lint

# Format
pnpm format
```

---

## Lock File Behavior

`pnpm-lock.yaml` pins exact package versions and is committed to source control. Do not edit it manually.

- `pnpm install` — installs per the lockfile (fails if `package.json` and lockfile are inconsistent).
- `pnpm add <package>` — adds a package, updates both `package.json` and `pnpm-lock.yaml`.
- `pnpm update` — upgrades packages within `package.json` version ranges, updates lockfile.

---

## Upgrading Dependencies

```sh
# Upgrade all packages within version ranges
pnpm update

# Upgrade a specific package to latest
pnpm update <package-name> --latest

# After upgrading, run typecheck and tests to catch breakage
pnpm typecheck && pnpm test
```

---

## CI/CD Usage

Example GitHub Actions step for Node.js 20 + pnpm:

```yaml
- uses: actions/setup-node@v4
  with:
    node-version: '20'

- uses: pnpm/action-setup@v3
  with:
    version: 9

- name: Install dependencies
  run: pnpm install --frozen-lockfile

- name: Type check
  run: pnpm typecheck

- name: Test
  run: pnpm test
```

---

## Troubleshooting

| Symptom | Likely Cause | Fix |
|---|---|---|
| `pnpm: command not found` | pnpm not in PATH | Run `npm config get prefix` and add `<prefix>/bin` to `PATH` |
| `pnpm install` fails with "Node.js ≥22 required" | Wrong pnpm version | Install pnpm v9: `npm install -g pnpm@9` |
| `Cannot find module '...'` at runtime | `pnpm build` not run | Run `pnpm build` first |
| Type errors after `pnpm install` | Outdated `@types/*` | Run `pnpm update` and `pnpm typecheck` |
| Tests fail after `pnpm update` | Breaking dependency update | Run `git diff pnpm-lock.yaml` to identify changed packages |
| Lockfile ignored in CI | Missing `--frozen-lockfile` | Use `pnpm install --frozen-lockfile` in CI |

---

## Further Reading

- [pnpm documentation](https://pnpm.io/)
- [ENV.md](ENV.md) — environment variables reference
