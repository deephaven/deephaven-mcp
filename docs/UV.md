# Using `uv`

[`uv`](https://github.com/astral-sh/uv) is a fast Python package manager and workflow tool. This document is a generic crash course for developers new to `uv`. Project-specific commands for working in this repository live in [`DEVELOPER_GUIDE.md`](DEVELOPER_GUIDE.md); end-user installation instructions live in the project [`README.md`](../README.md).

## Table of Contents

- [Why use `uv`?](#why-use-uv)
- [Installing `uv`](#installing-uv)
- [Virtual environments](#virtual-environments)
- [Installing dependencies](#installing-dependencies)
- [Synchronizing dependencies](#synchronizing-dependencies)
- [Running commands](#running-commands)
- [Code quality and testing](#code-quality-and-testing)
- [Lock files](#lock-files)
- [Upgrading dependencies](#upgrading-dependencies)
- [`uv tool install`](#uv-tool-install)
- [`.env` files](#env-files)
- [Common pitfalls](#common-pitfalls)
- [Further reading](#further-reading)

## Why use `uv`?

- **Speed.** Resolution and installs are dramatically faster than `pip`.
- **Reproducibility.** A `uv.lock` file pins exact versions across machines.
- **Convenience.** A single tool covers venv creation, dependency installs, lock-file management, and `python` / script execution.

## Installing `uv`

```sh
pip install uv
```

Other install methods (standalone installer, Homebrew, asdf, etc.) are listed in the [`uv` installation guide](https://github.com/astral-sh/uv#installation).

## Virtual environments

Create an isolated environment with the Python version you want:

```sh
uv venv .venv -p 3.12
```

`uv` commands automatically detect and use a `.venv` directory in the current project; activation is optional. To activate it manually:

```sh
source .venv/bin/activate          # Unix / macOS
.venv\Scripts\activate             # Windows
```

After activation you can use `python`, `pip`, `pytest`, etc. directly without the `uv run` prefix.

## Installing dependencies

From a project that has a `pyproject.toml`:

```sh
uv pip install .                   # runtime deps
uv pip install ".[dev]"            # an optional-dependency group named "dev"
```

`uv pip install` is a drop-in replacement for `pip install` and accepts the same arguments (`-r requirements.txt`, `--upgrade`, `-e`, etc.).

## Synchronizing dependencies

```sh
uv sync
```

Installs every package listed in `uv.lock` at the exact pinned version, removing anything not in the lock file. Use this after pulling new commits or switching branches to bring your environment into sync.

## Running commands

`uv run` runs a command inside the project's environment without requiring activation:

```sh
uv run python script.py
uv run pytest
uv run <any-installed-cli>
```

`uv` resolves and creates the environment on first run.

## Code quality and testing

`uv run` is the standard way to invoke any developer tool installed in your environment, so you don't need to activate the venv. Common patterns:

```sh
uv run pytest                              # run tests
uv run pytest --cov                        # run tests with coverage
uv run isort .                             # sort imports
uv run black .                             # format code
uv run ruff check src                      # lint
uv run ruff check src --fix                # lint and apply autofixes
uv run mypy src/                           # type-check
```

Any tool listed in your project's optional-dependency groups (e.g. a `[dev]` or `[lint]` extra) becomes invocable via `uv run <tool>` once the group is installed.

## Lock files

- `uv.lock` is generated from `pyproject.toml` and pins every transitive dependency.
- Commit it alongside `pyproject.toml`.
- After changing dependencies, regenerate it:

  ```sh
  uv lock
  ```

## Upgrading dependencies

```sh
uv pip install ".[dev]" --upgrade
uv lock                            # regenerate the lock file
```

Commit both `pyproject.toml` and the updated `uv.lock`.

## `uv tool install`

`uv tool install <package>` installs a Python package's CLI entry points into an isolated environment and places them on your `PATH`. There is no venv to manage.

```sh
uv tool install --python-preference managed <package>
uv tool list
uv tool upgrade <package>
uv tool dir                        # show where tools are installed
```

`--python-preference managed` tells `uv` to download and use its own Python interpreter, isolated from any system Python. Tool scripts land in `~/.local/bin/` (macOS / Linux) or `%LOCALAPPDATA%\uv\bin\` (Windows); ensure that directory is on your `PATH` (`uv tool update-shell` will add it for common shells).

## `.env` files

`uv run` does **not** auto-load `.env` files. To pass environment variables to a command, either export them in your shell first or use a tool such as [`python-dotenv`](https://github.com/theskumar/python-dotenv) inside your code:

```sh
export FOO=bar
uv run python script.py
```

## Common pitfalls

| Symptom | Cause / fix |
| --- | --- |
| `command not found: uv` | `uv` is not on `PATH` after install — open a new shell or add the install directory to `PATH`. |
| Environment doesn't match the lock file | Run `uv sync`. |
| `uv pip install` succeeds but the new package is not visible | You ran the install in a different venv than the one your shell / IDE is using. Verify with `uv pip list`. |
| Lock file is out of date after editing `pyproject.toml` | Run `uv lock`. |
| Wrong Python version in the venv | Recreate with `uv venv .venv -p <version>`. |

## Further reading

- [`uv` documentation](https://docs.astral.sh/uv/)
- [`uv` GitHub repository](https://github.com/astral-sh/uv)
- [`python-dotenv`](https://github.com/theskumar/python-dotenv)
