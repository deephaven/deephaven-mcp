#!/bin/bash

# Manages Deephaven CorePlus client installation and version discovery from GCS.
#
# Key features:
# - Install from GCS tarball or local wheel file into pip or uv virtual environments
# - Discover and list available versions (channels, enterprise, point releases, community)
# - Uninstall and patch proto packages
# - Automatic grpcio version constraint handling
#
# For detailed command usage, run: ./dev_manage_coreplus_client.sh --help

# Strict mode
set -euo pipefail
# set -x # Uncomment for debugging

# --- Configuration ---
JDK_VERSION_TAG="jdk17"
_GCS_JENKINS_BASE_PATH="gs://illumon-software-repo/jenkins/${JDK_VERSION_TAG}"
GCS_BUCKET_PATH="${_GCS_JENKINS_BASE_PATH}/release" # Default to release, may be overridden by --rc
DEFAULT_CLIENT_WHEEL_PATTERN="deephaven_coreplus_client*.whl"
# Environment variable to control whether to save wheel files to current directory
SAVE_WHLS="${SAVE_WHLS:-false}"

# --- Cleanup Infrastructure ---
GLOBAL_TEMP_FILES=()

# Ensures all globally registered temporary files are removed on script exit.
cleanup_global_temp_files() {
  if [ ${#GLOBAL_TEMP_FILES[@]} -gt 0 ]; then
    echo "Cleaning up global temporary files: ${GLOBAL_TEMP_FILES[*]} ..." >&2
    rm -rf "${GLOBAL_TEMP_FILES[@]}"
  fi
}

trap cleanup_global_temp_files EXIT INT TERM

# Adds a file or directory path to the list of global temporary items for cleanup.
# Usage: _register_temp_file "/path/to/tempfile"
_register_temp_file() { 
  GLOBAL_TEMP_FILES+=("$1")
}

# --- Core Utilities ---

# Print error message and exit with status 1
die() {
  echo "ERROR: $*" >&2
  exit 1
}

# Print usage information
usage() {
  echo "Usage: $0 <command> [--venv <path>] [options]"
  echo
  echo "Commands:"
  echo "  list-release-channels                      List all release channels (RC codenames + prod)."
  echo "  list-enterprise-versions                   List enterprise versions (optionally for a channel)."
  echo "  list-point-releases                        List point releases (optionally for a channel and EV)."
  echo "  list-community-versions                    List community versions (optionally for a channel, EV, PR)."
  echo "  resolve-install-versions                   Resolve install versions (optionally for channel, EV, PR, CV)."
  echo "  install                                    Install the client wheel for the resolved versions (requires --venv)."
  echo "  install-wheel                              Install directly from a provided wheel file (requires --venv)."
  echo "  uninstall                                  Uninstall deephaven-coreplus-client (requires --venv)."
  echo "  patch                                      Patch deephaven_enterprise proto package (requires --venv)."
  echo
  echo "Options:"
  echo "  --venv <path>            Path to Python virtual environment (required for install/install-wheel/uninstall/patch commands)"
  echo "  --wheel-file <path>      Wheel file path (required for install-wheel)"
  echo "  --channel <channel>      Release channel (e.g., prod, gplus, etc)"
  echo "  --ev <enterprise_ver>    Enterprise version (e.g., 20240517)"
  echo "  --pr <point_release>     Point release (e.g., 483 or 250506093038c2cba50b47)"
  echo "  --cv <community_ver>     Community version (e.g., 0.41.0rc1)"
  echo
  echo "Environment Variables:"
  echo "  SAVE_WHLS               Set to 'true' to save wheel files to current directory during install (default: false)"
  echo
  echo "Examples:"
  echo "  $0 list-release-channels"
  echo "  $0 list-point-releases --ev 20240517"
  echo "  $0 install --venv .venv"
  echo "  $0 install-wheel --venv .venv --wheel-file /path/to/wheel.whl"
  echo "  $0 uninstall --venv .venv"
  exit 1
}

# Check for required tools and exit if any are missing
# Usage: check_tools "tool1" "tool2" ...
check_tools() {
  local tool
  for tool in "$@"; do
    if ! command -v "$tool" &> /dev/null; then
      local install_hint=""
      case "$tool" in
        gsutil) install_hint="Please install Google Cloud SDK." ;;
        jq)     install_hint="Please install jq (e.g., 'sudo apt-get install jq' or 'brew install jq')." ;;
        tar)    install_hint="Please ensure 'tar' is installed and in your PATH." ;;
        find)   install_hint="Please ensure 'find' is installed and in your PATH." ;;
        *)      install_hint="Please ensure it is installed and in your PATH." ;;
      esac
      echo "ERROR: Required command '$tool' not found. $install_hint" >&2
      exit 1
    fi
  done
}

# --- Virtual Environment Utilities ---

# Get the Python executable from the virtual environment
get_venv_python() {
  local venv_path="$1"
  
  # Check Unix-style path first
  if [[ -f "$venv_path/bin/python3" ]]; then
    echo "$venv_path/bin/python3"
  elif [[ -f "$venv_path/bin/python" ]]; then
    echo "$venv_path/bin/python"
  # Check Windows-style path
  elif [[ -f "$venv_path/Scripts/python.exe" ]]; then
    echo "$venv_path/Scripts/python.exe"
  elif [[ -f "$venv_path/Scripts/python" ]]; then
    echo "$venv_path/Scripts/python"
  else
    die "Could not find python in virtual environment at: $venv_path"
  fi
}

# Simple pip wrapper - detects pip vs uv automatically
# Usage: run_pip <venv_path> <pip_args...>
run_pip() {
  local venv_path="$1"
  shift
  
  local python_exe
  python_exe=$(get_venv_python "$venv_path")
  
  # Try python -m pip first, fall back to uv pip if not available
  if "$python_exe" -m pip --version &>/dev/null; then
    echo "Using: $python_exe -m pip $*" >&2
    "$python_exe" -m pip "$@"
  elif command -v uv &>/dev/null; then
    # uv requires: uv pip <subcommand> --python <python> <args>
    local subcommand="$1"
    shift
    echo "Using: uv pip $subcommand --python $python_exe $*" >&2
    uv pip "$subcommand" --python "$python_exe" "$@"
  else
    die "Virtual environment has no pip module and uv is not available. Install uv: curl -LsSf https://astral.sh/uv/install.sh | sh"
  fi
}

# Pip installer with grpcio constraint support
# Usage: run_pip_install <venv_path> [--grpcio-constraint <version>] <pip_install_args...>
run_pip_install() {
  local venv_path="$1"
  shift
  
  local args=()
  local grpcio_constraint=""
  
  # Parse arguments to extract grpcio constraint
  while [[ $# -gt 0 ]]; do
    case $1 in
      --grpcio-constraint)
        grpcio_constraint="$2"
        shift 2
        ;;
      *)
        args+=("$1")
        shift
        ;;
    esac
  done
  
  # Handle grpcio constraint if specified
  if [[ -n "$grpcio_constraint" ]]; then
    echo "Installing with grpcio constraint: $grpcio_constraint" >&2
    run_pip "$venv_path" install --force-reinstall --no-deps "${args[@]}" || return $?
    run_pip "$venv_path" install --force-reinstall "$grpcio_constraint"
  else
    run_pip "$venv_path" install "${args[@]}"
  fi
}

# --- GCS Utilities ---

run_gsutil() {
  local cmd_output
  local cmd_stderr
  local err_file
  # Ensure TEMP_DIR is set for mktemp, or use default tmp location
  local base_tmp_dir="${TEMP_DIR:-/tmp}" # TEMP_DIR could be set by the calling context if needed
  err_file=$(mktemp "${base_tmp_dir}/gsutil_stderr.XXXXXX")
  
  # Execute command, redirecting stderr to temp file
  cmd_output=$(gsutil "$@" 2>"$err_file")
  local exit_code=$?
  cmd_stderr=$(<"$err_file")
  rm -f "$err_file"
  
  if [ $exit_code -ne 0 ]; then
    echo "ERROR: gsutil command failed with exit code $exit_code: gsutil $*" >&2
    if [ -n "$cmd_stderr" ]; then
      echo "gsutil stderr:" >&2
      echo "$cmd_stderr" | sed 's/^/  /' >&2 # Indent stderr
    fi
    return $exit_code
  fi
  echo "$cmd_output"
  return 0
}

# --- Argument Parsing ---

parse_args() {
  positional_args=()  # Always initialize array first for strict mode
  # Initialize to avoid unbound variable errors
  parsed_channel=""
  parsed_ev=""
  parsed_pr=""
  parsed_cv=""
  parsed_venv=""
  parsed_wheel_file=""
  # Defaults
  default_channel="prod"
  # Note: ev, pr, cv defaults are not set here to avoid expensive GCS calls
  # They will be computed by resolve_install_versions if needed by the install command
  # Parse
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --channel)
        parsed_channel="$2"; shift 2;;
      --ev)
        parsed_ev="$2"; shift 2;;
      --pr)
        parsed_pr="$2"; shift 2;;
      --cv)
        parsed_cv="$2"; shift 2;;
      --venv)
        parsed_venv="$2"; shift 2;;
      --wheel-file)
        parsed_wheel_file="$2"; shift 2;;
      -h|--help)
        usage; exit 0;;
      --*)
        die "Unknown argument: $1";;
      -*)
        die "Unknown argument: $1";;
      *)
        positional_args+=("$1"); shift;;
    esac
  done
  # Safe expansion for set -- to avoid unbound errors
  if [ ${#positional_args[@]} -gt 0 ]; then
    set -- "${positional_args[@]}"
  else
    set --
  fi
  # Fill from positional if not set
  : "${parsed_channel:=${1:-$default_channel}}"
  : "${parsed_ev:=${2:-}}"
  : "${parsed_pr:=${3:-}}"
  : "${parsed_cv:=${4:-}}"
  # Export resolved vars in lowercase
  channel="$parsed_channel"
  ev="$parsed_ev"
  pr="$parsed_pr"
  cv="$parsed_cv"
  venv_path="$parsed_venv"
  wheel_file_path="$parsed_wheel_file"
  export channel ev pr cv venv_path wheel_file_path
}

# --- Version Discovery Functions ---

# Determine the correct version sort command (lazy-loaded on first use)
determine_version_sort_command() {
  # Test system sort -V
  if echo -e "1.0\n1.10\n1.2" | sort -V >/dev/null 2>&1 && \
     [[ $(echo -e "1.2\n1.10" | sort -V | tail -n1) == "1.10" ]]; then
    echo "sort"
    return 0
  fi
  # Test gsort -V (for macOS with coreutils)
  if command -v gsort >/dev/null 2>&1 && \
     echo -e "1.0\n1.10\n1.2" | gsort -V >/dev/null 2>&1 && \
     [[ $(echo -e "1.2\n1.10" | gsort -V | tail -n1) == "1.10" ]]; then
    echo "gsort"
    return 0
  fi

  die "A 'sort' command supporting GNU-style version sorting (-V) is required but not found or not working correctly. On macOS, install GNU coreutils: 'brew install coreutils' (provides 'gsort')."
}

# List all release channels (RC codenames + prod)
list_release_channels() {
  check_tools "gsutil"
  local rc_path_base="${_GCS_JENKINS_BASE_PATH}/rc/"
  local rc_list
  rc_list=$(run_gsutil ls "${rc_path_base}") || rc_list=""
  local processed_list
  processed_list=$(echo "$rc_list" | xargs -n1 basename | sed 's|/$||' | sort)
  echo "prod"
  if [ -n "$processed_list" ]; then
    echo "$processed_list"
  fi
}

# List enterprise versions for a channel (prod or RC)
list_enterprise_versions_for_channel() {
  check_tools "gsutil"
  local channel="$1"
  if [ "$channel" = "prod" ]; then
    local path="${_GCS_JENKINS_BASE_PATH}/release"
    local raw_list
    raw_list=$(run_gsutil ls -d "$path/*") || raw_list=""
    echo "$raw_list" | sed "s|$path/||; s|/||" | grep -E '^[0-9]+$' | $(determine_version_sort_command) -V
  else
    local rc_path="${_GCS_JENKINS_BASE_PATH}/rc/${channel}"
    local files
    files=$(run_gsutil ls "$rc_path/*" 2>/dev/null) || files=""
    # Extract 8-digit enterprise version from any filename containing '-1.<EV>.'
    echo "$files" | \
      sed -n 's/.*-1\.\([0-9]\{8\}\)\..*/\1/p' | \
      sort -uV
  fi
}

# List point releases for a channel and EV
list_point_releases_for_channel_and_ev() {
  check_tools "gsutil"
  local channel="$1"
  local ev="$2"
  local path
  if [ "$channel" = "prod" ]; then
    path="${_GCS_JENKINS_BASE_PATH}/release/${ev}"
    local files
    files=$(run_gsutil ls "$path/deephaven-coreplus-*-1.${ev}.*-${JDK_VERSION_TAG}.tgz" 2>/dev/null) || files=""
    pr_list=$(echo "$files" | sed -n "s/.*-1\.${ev}\.\(.*\)-${JDK_VERSION_TAG}\.tgz/\1/p" | sort -u)
    echo "$pr_list" | awk '/^[0-9]{3}$/ { print "A" $0; next } { print "B" $0 }' | sort | sed 's/^[AB]//'
  else
    path="${_GCS_JENKINS_BASE_PATH}/rc/${channel}"
    local files
    files=$(run_gsutil ls "$path/*" 2>/dev/null) || files=""
    filtered=$(echo "$files" | grep 'deephaven-coreplus-' | grep -- "-1.${ev}." || true)
    echo "$filtered" | \
      sed -n "s/.*-1\.${ev}\.\([^-\.]*\).*/\1/p" | \
      sort -u
  fi
}

# List community versions for a channel, EV, PR
list_community_versions_for_channel_ev_pr() {
  check_tools "gsutil"
  local channel="$1"
  local ev="$2"
  local pr="$3"
  local path
  if [ "$channel" = "prod" ]; then
    path="${_GCS_JENKINS_BASE_PATH}/release/${ev}"
    local files
    files=$(run_gsutil ls "$path/deephaven-coreplus-*-1.${ev}.${pr}-${JDK_VERSION_TAG}.tgz" 2>/dev/null) || files=""
    echo "$files" | sed -n "s|.*/deephaven-coreplus-\([^-]*\)-1\.${ev}\.${pr}-${JDK_VERSION_TAG}\.tgz|\1|p" | sort -u
  else
    path="${_GCS_JENKINS_BASE_PATH}/rc/${channel}"
    local files
    files=$(run_gsutil ls "${_GCS_JENKINS_BASE_PATH}/rc/${channel}/deephaven-coreplus-*-1.${ev}.${pr}*.tgz" 2>/dev/null) || files=""
    echo "$files" | sed -n "s|.*/deephaven-coreplus-\(.*\)-1\.${ev}\.${pr}.*\.tgz|\1|p" | sort -u
  fi
}

# Resolve install versions (channel, EV, PR, CV)
resolve_install_versions() {
  local channel="${1:-}"
  local ev="${2:-}"
  local pr="${3:-}"
  local cv="${4:-}"

  # Channel
  if [ -z "$channel" ]; then
    channel=$(list_release_channels | head -n1)
  fi
  # EV
  if [ -z "$ev" ]; then
    ev=$(list_enterprise_versions_for_channel "$channel" | tail -n1)
  fi
  # PR
  if [ -z "$pr" ]; then
    pr=$(list_point_releases_for_channel_and_ev "$channel" "$ev" | tail -n1)
  fi
  # CV
  if [ -z "$cv" ]; then
    cv=$(list_community_versions_for_channel_ev_pr "$channel" "$ev" "$pr" | tail -n1)
  fi
  echo "$channel $ev $pr $cv"
}

# --- Installation Functions ---

# Core function to install a wheel file with grpcio override logic
install_wheel_file() {
  local whl_file="$1"
  
  if [ ! -f "$whl_file" ]; then
    die "Wheel file not found: $whl_file"
  fi
  
  echo "Installing wheel: $whl_file" >&2
  
  # --- grpcio override logic ---
  # This section handles a specific dependency constraint for 'grpcio'.
  # The client wheel being installed might have its own 'grpcio' version requirements
  # that could conflict with the 'grpcio' version already in use by other critical
  # packages in the environment (e.g., pydeephaven).
  # To prevent the installer from upgrading or downgrading the existing 'grpcio', potentially
  # breaking other parts of the system, we explicitly tell the installer to use the currently
  # installed version of 'grpcio'.
  #
  # The 'pip install --override' command expects a file path as its argument,
  # where the file contains the package==version specifiers.
  # Instead of creating, writing to, and then deleting a temporary file,
  # we use Bash's process substitution feature: <(echo "grpcio==${current_grpcio_version}").
  # Bash executes the 'echo' command and provides its output via a special file
  # descriptor (e.g., /dev/fd/63) that 'pip' can read like a file. This avoids
  # manual temporary file management and keeps the script cleaner, provided 'pip'
  # correctly handles reading from such file descriptors.
  local current_grpcio_version
  local cmd_args=()
  echo "Checking for existing grpcio installation..." >&2
  current_grpcio_version=$( (run_pip "$venv_path" show grpcio || true) 2>/dev/null | awk '/^Version:/ {print $2}' | tr -d '[:space:]' )
  if [[ -n "$current_grpcio_version" ]]; then
    echo "Found existing grpcio version: '$current_grpcio_version'. Will attempt to override grpcio to this version." >&2
    cmd_args+=(--grpcio-constraint "grpcio==${current_grpcio_version}")
  else
    echo "No existing grpcio installation found. grpcio will be installed based on wheel's dependencies if required." >&2
  fi
  # --- end grpcio override logic ---
  
  cmd_args+=(--no-cache-dir --only-binary :all: "$whl_file")
  
  # Install using centralized helper function
  # shellcheck disable=SC2290 # process substitution is intentional
  if ! run_pip_install "$venv_path" "${cmd_args[@]}"; then
    die "Failed to install $whl_file."
  fi
  echo "Successfully installed $whl_file" >&2
  
  # copy the whl file to the current directory if requested
  if [ "$SAVE_WHLS" = "true" ]; then
    cp "$whl_file" .
    echo "Saved wheel file: $(basename "$whl_file")" >&2
  fi
}

# Install CorePlus client directly from a provided wheel file (for CI)
install_from_wheel_file() {
  local wheel_path="$1"
  
  if [ -z "$wheel_path" ]; then
    die "Usage: install_from_wheel_file <wheel_path>"
  fi
  
  if [ ! -f "$wheel_path" ]; then
    die "Wheel file not found: $wheel_path"
  fi
  
  echo "Installing CorePlus client from wheel file: $wheel_path" >&2
  install_wheel_file "$wheel_path"
  echo "Install complete." >&2
}

install_from_tgz_archive() {
  # Usage: install_from_tgz_archive <channel> <ev> <pr> <cv>
  local channel="$1"
  local ev="$2"
  local pr="$3"
  local cv="$4"
  : "${JDK_VERSION_TAG:?JDK_VERSION_TAG must be set (e.g., jdk17)}"
  set -e
  local tmpdir
  tmpdir=$(mktemp -d)
  _register_temp_file "$tmpdir"

  base_path="gs://illumon-software-repo/jenkins/${JDK_VERSION_TAG}"
  subdir="release/${ev}"
  [ "$channel" != "prod" ] && subdir="rc/${channel}"
  artifact_path="${base_path}/${subdir}/deephaven-coreplus-${cv}-1.${ev}.${pr}-${JDK_VERSION_TAG}.tgz"

  echo "Fetching artifact: $artifact_path" >&2
  gsutil cp "$artifact_path" "$tmpdir/" || die "Failed to fetch artifact from $artifact_path"
  artifact_file=$(ls "$tmpdir"/deephaven-coreplus-*.tgz)

  if [[ "$artifact_file" == *.tgz ]]; then
    echo "Extracting $artifact_file to $tmpdir" >&2
    tar -xzf "$artifact_file" -C "$tmpdir" || die "Failed to extract $artifact_file"
    whl_file=$(find "$tmpdir" -type f -name 'deephaven_coreplus_client-*.whl' | head -n 1)
    if [ -z "$whl_file" ]; then
      die "No deephaven_coreplus_client-*.whl file found inside $artifact_file"
    fi
    
    # Use the extracted wheel installation function
    install_wheel_file "$whl_file"
  else
    die "Downloaded artifact is not a .tgz: $artifact_file"
  fi
  echo "Install complete." >&2
}

# --- Post-Installation Functions ---

# Find deephaven_enterprise directories in site-packages
find_enterprise_dirs() {
  local python_bin="$1"
  
  # Find proto directory
  local proto_dir
  proto_dir=$("$python_bin" -c '
import site
import os
for p in site.getsitepackages():
    proto_path = os.path.join(p, "deephaven_enterprise", "proto")
    if os.path.exists(proto_path):
        print(proto_path)
        break
')
  
  # Find base directory
  local base_dir
  base_dir=$("$python_bin" -c '
import site
import os
for p in site.getsitepackages():
    base_path = os.path.join(p, "deephaven_enterprise")
    if os.path.exists(base_path):
        print(base_path)
        break
')
  
  echo "$proto_dir" "$base_dir"
}

# Create proto package __init__.py
create_proto_init() {
  local proto_dir="$1"
  
  if [ -f "$proto_dir/__init__.py" ]; then
    echo "$proto_dir/__init__.py already exists, skipping creation."
    return 0
  fi
  
  echo "Creating missing $proto_dir/__init__.py ..."
  cat > "$proto_dir/__init__.py" <<'EOF'
from . import acl_pb2
from . import acl_pb2_grpc
from . import auth_pb2
from . import auth_pb2_grpc
from . import auth_service_pb2
from . import auth_service_pb2_grpc
from . import common_pb2
from . import common_pb2_grpc
from . import controller_common_pb2
from . import controller_common_pb2_grpc
from . import controller_pb2
from . import controller_pb2_grpc
from . import controller_service_pb2
from . import controller_service_pb2_grpc
from . import persistent_query_pb2
from . import persistent_query_pb2_grpc
from . import table_definition_pb2
from . import table_definition_pb2_grpc
EOF
  echo "Created $proto_dir/__init__.py."
}

# Create base package __init__.py
create_base_init() {
  local base_dir="$1"
  
  if [ -f "$base_dir/__init__.py" ]; then
    echo "$base_dir/__init__.py already exists, skipping creation."
    return 0
  fi
  
  echo "Creating missing $base_dir/__init__.py ..."
  echo "# Regular package marker" > "$base_dir/__init__.py"
  echo "Created $base_dir/__init__.py."
}

# TODO: Remove this function after the proto package is fixed in the wheel
# See: https://deephaven.atlassian.net/browse/DH-19813
# Usage: patch_deephaven_enterprise_proto_package <venv_path>
patch_deephaven_enterprise_proto_package() {
  local venv_path="$1"
  
  local python_bin
  python_bin=$(get_venv_python "$venv_path")
  
  # Find directories
  local proto_dir base_dir
  read -r proto_dir base_dir < <(find_enterprise_dirs "$python_bin")
  
  if [[ -z "$proto_dir" || -z "$base_dir" ]]; then
    echo "[ERROR] Could not find deephaven_enterprise in site-packages. Skipping patch." >&2
    return 1
  fi
  
  echo "[INFO] proto_dir: $proto_dir"
  echo "[INFO] base_dir: $base_dir"
  echo "Patching Deephaven Enterprise proto package at $proto_dir ..."
  
  mkdir -p "$proto_dir"
  create_proto_init "$proto_dir"
  create_base_init "$base_dir"
  
  echo "Deephaven Enterprise proto package patch complete."
}

# --- Validation Functions ---

validate_venv_required() {
  if [[ -z "$venv_path" ]]; then
    die "--venv <path> is required for command '$COMMAND'"
  fi
  
  if [[ ! -d "$venv_path" ]]; then
    die "Virtual environment not found at: $venv_path"
  fi
  
  if [[ ! -f "$venv_path/pyvenv.cfg" ]]; then
    die "$venv_path does not appear to be a valid Python virtual environment (missing pyvenv.cfg)"
  fi
  
  echo "Using virtual environment at: $venv_path" >&2
}

# --- Main Script Logic ---

if [ $# -lt 1 ]; then usage; fi

COMMAND="$1"; shift

# Parse arguments
parse_args "$@"

# Validate --wheel-file is only used with install-wheel
if [[ -n "$wheel_file_path" && "$COMMAND" != "install-wheel" ]]; then
  die "--wheel-file option is only valid for the 'install-wheel' command"
fi

# Command dispatch with validation
case "$COMMAND" in
  list-release-channels)
    list_release_channels
    ;;
  list-enterprise-versions)
    list_enterprise_versions_for_channel "$channel"
    ;;
  list-point-releases)
    list_point_releases_for_channel_and_ev "$channel" "$ev"
    ;;
  list-community-versions)
    list_community_versions_for_channel_ev_pr "$channel" "$ev" "$pr"
    ;;
  resolve-install-versions)
    resolve_install_versions "$channel" "$ev" "$pr" "$cv"
    ;;
  install)
    validate_venv_required
    set -- $(resolve_install_versions "$channel" "$ev" "$pr" "$cv")
    channel="$1"; ev="$2"; pr="$3"; cv="$4"
    install_from_tgz_archive "$channel" "$ev" "$pr" "$cv"
    patch_deephaven_enterprise_proto_package "$venv_path"
    ;;
  install-wheel)
    if [ -z "$wheel_file_path" ]; then
      die "install-wheel requires --wheel-file <path> argument"
    fi
    validate_venv_required
    install_from_wheel_file "$wheel_file_path"
    patch_deephaven_enterprise_proto_package "$venv_path"
    ;;
  uninstall)
    validate_venv_required
    echo "Uninstalling deephaven-coreplus-client from the current environment..." >&2
    run_pip "$venv_path" uninstall -y deephaven-coreplus-client || die "Failed to uninstall deephaven-coreplus-client"
    echo "deephaven-coreplus-client has been uninstalled." >&2
    ;;
  patch)
    validate_venv_required
    patch_deephaven_enterprise_proto_package "$venv_path"
    ;;
  *)
    usage
    ;;
esac

exit 0 # Explicitly exit with success
