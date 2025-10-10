#!/bin/bash

# Manages Deephaven CorePlus client installation and version discovery from GCS.
#
# Key features:
# - Installs a specific or the latest available deephaven_coreplus_client:
#   Downloads the CorePlus tarball, extracts it, and installs the client wheel using pip from a specified virtual environment.
# - Lists available Enterprise Versions, Point Releases, and Community Versions from GCS.
# - Determines and displays the latest consistent set of EV, PR, and CV.
#
# For detailed command usage, run: ./dev_manage_coreplus_client.sh --help

# Strict mode
set -euo pipefail
# set -x # Uncomment for debugging

die() {
  echo "ERROR: $*" >&2
  exit 1
}

# Function to determine the correct version sort command
_determine_version_sort_command() {
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

  echo "ERROR: A 'sort' command supporting GNU-style version sorting (-V) is required but not found or not working correctly." >&2
  echo "       Please ensure you have a compatible 'sort'." >&2
  echo "       On macOS, you can install GNU coreutils: 'brew install coreutils' (which provides 'gsort')." >&2
  exit 1
}

SORT_CMD_V_BASE=$(_determine_version_sort_command)

# --- Configuration ---
JDK_VERSION_TAG="jdk17"
_GCS_JENKINS_BASE_PATH="gs://illumon-software-repo/jenkins/${JDK_VERSION_TAG}"
GCS_BUCKET_PATH="${_GCS_JENKINS_BASE_PATH}/release" # Default to release, may be overridden by --rc
DEFAULT_CLIENT_WHEEL_PATTERN="deephaven_coreplus_client*.whl"
# Environment variable to control whether to save wheel files to current directory
SAVE_WHLS="${SAVE_WHLS:-false}"

GLOBAL_TEMP_FILES=()
# Ensures all globally registered temporary files are removed on script exit.
cleanup_global_temp_files() {
  if [ ${#GLOBAL_TEMP_FILES[@]} -gt 0 ]; then
    echo "Cleaning up global temporary files: ${GLOBAL_TEMP_FILES[*]} ..." >&2
    rm -rf "${GLOBAL_TEMP_FILES[@]}"
  fi
}
trap cleanup_global_temp_files EXIT INT TERM # Register cleanup for script exit, interrupt, or termination

# Adds a file or directory path to the list of global temporary items for cleanup.
# Usage: _register_temp_file "/path/to/tempfile"
_register_temp_file() { GLOBAL_TEMP_FILES+=("$1"); }

# --- Helper Functions ---

# Function to print usage
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

# Get the pip executable from the virtual environment
get_venv_pip() {
  local venv_path="$1"
  
  # Check Unix-style path first
  if [[ -f "$venv_path/bin/pip" ]]; then
    echo "$venv_path/bin/pip"
  # Check Windows-style path
  elif [[ -f "$venv_path/Scripts/pip.exe" ]]; then
    echo "$venv_path/Scripts/pip.exe"
  elif [[ -f "$venv_path/Scripts/pip" ]]; then
    echo "$venv_path/Scripts/pip"
  else
    die "Could not find pip in virtual environment at: $venv_path"
  fi
}

# Centralized pip installer function with grpcio constraint support
# Usage: run_pip_install <venv_path> [--grpcio-constraint <version>] <pip_install_args...>
run_pip_install() {
  local venv_path="$1"
  shift
  
  local pip_exe
  pip_exe=$(get_venv_pip "$venv_path")
  
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
  
  if [[ -n "$grpcio_constraint" ]]; then
    # pip doesn't have --override like uv, so use --force-reinstall --no-deps to bypass conflicts
    echo "Using pip with forced grpcio override: $pip_exe install --force-reinstall --no-deps ${args[*]}" >&2
    "$pip_exe" install --force-reinstall --no-deps "${args[@]}"
    local exit_code=$?
    if [[ $exit_code -eq 0 ]]; then
      # Now ensure grpcio is at the forced version
      echo "Ensuring grpcio version: $pip_exe install --force-reinstall $grpcio_constraint" >&2
      "$pip_exe" install --force-reinstall "$grpcio_constraint"
      exit_code=$?
    fi
    return $exit_code
  else
    echo "Using pip: $pip_exe install ${args[*]}" >&2
    "$pip_exe" install "${args[@]}"
  fi
}

# Simple pip show/uninstall wrapper
# Usage: run_pip <venv_path> <pip_args...>
run_pip() {
  local venv_path="$1"
  shift
  
  local pip_exe
  pip_exe=$(get_venv_pip "$venv_path")
  
  echo "Using pip: $pip_exe $*" >&2
  "$pip_exe" "$@"
}

# Parse optional CLI arguments as flags or positionally
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


# Function to check for required tools
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

# Function to clean up a temporary directory
cleanup_temp_dir() {
  if [ -n "$1" ] && [ -d "$1" ]; then
    echo "Cleaning up temporary directory: $1 ..." >&2
    rm -rf "$1"
    echo "Temporary directory $1 removed." >&2
  fi
}

# Helper function to run gsutil and capture stderr for better error reporting
run_gsutil() {
  local cmd_output
  local cmd_stderr
  local err_file
  # Ensure TEMP_DIR is set for mktemp, or use default tmp location
  local base_tmp_dir="${TEMP_DIR:-/tmp}" # TEMP_DIR could be set by the calling context if needed
  err_file=$(mktemp "${base_tmp_dir}/gsutil_stderr.XXXXXX")
  _register_temp_file "$err_file" # Register stderr temp file for global cleanup, to be safe
  
  # Execute command, redirecting actual stderr to the temp file
  cmd_output=$(gsutil "$@" 2>"$err_file")
  local exit_code=$?
  cmd_stderr=$(<"$err_file")
  rm -f "$err_file" # Clean up immediately
  
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

# --- Modular Version Discovery Functions ---

# 1. List all release channels (RC codenames + prod)
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

# 2. List enterprise versions for a channel (prod or RC)
list_enterprise_versions_for_channel() {
  check_tools "gsutil"
  local channel="$1"
  if [ "$channel" = "prod" ]; then
    local path="${_GCS_JENKINS_BASE_PATH}/release"
    local raw_list
    raw_list=$(run_gsutil ls -d "$path/*") || raw_list=""
    echo "$raw_list" | sed "s|$path/||; s|/||" | grep -E '^[0-9]+$' | ${SORT_CMD_V_BASE} -V
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


# 3. List point releases for a channel and EV
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

# 4. List community versions for a channel, EV, PR
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

# 5. Resolve install versions (channel, EV, PR, CV)
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

# --- Listing Functions REMOVED (replaced by modular functions above) ---

# --- Installation Function ---
fn_install_wheel() {
  check_tools "gsutil" "find"
  
  # Uses global _CONTEXT_EV, _CONTEXT_PR, _CONTEXT_CV
  echo "Determining versions for installation based on global context..." >&2
  echo "  Context EV: ${_CONTEXT_EV:-Not set}" >&2
  echo "  Context PR: ${_CONTEXT_PR:-Not set}" >&2
  echo "  Context CV: ${_CONTEXT_CV:-Not set}" >&2
  echo "  Context RC: ${_CONTEXT_RC_CODENAME:-Not set (using release path)}" >&2

  local enterprise_version=${_CONTEXT_EV}
  if [ -z "$enterprise_version" ]; then
    echo "Enterprise version not set in global context, attempting to find the latest..." >&2
    enterprise_version=$(get_latest_enterprise_version) # This function respects GCS_BUCKET_PATH (i.e. --rc)
    if [ -z "$enterprise_version" ]; then
      echo "ERROR: Could not determine the latest enterprise version from ${GCS_BUCKET_PATH}." >&2
      return 1
    fi
    echo "Using latest enterprise version: $enterprise_version" >&2
  fi

  local point_release=${_CONTEXT_PR}
  if [ -z "$point_release" ]; then
    # If EV was context-specified but PR was not, find latest PR for that EV.
    # If EV was also not context-specified (and thus found above), find latest PR for that found EV.
    echo "Point release not set in global context for EV $enterprise_version, attempting to find the latest..." >&2
    point_release=$(get_latest_point_release "$enterprise_version")
    if [ -z "$point_release" ]; then
      echo "ERROR: Could not determine the latest point release for EV $enterprise_version from ${GCS_BUCKET_PATH}." >&2
      return 1
    fi
    echo "Using latest point release for EV $enterprise_version: $point_release" >&2
  fi

  local community_version=${_CONTEXT_CV}
  if [ -z "$community_version" ]; then
    # Similar logic for CV: if EV/PR were context-specified or found, find latest CV for them.
    echo "Community version not set in global context for EV $enterprise_version PR $point_release, attempting to find the latest..." >&2
    community_version=$(get_latest_community_version "$enterprise_version" "$point_release")
    if [ -z "$community_version" ]; then
      echo "ERROR: Could not determine the latest community version for EV $enterprise_version PR $point_release from ${GCS_BUCKET_PATH}." >&2
      return 1
    fi
    echo "Using latest community version for EV $enterprise_version PR $point_release: $community_version" >&2
  fi

  echo "--- Selected for installation --- " >&2
  echo "  Enterprise Version:     ${enterprise_version}" >&2
  echo "  Enterprise Point Release: ${point_release}" >&2
  echo "  Community Version:        ${community_version}" >&2
  echo "------------------------------- " >&2

  local coreplus_tarball_name="deephaven-coreplus-${community_version}-1.${enterprise_version}.${point_release}-${JDK_VERSION_TAG}.tgz"
  local full_gcs_tarball_path="${GCS_BUCKET_PATH}/${enterprise_version}/${coreplus_tarball_name}"
  
  local temp_dir_name="tmp_coreplus_install_$$" # Unique temp dir name
  
  trap "cleanup_temp_dir \"${temp_dir_name}\"" EXIT # Ensure temp dir for this specific install is cleaned up if function exits prematurely

  echo "Starting installation of deephaven_coreplus_client wheel..." >&2
  local original_dir=$(pwd)
  
  echo "Creating temporary directory: ${temp_dir_name} ..." >&2
  mkdir -p "${temp_dir_name}"
  cd "${temp_dir_name}"
  echo "Changed directory to $(pwd)" >&2

  echo "Verifying existence of ${full_gcs_tarball_path} on GCS..." >&2
  if ! run_gsutil stat "${full_gcs_tarball_path}" >/dev/null 2>&1; then # Redirect stat's own stdout and stderr
    # run_gsutil will have already printed a detailed error from gsutil if it was a gsutil-level issue
    echo "ERROR: Verification failed. The specified tarball does not exist or is not accessible at ${full_gcs_tarball_path}" >&2
    echo "Please check the Enterprise Version (${enterprise_version}), Point Release (${point_release}), and Community Version (${community_version}) combination." >&2
    cd "${original_dir}"; exit 1 # Return to original directory and exit function due to tarball verification failure
  fi
  echo "Tarball verified successfully." >&2

  echo "Downloading ${coreplus_tarball_name} from ${full_gcs_tarball_path} ..." >&2
  run_gsutil cp "${full_gcs_tarball_path}" .
  if [ $? -ne 0 ]; then
    # run_gsutil already printed a detailed error from gsutil
    echo "Please check if the combination of EV=${enterprise_version}, PR=${point_release}, CV=${community_version} forms a valid tarball, or if there are GCS access issues." >&2
    cd "${original_dir}"; exit 1 # Return to original directory and exit function due to download failure
  fi
  echo "Download of ${coreplus_tarball_name} complete." >&2

  echo "Extracting ${coreplus_tarball_name}..." >&2
  if ! tar -xvzf "${coreplus_tarball_name}" > /dev/null; then # Suppress tar output
    echo "ERROR: Failed to extract ${coreplus_tarball_name}." >&2
    cd "${original_dir}"; exit 1 # Return to original directory and exit function due to extraction failure
  fi
  echo "Extraction complete." >&2
  
  echo "Searching for client wheel matching pattern: ${DEFAULT_CLIENT_WHEEL_PATTERN} ..." >&2
  local client_wheel_path
  echo "Locating client wheel (${DEFAULT_CLIENT_WHEEL_PATTERN})..." >&2
  client_wheel_path=$(find . -name "${DEFAULT_CLIENT_WHEEL_PATTERN}" -print -quit)

  if [ -z "${client_wheel_path}" ]; then
    echo "ERROR: Could not find the client wheel matching '${DEFAULT_CLIENT_WHEEL_PATTERN}' in the extracted archive." >&2
    echo "Files found with .whl extension:" >&2
    find . -name "*.whl"
    cd "${original_dir}"; exit 1 # Return to original directory and exit function due to wheel not found
  fi
  echo "Found client wheel: ${client_wheel_path}" >&2

  echo "Installing ${client_wheel_path}..." >&2

  # --- Start of grpcio override logic ---
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

  # Try to determine the currently installed grpcio version and clean it
  echo "Checking for existing grpcio installation..." >&2
  current_grpcio_version=$( (run_pip "$venv_path" show grpcio || true) 2>/dev/null | awk '/^Version:/ {print $2}' | tr -d '[:space:]' )

  if [[ -n "$current_grpcio_version" ]]; then
    echo "Found existing grpcio version: '${current_grpcio_version}'. Will attempt to override grpcio to this version." >&2
    # Add grpcio version constraint
    cmd_args+=(--grpcio-constraint "grpcio==${current_grpcio_version}")
  else
    echo "No existing grpcio installation found. grpcio will be installed based on wheel's dependencies if required." >&2
  fi
  # --- End of grpcio override logic ---

  # Add other standard options and the client wheel path
  # --only-binary :all: is used to ensure that only pre-compiled binary wheels are
  # installed for the client wheel and its dependencies. This avoids potential
  # build issues, particularly with packages like 'grpcio' which can be problematic
  # to build from source on some platforms/architectures.
  # --no-cache-dir is used to avoid issues with pip's caching mechanisms if needed.
  cmd_args+=(--no-cache-dir --only-binary :all: "${client_wheel_path}")

  # Install using centralized helper function
  if ! run_pip_install "$venv_path" "${cmd_args[@]}"; then # --only-binary :all: ensures only wheels; --no-cache-dir: avoid cache; override for grpcio if found
    echo "ERROR: Failed to install ${client_wheel_path}." >&2
    cd "${original_dir}"; exit 1 # Return to original directory and exit function due to install failure
  fi
  echo "Installation of ${client_wheel_path} successful." >&2

  cd "${original_dir}"
  echo "Returned to directory $(pwd)" >&2
  echo "Script finished successfully." >&2
}

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
  channel="$1"
  ev="$2"
  pr="$3"
  cv="$4"
  : "${JDK_VERSION_TAG:?JDK_VERSION_TAG must be set (e.g., jdk17)}"
  set -e
  tmpdir=$(mktemp -d)
  trap 'rm -rf "$tmpdir"' EXIT INT TERM

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

# --- install_coreplus_client (existing function should be above this) ---

# TODO: Remove this function after the proto package is fixed in the wheel (see https://deephaven.atlassian.net/browse/DH-19813)
# Usage: patch_deephaven_enterprise_proto_package <venv_path>
patch_deephaven_enterprise_proto_package() {
  local venv_path="$1"
  
  # Get the Python executable from the venv
  local python_bin
  python_bin=$(get_venv_python "$venv_path")

  local proto_dir base_dir
  proto_dir=$("$python_bin" -c 'import site, os; d=[p for p in site.getsitepackages() if os.path.exists(os.path.join(p, "deephaven_enterprise", "proto"))]; print(os.path.join(d[0], "deephaven_enterprise", "proto") if d else "")')
  base_dir=$("$python_bin" -c 'import site, os; d=[p for p in site.getsitepackages() if os.path.exists(os.path.join(p, "deephaven_enterprise"))]; print(os.path.join(d[0], "deephaven_enterprise") if d else "")')

  if [[ -z "$proto_dir" || -z "$base_dir" ]]; then
    echo "[ERROR] Could not find deephaven_enterprise/proto or base dir in site-packages. Skipping patch."
    exit 1
  fi

  echo "[INFO] proto_dir: $proto_dir"
  echo "[INFO] base_dir: $base_dir"
  echo "Patching Deephaven Enterprise proto package at $proto_dir ..."
  mkdir -p "$proto_dir"

  # Only create proto __init__.py if missing
  if [ ! -f "$proto_dir/__init__.py" ]; then
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
  else
    echo "$proto_dir/__init__.py already exists, skipping creation."
  fi

  # Only create base __init__.py if missing
  if [ ! -f "$base_dir/__init__.py" ]; then
    echo "Creating missing $base_dir/__init__.py ..."
    echo "# Regular package marker" > "$base_dir/__init__.py"
    echo "Created $base_dir/__init__.py."
  else
    echo "$base_dir/__init__.py already exists, skipping creation."
  fi

  echo "Deephaven Enterprise proto package patch complete."
}

# --- Main Script Logic ---

if [ $# -lt 1 ]; then usage; fi

COMMAND="$1"; shift

echo "DEBUG: COMMAND='$COMMAND'" >&2
echo "DEBUG: Remaining args: $@" >&2

# Parse arguments
parse_args "$@"

# Commands that require venv
case "$COMMAND" in
  install|install-wheel|uninstall|patch)
    # These commands modify the venv, so it's required
    if [[ -z "$venv_path" ]]; then
      die "--venv <path> is required for command '$COMMAND'"
    fi

    # Validate virtual environment exists
    if [[ ! -d "$venv_path" ]]; then
      die "Virtual environment not found at: $venv_path"
    fi

    # Validate it's actually a venv by checking for pyvenv.cfg
    if [[ ! -f "$venv_path/pyvenv.cfg" ]]; then
      die "$venv_path does not appear to be a valid Python virtual environment (missing pyvenv.cfg)"
    fi

    echo "Using virtual environment at: $venv_path" >&2
    ;;
esac

# Validate --wheel-file is only used with install-wheel
if [[ -n "$wheel_file_path" && "$COMMAND" != "install-wheel" ]]; then
  die "--wheel-file option is only valid for the 'install-wheel' command"
fi

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
    set -- $(resolve_install_versions "$channel" "$ev" "$pr" "$cv")
    channel="$1"; ev="$2"; pr="$3"; cv="$4"
    install_from_tgz_archive "$channel" "$ev" "$pr" "$cv"
    patch_deephaven_enterprise_proto_package "$venv_path"
    ;;
  install-wheel)
    echo "DEBUG: install-wheel command detected" >&2
    echo "DEBUG: wheel_file_path='$wheel_file_path'" >&2
    echo "DEBUG: venv_path='$venv_path'" >&2
    if [ -z "$wheel_file_path" ]; then
      die "install-wheel requires --wheel-file <path> argument"
    fi
    
    install_from_wheel_file "$wheel_file_path"
    patch_deephaven_enterprise_proto_package "$venv_path"
    ;;
  uninstall)
    echo "Uninstalling deephaven-coreplus-client from the current environment..." >&2
    run_pip "$venv_path" uninstall -y deephaven-coreplus-client || die "Failed to uninstall deephaven-coreplus-client"
    echo "deephaven-coreplus-client has been uninstalled." >&2
    ;;
  patch)
    patch_deephaven_enterprise_proto_package "$venv_path"
    ;;
  *)
    usage
    ;;
esac

exit 0 # Explicitly exit with success
