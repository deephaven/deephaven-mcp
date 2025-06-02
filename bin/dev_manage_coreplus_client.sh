#!/bin/bash

# Manages Deephaven CorePlus client installation and version discovery from GCS.
#
# Key features:
# - Installs a specific or the latest available deephaven_coreplus_client:
#   Downloads the CorePlus tarball, extracts it, and installs the client wheel using 'uv'.
# - Lists available Enterprise Versions, Point Releases, and Community Versions from GCS.
# - Determines and displays the latest consistent set of EV, PR, and CV.
#
# For detailed command usage, run: ./dev_manage_coreplus_client.sh help

# Strict mode
set -euo pipefail
# set -x # Uncomment for debugging

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
GCS_BUCKET_PATH="gs://illumon-software-repo/jenkins/jdk17/release"
DEFAULT_CLIENT_WHEEL_PATTERN="deephaven_coreplus_client*.whl"

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
  echo "Usage: $0 [command]"
  echo
  echo "Commands:"
  echo "  list-enterprise-versions                               List available enterprise versions."
  echo "  list-point-releases [<EV>]             List available point releases. Defaults to latest enterprise version if not specified."
  echo "  list-community-versions [<EV> [<PR>]]                List available community versions. Defaults to latest EV and latest PR for that EV."
  echo "  list-latest-versions                                   List the latest enterprise version and its corresponding latest point release and community version."
  echo "  install [--ev <EV>] [--pr <PR>] [--cv <CV>]            Install the client wheel."
  echo "                                                         If versions are not provided, the latest available will be used."
  echo "                                                         EV: Enterprise Version (e.g., 20240517)"
  echo "                                                         PR: Enterprise Point Release (e.g., 483)"
  echo "                                                         CV: Community Version (e.g., 0.39.1)"
  echo
  echo "Examples:"
  echo "  $0 list-enterprise-versions"
  echo "  $0 list-point-releases 20240517"
  echo "  $0 install --ev 20240517 --pr 483 --cv 0.39.1"
  echo "  $0 install # Installs the latest versions"
  echo "  $0 list-latest-versions # Lists the latest EV and its corresponding latest PR and CV"
  exit 1
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
        uv)     install_hint="Please install uv (see https://astral.sh/uv/install)." ;;
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

# --- Listing Functions ---

# List available enterprise versions (directories under GCS_BUCKET_PATH)
fn_list_enterprise_versions() {
  check_tools "gsutil"
  local raw_list
  raw_list=$(run_gsutil ls -d "${GCS_BUCKET_PATH}/*")
  if [ $? -ne 0 ]; then return 1; fi # run_gsutil already printed a detailed error

  if [ -z "$raw_list" ]; then
    # This means no directories found, not necessarily a gsutil error itself.
    # Depending on context, this might be an error or just an empty list.
    # For internal calls (like get_latest), an empty output is expected to be handled.
    return 1 # Or simply echo nothing and let caller decide
  fi

  echo "$raw_list" | sed "s|${GCS_BUCKET_PATH}/||; s|/||" | grep -E '^[0-9]+$' | "${SORT_CMD_V_BASE}" -V || return 1 # Extract dir name, filter for numeric (version-like) names, and version sort
}

# Extract tarball names for a given enterprise version
get_tarball_filenames_for_ev() {
  check_tools "gsutil"
  local enterprise_version=$1
  local raw_list
  raw_list=$(run_gsutil ls "${GCS_BUCKET_PATH}/${enterprise_version}/deephaven-coreplus-*-1.${enterprise_version}.*-jdk17.tgz")
  if [ $? -ne 0 ]; then return 1; fi
  if [ -z "$raw_list" ]; then return 1; fi # No matching tarballs
  echo "$raw_list" | xargs -n1 basename || return 1 # Extract just the filename from each GCS path
}

# Function to list point releases for a given enterprise version (defaults to latest EV if not specified)
fn_list_point_releases() {
  check_tools "gsutil"
  local enterprise_version="$1"
  local using_latest_ev=false

  if [ -z "$enterprise_version" ]; then
    echo "No enterprise version specified, attempting to use the latest..." >&2
    enterprise_version=$(get_latest_enterprise_version)
    if [ -z "$enterprise_version" ]; then
      echo "ERROR: Could not determine the latest enterprise version." >&2
      return 1
    fi
    using_latest_ev=true
    echo "Using latest Enterprise Version: ${enterprise_version}" >&2
  fi

  echo "Available point releases for Enterprise Version ${enterprise_version}:" >&2 # Outputting only data
  
  local all_prs
  all_prs=$(get_tarball_filenames_for_ev "$enterprise_version" |
    sed -n "s/deephaven-coreplus-.*-1\\.${enterprise_version}\\.//; s/-jdk17\\.tgz//p" | # Extract point release from tarball name
    sort -u)

  if [ -z "$all_prs" ]; then
    # echo "No point releases found for EV ${enterprise_version} or error listing them." >&2
    return 1
  fi

  # Separate numeric and alphanumeric PRs
  local numeric_prs=$(echo "$all_prs" | grep -E '^[0-9]+$' | sort -n || true)
  local alphanumeric_prs=$(echo "$all_prs" | grep -vE '^[0-9]+$' | sort || true)

  if [ -n "$numeric_prs" ]; then
    echo "$numeric_prs"
  fi
  if [ -n "$alphanumeric_prs" ]; then
    echo "$alphanumeric_prs"
  fi
}

# Function to list community versions for a given EV and PR (defaults to latest EV and/or latest PR if not specified)
fn_list_community_versions() {
  check_tools "gsutil"
  local ev_arg="$1"
  local pr_arg="$2"
  local ev_to_use
  local pr_to_use

  if [ -z "$ev_arg" ]; then
    echo "No enterprise version specified, attempting to use the latest..." >&2
    ev_to_use=$(get_latest_enterprise_version)
    if [ -z "$ev_to_use" ]; then echo "ERROR: Could not determine the latest enterprise version." >&2; return 1; fi
    echo "Using latest Enterprise Version: ${ev_to_use}" >&2
  else
    ev_to_use="$ev_arg"
  fi

  if [ -z "$pr_arg" ]; then
    echo "No point release specified for EV ${ev_to_use}, attempting to use the latest..." >&2
    pr_to_use=$(get_latest_point_release "$ev_to_use")
    if [ -z "$pr_to_use" ]; then echo "ERROR: Could not determine the latest point release for EV ${ev_to_use}." >&2; return 1; fi
    echo "Using latest Point Release for EV ${ev_to_use}: ${pr_to_use}" >&2
  else
    pr_to_use="$pr_arg"
  fi

  echo "Available community versions for Enterprise Version ${ev_to_use}, Point Release ${pr_to_use}:" >&2
  local raw_list
  raw_list=$(run_gsutil ls "${GCS_BUCKET_PATH}/${ev_to_use}/deephaven-coreplus-*-1.${ev_to_use}.${pr_to_use}-jdk17.tgz")
  if [ $? -ne 0 ]; then return 1; fi
  if [ -z "$raw_list" ]; then
    # echo "No community versions found for EV ${ev_to_use} and PR ${pr_to_use} or error listing them." >&2
    return 1 # No matching tarballs
  fi
  echo "$raw_list" | \
    xargs -n1 basename | \
    # Extract community version from tarball name
    sed -n "s/deephaven-coreplus-//; s/-1\\.${ev_to_use}\\.${pr_to_use}-jdk17\\.tgz//p" | \
    "${SORT_CMD_V_BASE}" -uV || return 1 # This final return 1 is if sed/sort fails, not gsutil
}

# Retrieves the latest enterprise version by listing and taking the last sorted one.
get_latest_enterprise_version() {
  fn_list_enterprise_versions | tail -n 1
}

# Retrieves the latest point release for a given enterprise version.
# Expects enterprise_version as $1.
get_latest_point_release() {
  local enterprise_version=$1
  fn_list_point_releases "$enterprise_version" | tail -n 1
}

# Function to list point releases for a given enterprise version AND community version
fn_list_point_releases_for_ev_and_cv() {
  check_tools "gsutil"
  local enterprise_version="$1"
  local community_version="$2"

  local all_prs
  all_prs=$(get_tarball_filenames_for_ev "$enterprise_version" | \
    grep "deephaven-coreplus-${community_version}-1\\.${enterprise_version}\\." | \ # Filter tarballs matching the specific CV and EV
    sed -n "s/deephaven-coreplus-${community_version}-1\\.${enterprise_version}\\.//; s/-jdk17\\.tgz//p" | \ # Extract point release from filtered tarball names
    sort -u)

  if [ -z "$all_prs" ]; then
    # echo "No point releases found for EV ${enterprise_version} and CV ${community_version} or error listing them." >&2
    return 1
  fi

  # Separate numeric and alphanumeric PRs
  local numeric_prs=$(echo "$all_prs" | grep -E '^[0-9]+$' | sort -n || true)
  local alphanumeric_prs=$(echo "$all_prs" | grep -vE '^[0-9]+$' | sort || true)

  if [ -n "$numeric_prs" ]; then
    echo "$numeric_prs"
  fi
  if [ -n "$alphanumeric_prs" ]; then
    echo "$alphanumeric_prs"
  fi
}

# Function to get the latest point release for a given enterprise version AND community version
get_latest_point_release_for_ev_and_cv() {
  local enterprise_version="$1"
  local community_version="$2"
  fn_list_point_releases_for_ev_and_cv "$enterprise_version" "$community_version" | tail -n 1
}

# Retrieves the latest community version for a given enterprise version and point release.
# Expects enterprise_version as $1 and point_release as $2.
get_latest_community_version() {
  local enterprise_version="$1"
  local point_release="$2"
  # This function now needs EV and PR to be meaningful in the new context.
  # It's called by fn_list_latest_versions, which will provide both.
  if [ -z "$enterprise_version" ] || [ -z "$point_release" ]; then
    echo "ERROR (internal): get_latest_community_version requires EV and PR." >&2
    return 1
  fi
  fn_list_community_versions "$enterprise_version" "$point_release" | tail -n 1
}


# --- Function to List Latest Full Version Info ---
fn_list_latest_versions() {
  check_tools "gsutil"
  echo "Determining latest consistent set of versions..." >&2
  local latest_ev
  latest_ev=$(get_latest_enterprise_version)
  if [ -z "$latest_ev" ]; then echo "ERROR: Could not determine the latest enterprise version." >&2; return 1; fi

  local latest_pr_for_ev
  latest_pr_for_ev=$(get_latest_point_release "$latest_ev") # Gets latest PR for the EV, across all CVs
  if [ -z "$latest_pr_for_ev" ]; then echo "ERROR: Could not determine the latest point release for EV ${latest_ev}." >&2; return 1; fi

  local latest_cv_for_ev_pr
  latest_cv_for_ev_pr=$(get_latest_community_version "$latest_ev" "$latest_pr_for_ev") # Gets latest CV for this specific EV & PR
  if [ -z "$latest_cv_for_ev_pr" ]; then
    echo "ERROR: Could not determine the latest community version for EV ${latest_ev} and PR ${latest_pr_for_ev}." >&2
    echo "This might indicate no tarballs exist for this specific EV/PR combination, or an issue with GCS listing." >&2
    return 1
  fi

  echo "Latest consistent versions found:"
  echo "  Enterprise Version: ${latest_ev}"
  echo "  Point Release:      ${latest_pr_for_ev}"
  echo "  Community Version:  ${latest_cv_for_ev_pr}"
  echo "(These versions form a consistent set from available tarballs)"
}


# --- Installation Function ---
fn_install_wheel() {
  check_tools "gsutil" "uv" "find"
  
  local enterprise_version_arg=$1
  local point_release_arg=$2
  local community_version_arg=$3

  echo "Determining versions for installation..." >&2

  local enterprise_version=${enterprise_version_arg}
  if [ -z "$enterprise_version" ]; then
    echo "Enterprise version not provided, attempting to find the latest..." >&2
    enterprise_version=$(get_latest_enterprise_version)
    if [ -z "$enterprise_version" ]; then echo "ERROR: Could not determine the latest enterprise version." >&2; exit 1; fi
    echo "Using latest enterprise version: ${enterprise_version}" >&2
  fi

  local point_release=${point_release_arg}
  if [ -z "$point_release" ]; then
    echo "Point release not provided for EV ${enterprise_version}, attempting to find the latest..." >&2
    point_release=$(get_latest_point_release "$enterprise_version")
    if [ -z "$point_release" ]; then echo "ERROR: Could not determine the latest point release for EV ${enterprise_version}." >&2; exit 1; fi
    echo "Using latest point release for EV ${enterprise_version}: ${point_release}" >&2
  fi

  local community_version=${community_version_arg}
  if [ -z "$community_version" ]; then
    echo "Community version not provided for EV ${enterprise_version} and PR ${point_release}, attempting to find the latest..." >&2
    community_version=$(get_latest_community_version "$enterprise_version" "$point_release")
    if [ -z "$community_version" ]; then echo "ERROR: Could not determine the latest community version for EV ${enterprise_version} and PR ${point_release}." >&2; exit 1; fi
    echo "Using latest community version for EV ${enterprise_version}, PR ${point_release}: ${community_version}" >&2
  fi

  echo "--- Selected for installation --- " >&2
  echo "  Enterprise Version:     ${enterprise_version}" >&2
  echo "  Enterprise Point Release: ${point_release}" >&2
  echo "  Community Version:        ${community_version}" >&2
  echo "------------------------------- " >&2

  local coreplus_tarball_name="deephaven-coreplus-${community_version}-1.${enterprise_version}.${point_release}-jdk17.tgz"
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

  echo "Installing ${client_wheel_path} using uv..." >&2

  # --- Start of grpcio override logic ---
  # This section handles a specific dependency constraint for 'grpcio'.
  # The client wheel being installed might have its own 'grpcio' version requirements
  # that could conflict with the 'grpcio' version already in use by other critical
  # packages in the environment (e.g., pydeephaven).
  # To prevent 'uv' from upgrading or downgrading the existing 'grpcio', potentially
  # breaking other parts of the system, we explicitly tell 'uv' to use the currently
  # installed version of 'grpcio'.
  #
  # The 'uv pip install --override' command expects a file path as its argument,
  # where the file contains the package==version specifiers.
  # Instead of creating, writing to, and then deleting a temporary file,
  # we use Bash's process substitution feature: <(echo "grpcio==${current_grpcio_version}").
  # Bash executes the 'echo' command and provides its output via a special file
  # descriptor (e.g., /dev/fd/63) that 'uv' can read like a file. This avoids
  # manual temporary file management and keeps the script cleaner, provided 'uv'
  # correctly handles reading from such file descriptors.

  local current_grpcio_version
  local cmd_args=()

  # Try to determine the currently installed grpcio version and clean it
  echo "Checking for existing grpcio installation..." >&2
  current_grpcio_version=$( (uv pip show grpcio || true) 2>/dev/null | awk '/^Version:/ {print $2}' | tr -d '[:space:]' )

  if [ -n "${current_grpcio_version}" ]; then
    echo "Found existing grpcio version: '${current_grpcio_version}'. Will attempt to override grpcio to this version using process substitution." >&2
    # For debugging, the content of the process substitution can be imagined as: echo "grpcio==${current_grpcio_version}"
    # Bash replaces <(echo ...) with a file descriptor path like /dev/fd/XX
    cmd_args+=(--override <(echo "grpcio==${current_grpcio_version}"))
  else
    echo "No existing grpcio installation found. grpcio will be installed based on wheel's dependencies if required." >&2
  fi
  # --- End of grpcio override logic ---

  # Add other standard options and the client wheel path
  # --only-binary :all: is used to ensure that only pre-compiled binary wheels are
  # installed for the client wheel and its dependencies. This avoids potential
  # build issues, particularly with packages like 'grpcio' which can be problematic
  # to build from source on some platforms/architectures.
  # --no-cache-dir is used to avoid issues with uv's caching mechanisms if needed.
  cmd_args+=(--no-cache-dir --only-binary :all: "${client_wheel_path}")

  echo "Installing wheel using uv: uv pip install ${cmd_args[@]}" >&2
  # Ensure cmd_args is expanded correctly.
  # Note: Process substitution <(...) happens before the command is executed by the shell.
  # The actual value in cmd_args will be the /dev/fd/XX path.
  if ! uv pip install "${cmd_args[@]}"; then # --only-binary :all: ensures only wheels; --no-cache-dir: avoid uv cache; override for grpcio if found
    echo "ERROR: Failed to install ${client_wheel_path} using uv." >&2
    cd "${original_dir}"; exit 1 # Return to original directory and exit function due to uv install failure
  fi
  echo "Installation of ${client_wheel_path} successful." >&2

  cd "${original_dir}"
  echo "Returned to directory $(pwd)" >&2
  echo "Script finished successfully." >&2
}


# --- Main Script Logic ---

if [ $# -eq 0 ]; then usage; fi

COMMAND=$1
shift

case "$COMMAND" in
  list-enterprise-versions)
    fn_list_enterprise_versions | sed 's/^/  /' # Add indentation for direct output
    ;;
  list-point-releases)
    fn_list_point_releases "${1:-}" | sed 's/^/  /' # Functions now handle missing EV and messaging
    ;;
  list-community-versions)
    fn_list_community_versions "${1:-}" "${2:-}" | sed 's/^/  /' # Function now handles missing EV/PR and messaging
    ;;
  install)
    INSTALL_EV=""
    INSTALL_PR=""
    INSTALL_CV=""
    # Parse options for the install command (--ev, --pr, --cv)
    while [ $# -gt 0 ]; do
      local current_opt="$1"
      case "$current_opt" in
        --ev)
          if [[ -z "${2:-}" || "${2:0:2}" == "--" ]]; then
            echo "ERROR: --ev option requires a value (got: '${2:-}')." >&2; usage
          fi
          INSTALL_EV="$2"; shift 2 ;;
        --pr)
          if [[ -z "${2:-}" || "${2:0:2}" == "--" ]]; then
            echo "ERROR: --pr option requires a value (got: '${2:-}')." >&2; usage
          fi
          INSTALL_PR="$2"; shift 2 ;;
        --cv)
          if [[ -z "${2:-}" || "${2:0:2}" == "--" ]]; then
            echo "ERROR: --cv option requires a value (got: '${2:-}')." >&2; usage
          fi
          INSTALL_CV="$2"; shift 2 ;;
        *)
          echo "ERROR: Unknown option for install: $current_opt" >&2; usage ;;
      esac
    done
    fn_install_wheel "$INSTALL_EV" "$INSTALL_PR" "$INSTALL_CV"
    ;;
  list-latest-versions)
    fn_list_latest_versions
    ;;
  *)
    echo "ERROR: Unknown command: $COMMAND" >&2
    usage
    ;;
esac

exit 0 # Explicitly exit with success
