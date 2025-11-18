#!/bin/bash
#
# Test Enterprise Client Installation in Pip and UV Virtual Environments
#
# This script addresses user-reported issues with enterprise virtual environment
# creation by testing both pip and uv installation workflows. It validates that
# the dev_manage_coreplus_client.sh script correctly installs the enterprise
# client wheel in both standard pip venvs and uv-created venvs.
#
# Usage:
#   ./scripts/venv_install_test.sh [OPTIONS]
#
# Options:
#   Venv Type Selection (default: test both):
#     --pip                     Test only pip venvs
#     --uv                      Test only uv venvs
#
#   Wheel Source Selection (default: archive only):
#     --archive                 Test local wheel file (default: enabled)
#     --latest                  Test installing latest version from GCS
#     --version VERSION         Test installing specific version from GCS
#
#   Other Options:
#     --wheel-file PATH         Path to wheel file (default: auto-detect from ops/artifacts)
#     --python PYTHON           Python executable to use (default: python3)
#     --keep-venvs              Keep venvs after testing (for debugging)
#
# Examples:
#   ./scripts/venv_install_test.sh                          # Test pip+uv with local archive
#   ./scripts/venv_install_test.sh --pip                    # Test pip only with local archive
#   ./scripts/venv_install_test.sh --uv --latest            # Test uv with latest from GCS
#   ./scripts/venv_install_test.sh --pip --uv --version 20240517  # Test both with specific version

set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Default values
WHEEL_FILE=""
KEEP_VENVS=false
PYTHON_EXE="python3"

# Venv type flags (default: test both)
TEST_PIP=false
TEST_UV=false

# Wheel source flags (default: archive only)
TEST_ARCHIVE=false
TEST_LATEST=false
TEST_VERSION=""

TEST_DIR=""

# Test tracking (initialized in setup_test_environment)
FAILED_TESTS=()
RUN_TESTS=()

# Parse arguments
while [[ $# -gt 0 ]]; do
  case $1 in
    --wheel-file)
      if [[ -z "${2:-}" ]]; then
        echo "Error: --wheel-file requires an argument" >&2
        exit 1
      fi
      WHEEL_FILE="$2"
      shift 2
      ;;
    --keep-venvs)
      KEEP_VENVS=true
      shift
      ;;
    --python)
      if [[ -z "${2:-}" ]]; then
        echo "Error: --python requires an argument" >&2
        exit 1
      fi
      PYTHON_EXE="$2"
      shift 2
      ;;
    --pip)
      TEST_PIP=true
      shift
      ;;
    --uv)
      TEST_UV=true
      shift
      ;;
    --archive)
      TEST_ARCHIVE=true
      shift
      ;;
    --latest)
      TEST_LATEST=true
      shift
      ;;
    --version)
      if [[ -z "${2:-}" ]]; then
        echo "Error: --version requires an argument" >&2
        exit 1
      fi
      TEST_VERSION="$2"
      shift 2
      ;;
    -h|--help)
      awk 'NR==1{next} /^#!/{next} /^#/{sub(/^# ?/, ""); print; next} {exit}' "$0"
      exit 0
      ;;
    *)
      echo "Unknown option: $1"
      exit 1
      ;;
  esac
done

# Apply defaults if nothing specified
if [[ "$TEST_PIP" == "false" ]] && [[ "$TEST_UV" == "false" ]]; then
  TEST_PIP=true
  TEST_UV=true
fi

if [[ "$TEST_ARCHIVE" == "false" ]] && [[ "$TEST_LATEST" == "false" ]] && [[ -z "$TEST_VERSION" ]]; then
  TEST_ARCHIVE=true
fi

# Validate Python executable
if ! command -v "$PYTHON_EXE" &>/dev/null; then
  echo "Error: Python executable not found: $PYTHON_EXE" >&2
  exit 1
fi

# =============================================================================
# Helper Functions
# =============================================================================

print_header() {
  echo ""
  echo "================================================================================"
  echo "$1"
  echo "================================================================================"
}

print_success() {
  echo -e "${GREEN}✅ $1${NC}"
}

print_error() {
  echo -e "${RED}❌ $1${NC}"
}

print_info() {
  echo -e "${YELLOW}ℹ️  $1${NC}"
}

# Get python executable from venv (supports both Unix and Windows paths)
get_venv_python() {
  local venv_path="$1"
  local python_exe="$venv_path/bin/python"
  
  if [[ ! -f "$python_exe" ]]; then
    python_exe="$venv_path/Scripts/python.exe"
  fi
  
  if [[ ! -f "$python_exe" ]]; then
    print_error "Could not find python executable in venv at $venv_path"
    return 1
  fi
  
  echo "$python_exe"
}

# Find wheel file and set global WHEEL_FILE variable
# Side effect: Sets/validates global WHEEL_FILE
# Usage: find_wheel_file
find_wheel_file() {
  if [[ -n "$WHEEL_FILE" ]]; then
    # User provided a wheel file path
    if [[ ! -f "$WHEEL_FILE" ]]; then
      print_error "Wheel file not found: $WHEEL_FILE"
      exit 1
    fi
  else
    # Auto-detect wheel file in artifacts directory
    SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    REPO_ROOT="$(dirname "$SCRIPT_DIR")"
    ARTIFACTS_DIR="$REPO_ROOT/ops/artifacts"
    
    if [[ ! -d "$ARTIFACTS_DIR" ]]; then
      print_error "Artifacts directory not found: $ARTIFACTS_DIR"
      exit 1
    fi
    
    WHEEL_FILE=$(find "$ARTIFACTS_DIR" -name "deephaven_coreplus_client*.whl" -print -quit)
    
    if [[ -z "$WHEEL_FILE" ]]; then
      print_error "No wheel file found in $ARTIFACTS_DIR"
      exit 1
    fi
  fi

  print_info "Using wheel file: $WHEEL_FILE"
  print_info "Wheel size: $(wc -c < "$WHEEL_FILE" | tr -d ' ') bytes"
}

# =============================================================================
# Core Functions (in usage order: create, then install)
# =============================================================================

# Generate test label for a venv type and source combination
# Usage: get_test_label <venv_type> <source>
get_test_label() {
  local venv_type="$1"
  local source="$2"
  
  if [[ "$source" == "version" ]]; then
    echo "${venv_type}+v${TEST_VERSION}"
  else
    echo "${venv_type}+${source}"
  fi
}

# Create venv (pip or uv)
# Usage: create_venv <type> <path>
create_venv() {
  local venv_type="$1"
  local venv_path="$2"
  
  case "$venv_type" in
    pip)
      echo "📦 Creating pip virtual environment..."
      if ! "$PYTHON_EXE" -m venv "$venv_path"; then
        print_error "Failed to create pip venv"
        return 1
      fi
      print_success "Pip venv created successfully"
      ;;
    uv)
      if ! command -v uv &> /dev/null; then
        print_error "uv is not installed (install: curl -LsSf https://astral.sh/uv/install.sh | sh)"
        return 1
      fi
      echo "📦 Creating uv virtual environment..."
      if ! uv venv "$venv_path"; then
        print_error "Failed to create uv venv"
        return 1
      fi
      print_success "UV venv created successfully"
      ;;
    *)
      print_error "Unknown venv type: $venv_type"
      return 1
      ;;
  esac
}

# Install and verify enterprise client
# Usage: install_and_verify <venv_path> <source_type> [source_arg]
install_and_verify() {
  local venv_path="$1"
  local source_type="$2"
  local source_arg="${3:-}"
  
  case "$source_type" in
    archive)
      echo "📥 Installing from archive: $source_arg"
      if ! ./bin/dev_manage_coreplus_client.sh install-wheel --venv "$venv_path" --wheel-file "$source_arg"; then
        print_error "Failed to install from archive"
        return 1
      fi
      print_success "Archive installed successfully"
      ;;
    latest)
      echo "📥 Installing latest version from GCS"
      if ! ./bin/dev_manage_coreplus_client.sh install --venv "$venv_path"; then
        print_error "Failed to install latest version"
        return 1
      fi
      print_success "Latest version installed successfully"
      ;;
    version)
      echo "📥 Installing version $source_arg from GCS"
      if ! ./bin/dev_manage_coreplus_client.sh install --venv "$venv_path" --ev "$source_arg"; then
        print_error "Failed to install version $source_arg"
        return 1
      fi
      print_success "Version $source_arg installed successfully"
      ;;
    *)
      print_error "Unknown source type: $source_type"
      return 1
      ;;
  esac
  
  # Verify installation
  echo "🔍 Verifying installation..."
  local python_exe
  python_exe=$(get_venv_python "$venv_path") || return 1
  
  if ! "$python_exe" -c "import deephaven_enterprise" 2>/dev/null; then
    print_error "Failed to import deephaven_enterprise"
    return 1
  fi
  print_success "Imported deephaven_enterprise"
  
  if ! "$python_exe" -c "from deephaven_enterprise.client.session_manager import SessionManager" 2>/dev/null; then
    print_error "Failed to import SessionManager"
    return 1
  fi
  print_success "Imported SessionManager"
  
  print_success "Installation verified successfully"
  return 0
}

# =============================================================================
# Test Orchestration
# =============================================================================

# Run a single test: create venv + install and verify enterprise
# Usage: run_single_test <venv_type> <source_type> <source_arg> <test_label>
run_single_test() {
  local venv_type="$1"
  local source_type="$2"
  local source_arg="$3"
  local test_label="$4"
  
  print_header "Test: ${venv_type} + ${source_type} ${source_arg}"
  local venv_path="$TEST_DIR/venv_${venv_type}_${source_type}_${RANDOM}_$$"
  
  RUN_TESTS+=("${test_label}")
  
  if create_venv "$venv_type" "$venv_path" && install_and_verify "$venv_path" "$source_type" "$source_arg"; then
    return 0
  else
    FAILED_TESTS+=("${test_label}")
    return 1
  fi
}

# Run test grid: venv_type × wheel_source
run_test_grid() {
  local venv_types=()
  [[ "$TEST_PIP" == "true" ]] && venv_types+=("pip")
  [[ "$TEST_UV" == "true" ]] && venv_types+=("uv")
  
  # Find wheel file if archive testing is enabled
  if [[ "$TEST_ARCHIVE" == "true" ]]; then
    find_wheel_file
  fi
  
  # Double loop: venv_type × source_type
  for venv_type in "${venv_types[@]}"; do
    if [[ "$TEST_ARCHIVE" == "true" ]]; then
      run_single_test "$venv_type" "archive" "$WHEEL_FILE" "$(get_test_label "$venv_type" "archive")"
      echo ""
    fi
    
    if [[ "$TEST_LATEST" == "true" ]]; then
      run_single_test "$venv_type" "latest" "" "$(get_test_label "$venv_type" "latest")"
      echo ""
    fi
    
    if [[ -n "$TEST_VERSION" ]]; then
      run_single_test "$venv_type" "version" "$TEST_VERSION" "$(get_test_label "$venv_type" "version")"
      echo ""
    fi
  done
}

# Print test summary
print_summary() {
  print_header "TEST SUMMARY"
  
  # Build list of all possible test combinations
  local all_venv_types=("pip" "uv")
  local all_sources=()
  [[ "$TEST_ARCHIVE" == "true" ]] && all_sources+=("archive")
  [[ "$TEST_LATEST" == "true" ]] && all_sources+=("latest")
  [[ -n "$TEST_VERSION" ]] && all_sources+=("version")
  
  # Show status for each possible combination
  for venv_type in "${all_venv_types[@]}"; do
    local venv_enabled=false
    [[ "$venv_type" == "pip" && "$TEST_PIP" == "true" ]] && venv_enabled=true
    [[ "$venv_type" == "uv" && "$TEST_UV" == "true" ]] && venv_enabled=true
    
    for source in "${all_sources[@]}"; do
      local test_label
      test_label=$(get_test_label "$venv_type" "$source")
      
      if [[ "$venv_enabled" == "false" ]]; then
        echo "  ${venv_type} + ${source}: SKIPPED"
      elif [[ " ${FAILED_TESTS[@]+"${FAILED_TESTS[@]}"} " =~ " ${test_label} " ]]; then
        print_error "${test_label}: FAILED"
      else
        print_success "${test_label}: PASSED"
      fi
    done
  done
  
  echo ""
  local ran=${#RUN_TESTS[@]}
  local failed=${#FAILED_TESTS[@]}
  local passed=$((ran - failed))
  
  if [[ $failed -gt 0 ]]; then
    print_error "$failed failed, $passed passed, $ran total"
    return 1
  else
    print_success "All $ran test(s) passed!"
    return 0
  fi
}

# =============================================================================
# Main Execution
# =============================================================================

# Setup test environment
setup_test_environment() {
  # Verify we're in the repo root
  if [[ ! -f "./bin/dev_manage_coreplus_client.sh" ]]; then
    print_error "Must run from repository root directory"
    exit 1
  fi
  
  # Create test directory
  if [[ "$KEEP_VENVS" == "true" ]]; then
    TEST_DIR="./test_venvs"
    mkdir -p "$TEST_DIR"
    print_info "Using test directory: $TEST_DIR (will be kept)"
  else
    TEST_DIR=$(mktemp -d -t test_venvs_XXXXXX)
    print_info "Using temporary directory: $TEST_DIR (will be deleted)"
    trap 'rm -rf "$TEST_DIR"' EXIT
  fi

  # Reset test tracking arrays
  FAILED_TESTS=()
  RUN_TESTS=()
}

setup_test_environment
run_test_grid
print_summary
exit $?
