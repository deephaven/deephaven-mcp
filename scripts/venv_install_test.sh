#!/bin/bash
#
# Test Enterprise Client Installation in Pip and UV Virtual Environments
#
# This script addresses user-reported issues with enterprise virtual environment
# creation by testing both pip and uv installation workflows.
#
# Usage:
#   ./scripts/test_enterprise_install.sh [OPTIONS]
#
# Options:
#   --wheel-file PATH    Path to wheel file (default: auto-detect from ops/artifacts)
#   --keep-venvs         Keep virtual environments after testing (for debugging)
#   --python PYTHON      Python executable to use (default: python3)
#   --skip-pip           Skip pip venv tests
#   --skip-uv            Skip uv venv tests
#
# Examples:
#   ./scripts/test_enterprise_install.sh
#   ./scripts/test_enterprise_install.sh --keep-venvs
#   ./scripts/test_enterprise_install.sh --wheel-file /path/to/wheel.whl

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
SKIP_PIP=false
SKIP_UV=false
TEST_DIR=""

# Parse arguments
while [[ $# -gt 0 ]]; do
  case $1 in
    --wheel-file)
      WHEEL_FILE="$2"
      shift 2
      ;;
    --keep-venvs)
      KEEP_VENVS=true
      shift
      ;;
    --python)
      PYTHON_EXE="$2"
      shift 2
      ;;
    --skip-pip)
      SKIP_PIP=true
      shift
      ;;
    --skip-uv)
      SKIP_UV=true
      shift
      ;;
    -h|--help)
      head -n 20 "$0" | grep "^#" | sed 's/^# *//'
      exit 0
      ;;
    *)
      echo "Unknown option: $1"
      exit 1
      ;;
  esac
done

# Helper functions
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

# Find wheel file if not specified
if [[ -z "$WHEEL_FILE" ]]; then
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

if [[ ! -f "$WHEEL_FILE" ]]; then
  print_error "Wheel file not found: $WHEEL_FILE"
  exit 1
fi

print_info "Using wheel file: $WHEEL_FILE"
print_info "Wheel size: $(wc -c < "$WHEEL_FILE" | tr -d ' ') bytes"

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

FAILED_TESTS=()

# Test pip installation
test_pip_installation() {
  print_header "Testing PIP Virtual Environment Installation"
  
  local venv_path="$TEST_DIR/venv_pip"
  
  echo "📦 Creating pip virtual environment..."
  if ! "$PYTHON_EXE" -m venv "$venv_path"; then
    print_error "Failed to create pip venv"
    return 1
  fi
  print_success "Pip venv created successfully"
  
  echo "📥 Installing wheel with pip..."
  local pip_exe="$venv_path/bin/pip"
  [[ ! -f "$pip_exe" ]] && pip_exe="$venv_path/Scripts/pip.exe"
  
  if ! "$pip_exe" install --upgrade pip --quiet; then
    print_error "Failed to upgrade pip"
    return 1
  fi
  
  if ! "$pip_exe" install --no-cache-dir --only-binary :all: "$WHEEL_FILE" --quiet; then
    print_error "Failed to install wheel with pip"
    return 1
  fi
  print_success "Wheel installed successfully with pip"
  
  echo "🔍 Verifying installation..."
  local python_exe="$venv_path/bin/python"
  [[ ! -f "$python_exe" ]] && python_exe="$venv_path/Scripts/python.exe"
  
  if ! "$python_exe" -c "import deephaven_enterprise; print(f'Version: {deephaven_enterprise.__version__}')"; then
    print_error "Failed to import deephaven_enterprise"
    return 1
  fi
  
  if ! "$python_exe" -c "from deephaven_enterprise.client import CorePlusSessionManager" 2>/dev/null; then
    print_error "Failed to import CorePlusSessionManager"
    return 1
  fi
  
  print_success "Installation verified successfully"
  return 0
}

# Test uv installation
test_uv_installation() {
  print_header "Testing UV Virtual Environment Installation"
  
  if ! command -v uv &> /dev/null; then
    print_error "uv is not installed (install: curl -LsSf https://astral.sh/uv/install.sh | sh)"
    return 1
  fi
  
  local venv_path="$TEST_DIR/venv_uv"
  
  echo "📦 Creating uv virtual environment..."
  if ! uv venv "$venv_path" --quiet; then
    print_error "Failed to create uv venv"
    return 1
  fi
  print_success "UV venv created successfully"
  
  echo "📥 Installing wheel with uv..."
  local python_exe="$venv_path/bin/python"
  [[ ! -f "$python_exe" ]] && python_exe="$venv_path/Scripts/python.exe"
  
  if ! uv pip install --python "$python_exe" "$WHEEL_FILE" --quiet; then
    print_error "Failed to install wheel with uv"
    return 1
  fi
  print_success "Wheel installed successfully with uv"
  
  echo "🔍 Verifying installation..."
  if ! "$python_exe" -c "import deephaven_enterprise; print(f'Version: {deephaven_enterprise.__version__}')"; then
    print_error "Failed to import deephaven_enterprise"
    return 1
  fi
  
  if ! "$python_exe" -c "from deephaven_enterprise.client import CorePlusSessionManager" 2>/dev/null; then
    print_error "Failed to import CorePlusSessionManager"
    return 1
  fi
  
  print_success "Installation verified successfully"
  return 0
}

# Run tests
if [[ "$SKIP_PIP" == "false" ]]; then
  if ! test_pip_installation; then
    FAILED_TESTS+=("pip")
  fi
fi

if [[ "$SKIP_UV" == "false" ]]; then
  if ! test_uv_installation; then
    FAILED_TESTS+=("uv")
  fi
fi

# Print summary
print_header "TEST SUMMARY"

if [[ "$SKIP_PIP" == "false" ]]; then
  if [[ " ${FAILED_TESTS[*]} " =~ " pip " ]]; then
    print_error "pip installation: FAILED"
  else
    print_success "pip installation: PASSED"
  fi
fi

if [[ "$SKIP_UV" == "false" ]]; then
  if [[ " ${FAILED_TESTS[*]} " =~ " uv " ]]; then
    print_error "uv installation: FAILED"
  else
    print_success "uv installation: PASSED"
  fi
fi

# Exit with appropriate code
if [[ ${#FAILED_TESTS[@]} -gt 0 ]]; then
  echo ""
  print_error "${#FAILED_TESTS[@]} test(s) failed"
  exit 1
else
  echo ""
  print_success "All tests passed!"
  exit 0
fi
