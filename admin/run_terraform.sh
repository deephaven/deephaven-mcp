#!/bin/bash
#
# This script makes running the deployment terraform easier.
#

set -e

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

cd "${SCRIPT_DIR}/terraform/"

# Load .env file from project root and require INKEEP_API_KEY
if [ ! -f "${SCRIPT_DIR}/../.env" ]; then
  echo "ERROR: .env file not found in project root (${SCRIPT_DIR}/../.env)." >&2
  exit 1
fi

set -a
. "${SCRIPT_DIR}/../.env"
set +a

if [ -z "${INKEEP_API_KEY}" ]; then
  echo "ERROR: INKEEP_API_KEY is not set in .env file (${SCRIPT_DIR}/../.env)." >&2
  exit 1
fi

function init_tf() {
  echo "INIT: ${1}"
  pushd "${1}"
  terraform init
  popd
}

function init_upgrade_tf() {
  echo "INIT: $1"
  pushd "$1"
  terraform init -upgrade
  popd
}

function init_tf_ws() {
  echo "INIT: $1"
  pushd "$1"
  terraform workspace new "${WORKSPACE}"
  popd
}

function apply_tf() {
  echo "APPLY: $@"
  pushd "$1"
  terraform workspace select "${WORKSPACE}"
  terraform ${@:2} -var "inkeep_api_key=${INKEEP_API_KEY}" 
  popd
}

function nuke_workspace() {
  echo "NUKE-WS: $@"
  pushd $1
  # Admin workspace provides a unique admin namespace for deleting real workspaces
  terraform workspace select admin || terraform workspace new admin
  terraform workspace delete ${@:2} ${WORKSPACE} || true
  popd
}

function usage() {
  echo "Usage: <workspace> <command>"
  echo "Commands:"
  echo "    gcloud-init: initialize gcloud (recommend using gcloud-init-auth instead)"
  echo "    gcloud-auth: authenticate to glcoud (recommend using gcloud-init-auth instead)"
  echo "    gcloud-init-auth: initialize and authenticate to gcloud (required upon Codespace creation)"
  echo "    init: initialize terraform"
  echo "    init-upgrade: upgrade and initialize terraform"
  echo "    init-ws: initialize terraform workspace"
  echo "    apply: apply all sub-modules to bring up the entire system"
  echo "    destroy: destroy all sub-modules to bring down the entire system"
  echo "    redeploy-image: redeploy the image to Cloud Run"
  echo "    nuke-workspace: delete the entire workspace (use with caution)"
  echo "    tf-cmd: run a terraform command with the given arguments (use with caution)"
  echo "    artifacts-all-repos-list: list all GCP Artifact Repositories"
  echo "    artifacts-mcp-repo-list: list the images in the GCP Artifact Repository for the mcp"
  echo "    help: this message"
  exit -1
}

# Validate required parameters
if [ -z "${1}" ] || [ -z "${2}" ]; then
  usage
  exit 1
fi

export WORKSPACE=$1
export COMMAND=$2
export TF_VAR_FILE=$(realpath "./workspace-${WORKSPACE}.tfvars")
echo "CONFIGURATION: ${TF_VAR_FILE}"
export TF_CLI_ARGS_plan="-var-file=${TF_VAR_FILE}"
export TF_CLI_ARGS_apply=${TF_CLI_ARGS_plan}
export TF_CLI_ARGS_destroy=${TF_CLI_ARGS_plan}

# Extract values from the workspace file
export PROJECT=$(grep 'project_id' ${TF_VAR_FILE} | cut -d '=' -f2 | tr -d ' "' | xargs)
echo "PROJECT: ${PROJECT}"
export REGION=$(grep 'region' ${TF_VAR_FILE} | cut -d '=' -f2 | tr -d ' "' | xargs)
echo "REGION: ${REGION}"
export IMAGE=$(grep 'image' ${TF_VAR_FILE} | cut -d '=' -f2 | tr -d ' "')
echo "IMAGE: ${IMAGE}"

if [[ "$#" -ne 2 && "${COMMAND}" -ne "tf-cmd" ]]; then
  usage
fi

TF_FLAGS="-auto-approve"
# TF_FLAGS="-auto-approve -compact-warnings"

case $COMMAND in
gcloud-init)
  gcloud init
  ;;
gcloud-auth)
  gcloud auth application-default login
  ;;
gcloud-init-auth)
  gcloud init
  gcloud auth application-default login
  ;;
init)
  init_tf ./mcp-docs
  ;;
init-upgrade)
  init_upgrade_tf ./mcp-docs
  ;;
init-ws)
  init_tf_ws ./mcp-docs
  ;;
apply)
  apply_tf ./mcp-docs apply ${TF_FLAGS}
  ;;
destroy)
  apply_tf ./mcp-docs destroy ${TF_FLAGS}
  ;;
redeploy-image)
  echo "Redeploying image to Cloud Run..."
  echo "Project: ${PROJECT}"
  echo "Region: ${REGION}"
  echo "Image: ${IMAGE}"

  if gcloud run services describe deephaven-mcp-docs-${WORKSPACE} --region=${REGION} --project=${PROJECT}; then
    gcloud run deploy deephaven-mcp-docs-${WORKSPACE} \
      --image=${IMAGE} \
      --project=${PROJECT} \
      --region=${REGION}
  else
    echo "Cloud Run service deephaven-mcp-docs-${WORKSPACE} does not exist."
  fi
  ;;
nuke-workspace)
  read -p "Are you sure you want to nuke the workspace (${WORKSPACE})!?!? [Y|N] " yn
  case $yn in
    Y|y)
      echo "NUKING THE WORKSPACE ($WORKSPACE)..."
      nuke_workspace ./mcp-docs
      ;;
    *)
      echo "ABORTING NUKE..."
      ;;
  esac 
  ;;  
tf-cmd)
  apply_tf ${@:3}
  ;;
artifacts-all-repos-list)
  echo "Listing GCP Artifact Repositories ..."
  gcloud artifacts repositories list --project=${PROJECT}
  ;;  
artifacts-mcp-repo-list)
  REPO_PATH="${REGION}-docker.pkg.dev/${PROJECT}/deephaven-mcp-docs"
  echo "Listing mcp images in GCP Artifact Repository: ${REPO_PATH}"
  gcloud artifacts docker images list ${REPO_PATH}
  ;;
help)
  usage
  ;;
*)
  usage
  ;;
esac