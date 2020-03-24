#!/bin/bash -e

function cleanup {
  rv=$?
  rm ./env-init-functions.sh
  exit $rv
}
trap cleanup EXIT

if [[ -z "$ENV_INIT_BRANCH" ]]; then ENV_INIT_BRANCH="master"; fi

echo "dev-env-init branch: $ENV_INIT_BRANCH"

curl "https://raw.githubusercontent.com/DataSentics/dev-env-init/$ENV_INIT_BRANCH/env-init-functions.sh?$(date +%s)" -H 'Cache-Control: no-cache' --silent -o env-init-functions.sh
. "env-init-functions.sh"

prepare_environment_databricks_app
