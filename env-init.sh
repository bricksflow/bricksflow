#!/bin/bash -e

function cleanup {
  rv=$?
  rm ./dev_env_init.sh
  exit $rv
}
trap cleanup EXIT

if [[ -z "$ENV_INIT_BRANCH" ]]; then ENV_INIT_BRANCH="master"; fi

echo "dev-env-init branch: $ENV_INIT_BRANCH"

curl "https://raw.githubusercontent.com/bricksflow/dev-env-init/$ENV_INIT_BRANCH/dev_env_init.sh?$(date +%s)" -H 'Cache-Control: no-cache' --silent -o dev_env_init.sh
. "dev_env_init.sh"

prepare_environment_databricks_app
