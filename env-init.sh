#!/bin/bash -e

function cleanup {
  rv=$?
  rm ./env-init-functions.sh
  exit $rv
}
trap cleanup EXIT

curl "https://raw.githubusercontent.com/DataSentics/dev-env-init/master/env-init-functions.sh?$(date +%s)" -H 'Cache-Control: no-cache' --silent -o env-init-functions.sh
. "env-init-functions.sh"

prepare_environment_databricks_app
