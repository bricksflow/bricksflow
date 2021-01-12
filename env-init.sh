#!/bin/bash -e

PENVY_VERSION="1.0.6"
BENVY_VERSION="1.0.1"
POSSIBLE_PATHS_LIST_URL="https://raw.githubusercontent.com/pyfony/penvy/master/src/penvy/conda/conda_executable_paths.txt"

resolve_conda_executable_path() {
  if hash conda 2>/dev/null; then
    echo "Using conda from PATH"

    CONDA_EXECUTABLE_PATH="conda"
  else
    POSSIBLE_PATHS_LIST=$(curl --silent $POSSIBLE_PATHS_LIST_URL)

    while IFS= read -r line; do
      FILE_PATH=$(sed -E "s|\~|$HOME|g" <<< $line)

      if [ -f "$FILE_PATH" ]; then
        echo "Using conda from: $FILE_PATH"
        CONDA_EXECUTABLE_PATH="$FILE_PATH"
        break
      fi
    done <<< "$POSSIBLE_PATHS_LIST"

    if [ -z ${CONDA_EXECUTABLE_PATH+x} ]; then
      echo "Unable to resolve Conda executable path"
      exit 1
    fi
  fi
}

resolve_conda_executable_path

CONDA_BASE_DIR=$($CONDA_EXECUTABLE_PATH info --base | sed 's/\\/\//g')

if [ -d "$CONDA_BASE_DIR/Scripts" ]; then
  CONDA_BIN_DIR="$CONDA_BASE_DIR/Scripts" # Windows
else
  CONDA_BIN_DIR="$CONDA_BASE_DIR/bin" # Linux/Mac
fi

$CONDA_BIN_DIR/pip install "penvy==$PENVY_VERSION"
$CONDA_BIN_DIR/pip install "benvy==$BENVY_VERSION"
$CONDA_BIN_DIR/benvy-init "$@"
