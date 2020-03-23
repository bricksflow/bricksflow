#!/bin/bash

SRC_FILES_CRLF=`find src -not -type d -exec file "{}" ";" | grep CRLF`
ROOT_FILES_CRLF=`find . -maxdepth 1 -not -type d -and -not -name "poetry.lock" -and -not -name "pyproject.toml" -exec file "{}" ";" | grep CRLF`

if [ ! -z "$SRC_FILES_CRLF" ] || [ ! -z "$ROOT_FILES_CRLF" ]; then
  echo "The following files contain CRLF line endings:"
  echo $SRC_FILES_CRLF
  echo $ROOT_FILES_CRLF
  exit 1
else
  echo "No CRLF problems found"
fi
