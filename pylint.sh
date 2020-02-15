#!/bin/bash -e

echo "Checking modules and classes"

pylint \
--ignore-patterns=".+(Test|\.(job|consumer))\.py" \
--rcfile=.pylintrc \
--ignored-modules=pyspark.sql.functions \
--extension-pkg-whitelist=pyspark \
--generated-members=pyspark.* \
--class-naming-style=PascalCase \
--module-rgx=".+" \
--function-rgx="^[a-z]+((\d)|([A-Z0-9][a-z0-9]+))*([A-Z])?$" \
--method-rgx="^[_]{0,2}[a-z]+((\d)|([A-Z0-9][a-z0-9]+))*([A-Z])?(__)?$" \
--attr-rgx="^[_]{0,2}[a-z]+((\d)|([A-Z0-9][a-z0-9]+))*([A-Z])?$" \
--argument-rgx="^[a-z]+((\d)|([A-Z0-9][a-z0-9]+))*([A-Z])?$" \
--variable-rgx="^[a-z]+((\d)|([A-Z0-9][a-z0-9]+))*([A-Z])?$" \
--class-attribute-rgx="^[_]{0,2}[a-z]+((\d)|([A-Z0-9][a-z0-9]+))*([A-Z])?$" \
--const-rgx="^[a-z]+((\d)|([A-Z0-9][a-z0-9]+))*([A-Z])?$" \
src

echo "Checking jobs and consumers"

find src -iname "*.job.py" -o -iname "*.consumer.py" | xargs pylint --disable=unused-import --rcfile=.pylintrc \
--ignored-modules=pyspark.sql.functions \
--extension-pkg-whitelist=pyspark \
--generated-members=pyspark.* \
--class-naming-style=PascalCase \
--module-rgx="^(consumer|job)$" \
--function-rgx="^[a-z]+((\d)|([A-Z0-9][a-z0-9]+))*([A-Z])?$" \
--method-rgx="^[_]{0,2}[a-z]+((\d)|([A-Z0-9][a-z0-9]+))*([A-Z])?(__)?$" \
--attr-rgx="^[_]{0,2}[a-z]+((\d)|([A-Z0-9][a-z0-9]+))*([A-Z])?$" \
--argument-rgx="^[a-z]+((\d)|([A-Z0-9][a-z0-9]+))*([A-Z])?$" \
--variable-rgx="^[a-z]+((\d)|([A-Z0-9][a-z0-9]+))*([A-Z])?$" \
--class-attribute-rgx="^[_]{0,2}[a-z]+((\d)|([A-Z0-9][a-z0-9]+))*([A-Z])?$" \
--const-rgx="^[a-z]+((\d)|([A-Z0-9][a-z0-9]+))*([A-Z])?$"
