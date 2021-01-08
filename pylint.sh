#!/bin/bash -e

pylint \
--ignore-patterns=".+(Test|_test)\.py" \
--rcfile=.pylintrc \
--ignored-modules=pyspark.sql.functions \
--extension-pkg-whitelist=pyspark \
--generated-members=pyspark.* \
--class-naming-style=PascalCase \
--module-rgx=".+" \
--function-rgx="^.*$" \
--method-rgx="^[_]{0,2}[a-z]+((\d)|([A-Z0-9][a-z0-9]+))*([A-Z])?(__)?$" \
--attr-rgx="^[_]{0,2}[a-z]+((\d)|([A-Z0-9][a-z0-9]+))*([A-Z])?$" \
--argument-rgx="^[a-z]+((\d)|([A-Z0-9][a-z0-9]+))*([A-Z])?$" \
--variable-rgx="^[a-z]+((\d)|([A-Z0-9][a-z0-9]+))*([A-Z])?$" \
--class-attribute-rgx="^[_]{0,2}[a-z]+((\d)|([A-Z0-9][a-z0-9]+))*([A-Z])?$" \
--const-rgx="^[a-z]+((\d)|([A-Z0-9][a-z0-9]+))*([A-Z])?$" \
src
