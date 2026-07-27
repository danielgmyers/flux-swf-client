#!/bin/bash

MODULES=$(scripts/find-modules-to-publish.sh)
if [ ! $? -eq 0 ]; then
  echo "No unpublished module versions found, skipping mvn execution."
  exit 0
fi

mvn -pl "$MODULES" -am "$@"
