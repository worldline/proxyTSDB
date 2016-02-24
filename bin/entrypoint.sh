#!/bin/bash

set -e

CMD="python /app/bin/proxyTSDB.py"
ARGS=""

if [ "${1:0:1}" = "-" ]; then
    ARGS="$@"
elif [ -n "$1" ]; then
    CMD="$1"
    shift
    ARGS="$@"
fi

echo Running command: $CMD $ARGS
exec $CMD $ARGS
