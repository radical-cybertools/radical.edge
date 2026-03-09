#!/bin/sh
# Wrapper script for Radical Edge Service
# This script ensures that the service runs with the correct environment variables
# (like PATH) so that any child processes spawned by edge plugins inherit the
# correct virtualenv or conda environment path.

BIN_DIR="$( cd "$( dirname "$0" )" && pwd )"

# Try to activate virtualenv if we are inside one
if [ -f "$BIN_DIR/activate" ]; then
    . "$BIN_DIR/activate"
else
    # If no virtualenv activate script, just ensure our BIN_DIR is first in PATH
    export PATH="$BIN_DIR:$PATH"
fi

# Execute the actual service script, passing along all arguments
exec "$BIN_DIR/radical-edge-service.py" "$@"
