#!/bin/sh
# Wrapper script for Radical Edge Service
# This script ensures that the service runs with the correct environment variables
# (like PATH and PYTHONPATH) so that any child processes spawned by edge plugins
# inherit the correct virtualenv or conda environment path.

BIN_DIR="$( cd "$( dirname "$0" )" && pwd )"
BASE_DIR="$( cd "$BIN_DIR/.." && pwd )"

# Try to activate virtualenv if we are inside one
if [ -f "$BIN_DIR/activate" ]; then
    . "$BIN_DIR/activate"
else
    # If no virtualenv activate script, just ensure our BIN_DIR is first in PATH
    export PATH="$BIN_DIR:$PATH"
fi

# Set PYTHONPATH to include site-packages from the virtualenv/installation
# This ensures child processes can find installed Python modules
for SITE_PKG in "$BASE_DIR"/lib/python*/site-packages; do
    if [ -d "$SITE_PKG" ]; then
        if [ -z "$PYTHONPATH" ]; then
            export PYTHONPATH="$SITE_PKG"
        else
            export PYTHONPATH="$SITE_PKG:$PYTHONPATH"
        fi
        break
    fi
done

# Also check for editable installs (src layout)
if [ -d "$BASE_DIR/src" ]; then
    if [ -z "$PYTHONPATH" ]; then
        export PYTHONPATH="$BASE_DIR/src"
    else
        export PYTHONPATH="$BASE_DIR/src:$PYTHONPATH"
    fi
fi

# Execute the actual service script, passing along all arguments
exec "$BIN_DIR/radical-edge-service.py" "$@"
