#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

pause_if_interactive() {
    if [[ -t 0 && -t 1 ]]; then
        echo
        read -r -p "Press Enter to close..." _
    fi
}

print_failure_help() {
    local exit_code="$1"

    echo
    echo "Keyword pipeline failed with exit code ${exit_code}."
    echo "Common causes:"
    echo "  - Streamlit is not installed in the selected Python environment."
    echo "  - The port is already in use. Try: ./run_pipeline.sh --port 8502"
    echo "  - The Python app crashed during startup."
}

if [[ -f "venv/bin/activate" ]]; then
    # Project-local virtualenv after Ubuntu migration.
    source "venv/bin/activate"
fi

PYTHON_BIN="${VIRTUAL_ENV:+$VIRTUAL_ENV/bin/python}"
if [[ -z "${PYTHON_BIN:-}" ]]; then
    PYTHON_BIN="$(command -v python3 || command -v python)"
fi

if [[ -z "${PYTHON_BIN:-}" ]]; then
    echo "Unable to find a Python interpreter. Install Python 3 or create ./venv first."
    pause_if_interactive
    exit 1
fi

echo "Starting keyword pipeline from: $SCRIPT_DIR"
echo "Using Python: $PYTHON_BIN"
echo

set +e
"$PYTHON_BIN" -m src.main "$@"
exit_code=$?
set -e

if [[ $exit_code -ne 0 ]]; then
    print_failure_help "$exit_code"
    pause_if_interactive
fi

exit "$exit_code"
