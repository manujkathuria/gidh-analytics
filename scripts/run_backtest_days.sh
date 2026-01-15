#!/bin/bash

# --- 1. Locate Environment ---
# Finds the .env file relative to the script's location
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
APP_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
ENV_FILE="$APP_DIR/.env"

if [ ! -f "$ENV_FILE" ]; then
    echo "❌ Error: .env file not found at $ENV_FILE"
    exit 1
fi

# --- 2. Load Configuration from .env ---
# This allows the script to read variables defined in your .env
export $(grep -v '^#' "$ENV_FILE" | xargs)

# Map .env variables to script variables
DATA_DIR="${BACKTEST_BACKUP_DIR}"
TMP_EXTRACT_DIR="${BACKTEST_DATA_DIRECTORY}"
VENV_DIR="$APP_DIR/.venv"
APP_MAIN="main.py"

# --- 3. Process Backtest Files ---
files=$(ls "$DATA_DIR"/backup_*.tar.xz | sort)

for file in $files; do
    base=$(basename "$file")
    date=${base#backup_}
    date=${date%.tar.xz}

    echo "🚀 Processing $date using $file"

    # Auto-update the date in .env for the Python app to read
    sed -i "s/^BACKTEST_DATE=.*/BACKTEST_DATE=$date/" "$ENV_FILE"

    # Extract data to the directory specified in .env
    rm -rf "$TMP_EXTRACT_DIR"
    mkdir -p "$TMP_EXTRACT_DIR"
    tar -xJf "$file" -C "$TMP_EXTRACT_DIR"

    # Run the application
    cd "$APP_DIR"
    source "${VENV_DIR}/bin/activate"
    python "${APP_MAIN}"

    if [ $? -ne 0 ]; then
        echo "❌ Failed at $date"
        deactivate
        exit 1
    fi
    deactivate
    echo "✅ Finished $date"
done