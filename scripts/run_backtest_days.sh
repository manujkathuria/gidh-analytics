#!/bin/bash

# --- 1. Locate Environment ---
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
APP_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
ENV_FILE="$APP_DIR/.env"

if [ ! -f "$ENV_FILE" ]; then
    echo "❌ Error: .env file not found at $ENV_FILE"
    exit 1
fi

# --- 2. Load Configuration ---
export $(grep -v '^#' "$ENV_FILE" | xargs)

DATA_DIR="${BACKTEST_BACKUP_DIR}"
TMP_EXTRACT_DIR="${BACKTEST_DATA_DIRECTORY}"
VENV_DIR="$APP_DIR/.venv"
APP_MAIN="main.py"

# --- 3. Run Single Date Backtest (Default Behavior) ---
# It uses the BACKTEST_DATE from your .env file
TARGET_DATE="${BACKTEST_DATE}"
file="${DATA_DIR}/backup_${TARGET_DATE}.tar.xz"

if [ ! -f "$file" ]; then
    echo "❌ Error: Backup file for $TARGET_DATE not found at $file"
    echo "Check your BACKTEST_DATE in .env or the BACKTEST_BACKUP_DIR."
    exit 1
fi

echo "🚀 Starting backtest for $TARGET_DATE using $file"

# Ensure extraction directory is clean
rm -rf "$TMP_EXTRACT_DIR"
mkdir -p "$TMP_EXTRACT_DIR"
tar -xJf "$file" -C "$TMP_EXTRACT_DIR"

# Run the application
cd "$APP_DIR"
if [ -f "${VENV_DIR}/bin/activate" ]; then
    source "${VENV_DIR}/bin/activate"
else
    echo "❌ Virtual environment not found."
    exit 1
fi

python "${APP_MAIN}"

if [ $? -ne 0 ]; then
    echo "❌ Backtest failed for $TARGET_DATE"
    deactivate
    exit 1
fi

deactivate
echo "✅ Successfully finished backtest for $TARGET_DATE"