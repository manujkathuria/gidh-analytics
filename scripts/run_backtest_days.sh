#!/bin/bash

# CONFIGURATION
DATA_DIR="/home/manuj/workspace/wealth-wave-ventures/backup/backtest"
ENV_FILE="/home/manuj/workspace/wealth-wave-ventures/gidh-analytics/.env"
TMP_EXTRACT_DIR="/home/manuj/workspace/wealth-wave-ventures/gidh-analytics/data/kite"
APP_DIR="/home/manuj/workspace/wealth-wave-ventures/gidh-analytics"
VENV_DIR="${APP_DIR}/.venv"
APP_MAIN="main.py"
SLEEP_BETWEEN_RUNS=5

# 1. Get sorted list of backup files
files=$(ls "$DATA_DIR"/backup_*.tar.xz | sort)

# 2. Loop through each file in date order
for file in $files; do
    echo "Processing $file..."

    # 3. Extract date from filename (backup_YYYY-MM-DD.tar.xz)
    base=$(basename "$file")
    date=${base#backup_}
    date=${date%.tar.xz}

    echo "Setting BACKTESTING_DATE=$date"

    # 4. Update .env file (replace or append)
    if grep -q "^BACKTEST_DATE=" "$ENV_FILE"; then
        sed -i.bak "s/^BACKTEST_DATE=.*/BACKTEST_DATE=$date/" "$ENV_FILE"
    else
        echo "BACKTEST_DATE=$date" >> "$ENV_FILE"
    fi

    # 5. Clear old extracted data and extract new one
    rm -rf "$TMP_EXTRACT_DIR"
    mkdir -p "$TMP_EXTRACT_DIR"
    tar -xJf "$file" -C "$TMP_EXTRACT_DIR"

    # 6. Run the Python app via venv
    echo "Running backtest for $date"
    cd "$APP_DIR" || { echo "❌ Could not cd into $APP_DIR"; exit 1; }

    # --- ADDED: Activate Virtual Environment ---
    if [ -f "${VENV_DIR}/bin/activate" ]; then
        source "${VENV_DIR}/bin/activate"
    else
        echo "Error: Virtual environment not found at ${VENV_DIR}"
        exit 1
    fi

    # --- MODIFIED: Run the python application ---
    python "${APP_MAIN}"

    if [ $? -ne 0 ]; then
        echo "❌ Python app failed for $date. Exiting."
        # --- ADDED: Deactivate on failure ---
        deactivate
        exit 1
    fi

    # --- ADDED: Deactivate Virtual Environment ---
    deactivate

    echo "✅ Finished backtest for $date"
    echo "-----------------------------"

    # Optional delay
    sleep "$SLEEP_BETWEEN_RUNS"
done

echo "🎉 All backtests completed."