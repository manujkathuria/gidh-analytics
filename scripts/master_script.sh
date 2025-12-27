#!/bin/bash

# Master script for GIDH Analytics application

# --- Configuration ---
# Using a more dynamic way to find the root path, but keeping your original as fallback
ROOT_PATH="/home/manuj/workspace/wealth-wave-ventures/gidh-analytics"
SCRIPTS_DIR="${ROOT_PATH}/scripts"
VENV_DIR="${ROOT_PATH}/.venv"
LOG_DIR="${ROOT_PATH}/logs"
PYTHON_APP_PID_FILE="/tmp/gidh_python_app.pid"

# --- DB Configuration ---
DB_CONTAINER="timescaledb"
DB_NAME="gidh_analytics"
DB_USER="postgres"

# Ensure log directory exists
mkdir -p "${LOG_DIR}"

# --- Environment Setup ---
# Crucial: This allows imports like 'from common.parameters' to work across all scripts
export PYTHONPATH="${ROOT_PATH}"

# --- Functions ---

start_python_app() {
    echo "Starting Python application (main.py) in background..."
    cd "${ROOT_PATH}" || { echo "Error: Could not cd to ${ROOT_PATH}"; return 1; }

    if [ -d "${VENV_DIR}" ]; then
        source "${VENV_DIR}/bin/activate"
        # Run main.py which now handles its own config validation
        nohup python main.py >> "${LOG_DIR}/python_app.log" 2>&1 &
        echo $! > "${PYTHON_APP_PID_FILE}"
        deactivate
        echo "Python application started with PID $(cat "${PYTHON_APP_PID_FILE}")."
    else
        echo "Error: Virtual environment not found at ${VENV_DIR}"
        return 1
    fi
}

# New function to run the pre-market stock selection
run_selection() {
    echo "Running Pre-Market Stock Selection (Analytics)..."
    cd "${ROOT_PATH}" || { echo "Error: Could not cd to ${ROOT_PATH}"; return 1; }

    if [ -d "${VENV_DIR}" ]; then
        source "${VENV_DIR}/bin/activate"
        # Running as a module to correctly resolve subpackage imports
        python -m analytics.selector.run
        deactivate
        echo "✅ Selection process finished."
    else
        echo "Error: Virtual environment not found."
        return 1
    fi
}

stop_python_app() {
    echo "Stopping Python application..."
    if [ -f "${PYTHON_APP_PID_FILE}" ]; then
        PID=$(cat "${PYTHON_APP_PID_FILE}")
        if ps -p "$PID" > /dev/null; then
            echo "Sending SIGINT to PID $PID..."
            kill -SIGINT "$PID"
            sleep 3

            if ps -p "$PID" > /dev/null; then
                echo "SIGINT failed; sending SIGTERM..."
                kill "$PID"
                sleep 5

                if ps -p "$PID" > /dev/null; then
                    echo "SIGTERM failed; force killing..."
                    kill -9 "$PID"
                    echo "Process $PID killed."
                else
                    echo "Process $PID stopped after SIGTERM."
                fi
            else
                echo "Process $PID stopped after SIGINT."
            fi
        else
            echo "No process found with PID $PID."
        fi
        rm -f "${PYTHON_APP_PID_FILE}"
    else
        echo "No PID file found (${PYTHON_APP_PID_FILE})."
    fi
}

run_login_script() {
    echo "Running login script interactively..."
    cd "${SCRIPTS_DIR}" || { echo "Error: Could not cd to ${SCRIPTS_DIR}"; return 1; }

    if [ ! -d "${VENV_DIR}" ]; then
        echo "Virtual environment not found, creating one..."
        python3 -m venv "${VENV_DIR}"
        source "${VENV_DIR}/bin/activate"
        pip install -U pip
        pip install -r "${ROOT_PATH}/requirements.txt" 2>/dev/null || echo "No requirements.txt found"
    else
        source "${VENV_DIR}/bin/activate"
    fi

    # Ensuring the script can see the root packages if it evolves
    export PYTHONPATH="${ROOT_PATH}"
    python login.py

    deactivate
    cd - > /dev/null
}

run_backup() {
    echo "Starting database backup script..."
    cd "${SCRIPTS_DIR}" || { echo "Error: Could not cd to ${SCRIPTS_DIR}"; return 1; }
    ./backup_db.sh >> "${LOG_DIR}/backup.log" 2>&1 || { echo "Error: backup_db.sh failed"; cd - > /dev/null; return 1; }
    cd - > /dev/null
    echo "Backup script finished."
}

truncate_tables() {
    echo "⚠️ WARNING: This will permanently delete all data from live_order_depth table."
    read -p "Are you sure you want to continue? (y/n) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo "Truncate operation cancelled."
        exit 1
    fi

    TABLES_TO_TRUNCATE=(
        "public.live_order_depth"
    )

    echo "Truncating tables in database: ${DB_NAME}..."
    for table in "${TABLES_TO_TRUNCATE[@]}"; do
        echo "  → Truncating ${table}..."
        docker exec -t "${DB_CONTAINER}" psql -U "${DB_USER}" -d "${DB_NAME}" -c "TRUNCATE TABLE ${table} RESTART IDENTITY CASCADE;"
        if [ $? -ne 0 ]; then
            echo "❌ Error: Failed to truncate ${table}."
            return 1
        fi
    done

    echo "✅ All specified tables have been truncated successfully."
}

run_maintenance() {
    echo "Starting database maintenance..."

    # --- Configuration ---
    TABLES_TO_PRUNE=(
        "public.live_ticks"
        "public.enriched_features"
    )
    TABLE_TO_TRUNCATE="public.live_order_depth"
    RETENTION_DAYS="14"

    # --- Prune old data ---
    echo "Pruning data older than ${RETENTION_DAYS} days..."
    for table in "${TABLES_TO_PRUNE[@]}"; do
        echo "  → Pruning ${table}..."
        docker exec -t "${DB_CONTAINER}" psql -U "${DB_USER}" -d "${DB_NAME}" -c "DELETE FROM ${table} WHERE timestamp < NOW() - INTERVAL '${RETENTION_DAYS} days';"
        if [ $? -ne 0 ]; then
            echo "❌ Error: Failed to prune ${table}."
            return 1
        fi
    done

    # --- Truncate order depth data ---
    echo "Clearing all data from ${TABLE_TO_TRUNCATE}..."
    docker exec -t "${DB_CONTAINER}" psql -U "${DB_USER}" -d "${DB_NAME}" -c "TRUNCATE TABLE ${TABLE_TO_TRUNCATE};"
    if [ $? -ne 0 ]; then
        echo "❌ Error: Failed to truncate ${TABLE_TO_TRUNCATE}."
        return 1
    fi

    echo "✅ Database maintenance completed successfully."
}

refresh_mv() {
    echo "Refreshing materialized view: large_trade_thresholds_mv..."
    docker exec -t "${DB_CONTAINER}" psql -U "${DB_USER}" -d "${DB_NAME}" -c "REFRESH MATERIALIZED VIEW large_trade_thresholds_mv;"
    if [ $? -ne 0 ]; then
        echo "❌ Error: Failed to refresh materialized view."
        return 1
    fi
    echo "✅ Materialized view refreshed successfully."
}


# --- Main Script Logic ---
case "$1" in
    start)
        start_python_app
        ;;
    stop)
        stop_python_app >> "${LOG_DIR}/stop.log" 2>&1
        ;;
    select)
        # Added new select command for pre-market analytics
        run_selection
        ;;
    login)
        run_login_script
        ;;
    backup)
        run_backup
        ;;
    truncate)
        truncate_tables
        ;;
    maintain)
        run_maintenance
        ;;
    refresh)
        refresh_mv
        ;;
    *)
        # Updated usage to include 'select'
        echo "Usage: $0 {start|stop|select|login|backup|truncate|maintain|refresh}"
        exit 1
        ;;
esac

exit 0