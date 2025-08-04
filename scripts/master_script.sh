#!/bin/bash

# Master script for GIDH Analytics application

# --- Configuration ---
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

# --- Functions ---

start_python_app() {
    echo "Starting Python application (main.py) in background..."
    cd "${ROOT_PATH}" || { echo "Error: Could not cd to ${ROOT_PATH}"; return 1; }

    if [ -d "${VENV_DIR}" ]; then
        source "${VENV_DIR}/bin/activate"
        nohup python main.py >> "${LOG_DIR}/python_app.log" 2>&1 &
        echo $! > "${PYTHON_APP_PID_FILE}"
        deactivate
        echo "Python application started with PID $(cat "${PYTHON_APP_PID_FILE}")."
    else
        echo "Error: Virtual environment not found at ${VENV_DIR}"
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
    echo "⚠️ WARNING: This will permanently delete all data from live tables."
    read -p "Are you sure you want to continue? (y/n) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo "Truncate operation cancelled."
        exit 1
    fi

    TABLES_TO_TRUNCATE=(
        "public.live_ticks"
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

# --- Main Script Logic ---
case "$1" in
    start)
        start_python_app
        ;;
    stop)
        stop_python_app >> "${LOG_DIR}/stop.log" 2>&1
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
    *)
        echo "Usage: $0 {start|stop|login|backup|truncate}"
        exit 1
        ;;
esac

exit 0
