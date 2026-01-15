#!/bin/bash

# === Config ===
DB_CONTAINER="timescaledb"
DB_NAME="gidh_analytics"
DB_USER="postgres"
BACKUP_ROOT="/home/manuj/workspace/wealth-wave-ventures/backup"

# === Date Handling ===
if [[ -n "$1" ]]; then
    DATE_INPUT="$1"
    if ! date -d "$DATE_INPUT" "+%F" >/dev/null 2>&1; then
        echo "❌ Invalid date format. Please use YYYY-mm-dd."
        exit 1
    fi
    BACKUP_DATE=$(date -d "$DATE_INPUT" "+%F")
else
    BACKUP_DATE=$(date "+%F")
fi

BACKUP_DIR="${BACKUP_ROOT}/${BACKUP_DATE}"
TICKS_DIR="${BACKUP_DIR}/live_ticks"
DEPTH_DIR="${BACKUP_DIR}/live_order_depth"

mkdir -p "$TICKS_DIR"
mkdir -p "$DEPTH_DIR"

echo "📦 Backing up TimescaleDB data for $BACKUP_DATE to $BACKUP_DIR"

# === Set UTC start and end time (IST 09:15 - 15:30) ===
START_TIME="${BACKUP_DATE} 03:45:00"
END_TIME="${BACKUP_DATE} 10:00:00"

echo "⏱️  Time Range: $START_TIME UTC → $END_TIME UTC"

# === Export live_ticks and live_order_depth per stock ===
echo "→ Exporting live_ticks and live_order_depth per stock for $BACKUP_DATE..."

# Optimized: Removed -t to prevent TTY hangs and switched to GROUP BY for speed
STOCKS_RAW=$(docker exec $DB_CONTAINER psql -U $DB_USER -d $DB_NAME -q -X -t -A -c "
    SELECT stock_name
    FROM live_ticks
    WHERE timestamp >= '$START_TIME' AND timestamp <= '$END_TIME'
    GROUP BY stock_name;
")

# Populate array from the raw output
mapfile -t STOCKS <<< "$STOCKS_RAW"

for STOCK in "${STOCKS[@]}"; do
    [[ -z "$STOCK" ]] && continue
    # Clean up whitespace/carriage returns that might come from Docker
    STOCK=$(echo "$STOCK" | tr -d '\r' | xargs)
    SAFE_STOCK=$(echo "$STOCK" | tr -cd '[:alnum:]_-')

    # live_ticks export (Removed -t)
    QUERY_TICKS="SELECT * FROM live_ticks
           WHERE stock_name = '$STOCK'
           AND timestamp >= '$START_TIME' AND timestamp <= '$END_TIME'
           ORDER BY timestamp ASC"
    echo "    → live_ticks: $STOCK"
    docker exec $DB_CONTAINER psql -U $DB_USER -d $DB_NAME -q -X \
        -c "\COPY ($QUERY_TICKS) TO STDOUT WITH CSV HEADER" \
        > "${TICKS_DIR}/live_ticks_${SAFE_STOCK}.csv"

    # live_order_depth export (Removed -t)
    QUERY_DEPTH="SELECT * FROM live_order_depth
           WHERE stock_name = '$STOCK'
           AND timestamp >= '$START_TIME' AND timestamp <= '$END_TIME'
           ORDER BY timestamp ASC"
    echo "    → live_order_depth: $STOCK"
    docker exec $DB_CONTAINER psql -U $DB_USER -d $DB_NAME -q -X \
        -c "\COPY ($QUERY_DEPTH) TO STDOUT WITH CSV HEADER" \
        > "${DEPTH_DIR}/live_order_depth_${SAFE_STOCK}.csv"
done

# === Compress the main backup directory ===
echo "📦 Compressing daily backup directory into tar.xz archive..."
cd "$BACKUP_ROOT" || exit 1
if [ -d "${BACKUP_DATE}" ]; then
    tar -cJf "backup_${BACKUP_DATE}.tar.xz" "${BACKUP_DATE}"
    rm -rf "${BACKUP_DIR}"
    echo "✅ Backup completed and compressed!"
else
    echo "⚠️  No backup directory found to compress. Perhaps no stocks were found for this date?"
fi