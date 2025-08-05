#!/bin/bash

# === Configuration ===
# The directory where your 'backup_YYYY-MM-DD.tar.xz' files are located.
# Please change this to your actual backup directory path.
SOURCE_DIR="/Users/manujkathuria/Workspace/wealth-wave-ventures/backup/backtest"

# The directory where the final consolidated CSV files will be saved.
# This directory will be created if it doesn't exist.
OUTPUT_DIR="${SOURCE_DIR}/consolidated_ticks"

# A temporary directory for extracting the archives.
# This will be created and then removed by the script.
TEMP_DIR="${SOURCE_DIR}/temp_extract"


# --- Script Start ---
echo "Starting consolidation process..."

# 1. Setup: Create necessary directories and clean up previous runs.
echo "Setting up directories..."
rm -rf "${TEMP_DIR}" # Remove old temp data if it exists
mkdir -p "${TEMP_DIR}"
mkdir -p "${OUTPUT_DIR}"
echo " -> Output will be saved in: ${OUTPUT_DIR}"
echo " -> Using temporary directory: ${TEMP_DIR}"
echo ""


# 2. Extract all archives into the temporary directory.
echo "--- Step 1: Extracting all archives ---"
# Find all .tar.xz files in the source directory
ARCHIVES=$(find "${SOURCE_DIR}" -maxdepth 1 -type f -name 'backup_*.tar.xz' | sort)

if [ -z "$ARCHIVES" ]; then
    echo "❌ No backup archives (*.tar.xz) found in ${SOURCE_DIR}. Exiting."
    exit 1
fi

for archive in $ARCHIVES; do
    echo " -> Extracting $(basename "$archive")"
    # Extract the archive into the temp directory. The -J flag is for .xz files.
    tar -xJf "$archive" -C "${TEMP_DIR}"
    if [ $? -ne 0 ]; then
        echo "❌ Error extracting ${archive}. Please check the file and try again. Exiting."
        rm -rf "${TEMP_DIR}" # Clean up on failure
        exit 1
    fi
done
echo "✅ All archives extracted successfully."
echo ""


# 3. Find all unique stock names from the extracted files.
echo "--- Step 2: Identifying all unique stocks ---"
# This command finds all 'live_ticks_*.csv' files, extracts just the filename,
# removes the prefix and suffix, and then gets a unique sorted list.
STOCK_NAMES=$(find "${TEMP_DIR}" -type f -name 'live_ticks_*.csv' -exec basename {} \; | sed -e 's/live_ticks_//' -e 's/\.csv//' | sort -u)

if [ -z "$STOCK_NAMES" ]; then
    echo "❌ No 'live_ticks' CSV files found in the extracted archives. Exiting."
    rm -rf "${TEMP_DIR}"
    exit 1
fi

echo "Found the following stocks to process:"
echo "${STOCK_NAMES}"
echo ""


# 4. Consolidate files for each stock.
echo "--- Step 3: Consolidating data for each stock ---"
for stock in ${STOCK_NAMES}; do
    echo " -> Processing: ${stock}"
    
    # Define the final output file for the current stock.
    CONSOLIDATED_FILE="${OUTPUT_DIR}/${stock}_ticks.csv"
    
    # Find all CSV files for the current stock across all extracted date directories, sorted chronologically.
    FILES_FOR_STOCK=$(find "${TEMP_DIR}" -type f -name "live_ticks_${stock}.csv" | sort)
    
    # A flag to ensure we only copy the header from the very first file.
    is_first_file=true
    
    # Loop through each daily file for the stock.
    for daily_file in ${FILES_FOR_STOCK}; do
        if [ "$is_first_file" = true ]; then
            # For the first file, copy the entire content (including header) to the new consolidated file.
            cat "${daily_file}" > "${CONSOLIDATED_FILE}"
            is_first_file=false
        else
            # For all subsequent files, use 'tail -n +2' to skip the header line
            # and append the rest of the file to the consolidated file.
            tail -n +2 "${daily_file}" >> "${CONSOLIDATED_FILE}"
        fi
    done
    echo "    ✅ Consolidated data saved to $(basename "$CONSOLIDATED_FILE")"
done
echo ""


# 5. Cleanup: Remove the temporary directory.
echo "--- Step 4: Cleaning up ---"
rm -rf "${TEMP_DIR}"
echo " -> Removed temporary directory."
echo ""

echo "🎉 All done! Consolidated files are in ${OUTPUT_DIR}"
