#!/bin/bash
set -e

import_file() {
    local target_file="$1"
    echo "Importing: $target_file"
    psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
        -c "COPY staging_raw FROM '$target_file' WITH (FORMAT csv, HEADER true, DELIMITER ',', QUOTE '\"', ESCAPE '\"');"
}

import_file '/var/lib/postgresql/data_files/MOCK_DATA.csv'
import_file '/var/lib/postgresql/data_files/MOCK_DATA (1).csv'
import_file '/var/lib/postgresql/data_files/MOCK_DATA (2).csv'
import_file '/var/lib/postgresql/data_files/MOCK_DATA (3).csv'
import_file '/var/lib/postgresql/data_files/MOCK_DATA (4).csv'
import_file '/var/lib/postgresql/data_files/MOCK_DATA (5).csv'
import_file '/var/lib/postgresql/data_files/MOCK_DATA (6).csv'
import_file '/var/lib/postgresql/data_files/MOCK_DATA (7).csv'
import_file '/var/lib/postgresql/data_files/MOCK_DATA (8).csv'
import_file '/var/lib/postgresql/data_files/MOCK_DATA (9).csv'

echo "All CSV files have been imported successfully!"
