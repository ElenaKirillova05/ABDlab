#!/bin/bash
set -e

import_to_staging() {
    local source_file="$1"
    echo "Processing: $source_file"
    psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
        -c "COPY staging_source FROM '$source_file' WITH (FORMAT csv, HEADER true, DELIMITER ',', QUOTE '\"', ESCAPE '\"');"
}

import_to_staging '/data/MOCK_DATA.csv'
import_to_staging '/data/MOCK_DATA (1).csv'
import_to_staging '/data/MOCK_DATA (2).csv'
import_to_staging '/data/MOCK_DATA (3).csv'
import_to_staging '/data/MOCK_DATA (4).csv'
import_to_staging '/data/MOCK_DATA (5).csv'
import_to_staging '/data/MOCK_DATA (6).csv'
import_to_staging '/data/MOCK_DATA (7).csv'
import_to_staging '/data/MOCK_DATA (8).csv'
import_to_staging '/data/MOCK_DATA (9).csv'

echo "All CSV files successfully imported into staging_source!"
