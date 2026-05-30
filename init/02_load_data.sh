#!/bin/bash
set -e

load_csv() {
    local file="$1"
    echo "Loading: $file"
    psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
        -c "COPY mock_data FROM '$file' WITH (FORMAT csv, HEADER true, DELIMITER ',', QUOTE '\"', ESCAPE '\"');"
}

load_csv '/data/MOCK_DATA.csv'
load_csv '/data/MOCK_DATA (1).csv'
load_csv '/data/MOCK_DATA (2).csv'
load_csv '/data/MOCK_DATA (3).csv'
load_csv '/data/MOCK_DATA (4).csv'
load_csv '/data/MOCK_DATA (5).csv'
load_csv '/data/MOCK_DATA (6).csv'
load_csv '/data/MOCK_DATA (7).csv'
load_csv '/data/MOCK_DATA (8).csv'
load_csv '/data/MOCK_DATA (9).csv'

echo "All CSV files loaded successfully"
