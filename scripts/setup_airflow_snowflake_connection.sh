#!/bin/bash

# Script to set up Airflow Snowflake connection using .env variables
# Run this script from the project root directory

set -e

# Check if .env file exists
if [ ! -f ".env" ]; then
    echo "Error: .env file not found in current directory"
    echo "Please create a .env file with your Snowflake credentials"
    exit 1
fi

# Source the .env file
source .env

# Check if Airflow is running
if ! docker-compose -f docker-compose.airflow.yml ps | grep -q "airflow-scheduler"; then
    echo "Error: Airflow is not running. Start it first: docker-compose -f docker-compose.airflow.yml up -d"
    exit 1
fi

# Check if required Snowflake variables are set
if [ -z "$SNOWFLAKE_ACCOUNT" ] || [ -z "$SNOWFLAKE_USER" ]; then
    echo "Error: Required Snowflake environment variables are not set in .env"
    echo "Please set: SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PRIVATE_KEY_FILE"
    exit 1
fi

# Check if private key file exists
if [ ! -f "$SNOWFLAKE_PRIVATE_KEY_FILE" ]; then
    echo "Error: Private key file not found at $SNOWFLAKE_PRIVATE_KEY_FILE"
    echo "Please check SNOWFLAKE_PRIVATE_KEY_FILE in .env"
    exit 1
fi

echo "Setting up Snowflake connection for account: $SNOWFLAKE_ACCOUNT"

# Read and base64 encode the private key (use -w 0 on Linux to disable line wrapping)
PRIVATE_KEY_CONTENT=$(cat "$SNOWFLAKE_PRIVATE_KEY_FILE" | base64 -w 0 2>/dev/null || cat "$SNOWFLAKE_PRIVATE_KEY_FILE" | base64)

# Build the extra JSON with all Snowflake parameters
EXTRA_JSON=$(cat <<EOF
{
  "account": "$SNOWFLAKE_ACCOUNT",
  "role": "$SNOWFLAKE_ROLE",
  "warehouse": "$SNOWFLAKE_WAREHOUSE",
  "database": "$SNOWFLAKE_DATABASE",
  "private_key_content": "$PRIVATE_KEY_CONTENT"
}
EOF
)

# Delete existing connection if it exists
docker-compose -f docker-compose.airflow.yml exec -T airflow-scheduler airflow connections delete snowflake_default 2>/dev/null || true

# Add new connection
docker-compose -f docker-compose.airflow.yml exec -T airflow-scheduler airflow connections add 'snowflake_default' \
    --conn-type 'snowflake' \
    --conn-login "$SNOWFLAKE_USER" \
    --conn-schema "$SNOWFLAKE_SCHEMA" \
    --conn-extra "$EXTRA_JSON"

echo "✅ Snowflake connection setup complete"
echo ""
echo "Connection details:"
echo "  Account: $SNOWFLAKE_ACCOUNT"
echo "  User: $SNOWFLAKE_USER"
echo "  Role: $SNOWFLAKE_ROLE"
echo "  Warehouse: $SNOWFLAKE_WAREHOUSE"
echo "  Database: $SNOWFLAKE_DATABASE"
echo "  Schema: $SNOWFLAKE_SCHEMA"
echo ""
echo "You can verify the connection in Airflow UI:"
echo "  http://localhost:8080/connection/list/"
