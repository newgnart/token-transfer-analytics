#!/usr/bin/env python3
"""
Database connection tests using pytest
"""

import os
import pytest
from dotenv import load_dotenv

from scripts.utils.database_client import PostgresClient, SnowflakeClient

load_dotenv()


def test_postgres_connection():
    """Test PostgreSQL database connection and basic query execution."""
    # Get client and connection parameters
    client = PostgresClient.from_env()
    params = client.connection_params

    print(f"🔌 Connecting to PostgreSQL at {params['host']}:{params['port']}...")

    # Test connection using context manager
    with client.get_connection() as conn:
        assert conn is not None, "Connection should not be None"

        with conn.cursor() as cur:
            # Test basic query
            cur.execute("SELECT version()")
            version = cur.fetchone()[0]

            assert version is not None, "Version query should return a result"
            assert "PostgreSQL" in version, "Version should contain 'PostgreSQL'"

            print("✅ Connected successfully!")
            print(f"📊 PostgreSQL version: {version.split(',')[0]}")


@pytest.mark.skipif(
    not os.getenv("SNOWFLAKE_PRIVATE_KEY_FILE"),
    reason="Snowflake credentials not configured (SNOWFLAKE_PRIVATE_KEY_PATH not set)",
)
def test_snowflake_connection():
    """Test Snowflake connection using private key authentication."""
    # Get client
    client = SnowflakeClient.from_env()

    # Test connection using context manager
    with client.get_connection() as conn:
        assert conn is not None, "Snowflake connection should not be None"

        cursor = conn.cursor()
        try:
            cursor.execute("SELECT CURRENT_VERSION()")
            version = cursor.fetchone()[0]

            assert version is not None, "Version query should return a result"

            print("✅ Snowflake connected successfully!")
            print(f"📊 Snowflake version: {version}")
        finally:
            cursor.close()
