from abc import ABC, abstractmethod
from contextlib import contextmanager
from typing import Optional, Any, Dict, List
import os

import psycopg
import dlt
import snowflake.connector


class BaseDatabaseClient(ABC):
    """Abstract base class for database clients with common patterns."""

    def __init__(self):
        """Initialize with connection parameters."""
        self.connection_params = self._build_connection_params()
        self._engine = None

    @abstractmethod
    def _build_connection_params(self) -> Dict[str, Any]:
        """Build connection parameters from environment or config."""
        pass

    @abstractmethod
    @contextmanager
    def get_connection(self):
        """Context manager for database connections."""
        pass

    @abstractmethod
    def get_dlt_destination(self):
        """Get DLT destination for this database."""
        pass

    @staticmethod
    def _get_env_var(key: str, default: str = None) -> str:
        """Helper method to get environment variables."""
        return os.getenv(key, default)


class PostgresClient(BaseDatabaseClient):
    """Object-oriented PostgreSQL client for database operations."""

    def __init__(
        self,
        host: str = None,
        port: int = None,
        database: str = None,
        user: str = None,
        password: str = None,
    ):
        """
        Initialize PostgresDestination with database configuration.

        Args:
            host: Database host
            port: Database port
            database: Database name
            user: Database user
            password: Database password
        """
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        super().__init__()

    @classmethod
    def from_env(cls) -> "PostgresClient":
        """Create from environment variables"""
        return cls(
            host=cls._get_env_var("POSTGRES_HOST"),
            port=int(cls._get_env_var("POSTGRES_PORT", "5432")),
            database=cls._get_env_var("POSTGRES_DB"),
            user=cls._get_env_var("POSTGRES_USER"),
            password=cls._get_env_var("POSTGRES_PASSWORD"),
        )

    def _build_connection_params(self) -> Dict[str, Any]:
        """Build connection parameters from instance variables."""
        return {
            "host": self.host,
            "port": self.port,
            "dbname": self.database,
            "user": self.user,
            "password": self.password,
        }

    @contextmanager
    def get_connection(self):
        """Context manager for PostgreSQL connections."""
        conn = None
        try:
            conn = psycopg.connect(**self.connection_params)
            yield conn
        finally:
            if conn:
                conn.close()

    def get_dlt_destination(self) -> Any:
        """Return DLT destination for pipeline operations."""
        params = self.connection_params
        connection_url = f"postgresql://{params['user']}:{params['password']}@{params['host']}:{params['port']}/{params['dbname']}"
        return dlt.destinations.postgres(connection_url)


class SnowflakeClient(BaseDatabaseClient):
    """Reusable Snowflake client with connection management."""

    def __init__(
        self,
        account: str = None,
        user: str = None,
        authenticator: str = None,
        private_key_file: str = None,
        warehouse: str = None,
        database: str = None,
        role: str = None,
    ):
        """Initialize Snowflake client with environment configuration."""
        self.account = account
        self.user = user
        self.authenticator = authenticator
        self.private_key_file = private_key_file
        self.warehouse = warehouse
        self.database = database
        self.role = role
        super().__init__()

    @classmethod
    def from_env(cls) -> "SnowflakeClient":
        """Create from environment variables"""
        return cls(
            account=cls._get_env_var("SNOWFLAKE_ACCOUNT"),
            user=cls._get_env_var("SNOWFLAKE_USER"),
            authenticator=cls._get_env_var("SNOWFLAKE_AUTHENTICATOR"),
            private_key_file=os.path.expanduser(
                cls._get_env_var("SNOWFLAKE_PRIVATE_KEY_PATH")
            ),
            warehouse=cls._get_env_var("SNOWFLAKE_WAREHOUSE"),
            database=cls._get_env_var("SNOWFLAKE_DATABASE"),
            role=cls._get_env_var("SNOWFLAKE_ROLE"),
        )

    def _build_connection_params(self) -> Dict[str, Any]:
        """Build connection parameters from environment variables."""
        return {
            "account": self.account,
            "user": self.user,
            "authenticator": self.authenticator,
            "private_key_file": self.private_key_file,
            "warehouse": self.warehouse,
            "database": self.database,
            "role": self.role,
        }

    @contextmanager
    def get_connection(self):
        """Context manager for Snowflake connections."""
        conn = None
        try:
            conn = snowflake.connector.connect(**self.connection_params)
            yield conn
        finally:
            if conn:
                conn.close()

    def get_dlt_destination(self):
        """Get DLT destination configuration for Snowflake."""
        private_key_file = self.connection_params["private_key_file"]

        try:
            with open(private_key_file, "r") as key_file:
                private_key_data = key_file.read()
        except FileNotFoundError:
            raise FileNotFoundError(f"Private key file not found: {private_key_file}")

        # Create credentials object that DLT expects
        credentials = {
            "username": self.connection_params["user"],
            "host": self.connection_params["account"],
            "warehouse": self.connection_params["warehouse"],
            "database": self.connection_params["database"],
            "role": self.connection_params["role"],
            "private_key": private_key_data,
        }

        # Only add private key password if it exists
        if self.connection_params.get("private_key_file_pwd"):
            credentials["private_key_passphrase"] = self.connection_params[
                "private_key_file_pwd"
            ]

        return dlt.destinations.snowflake(credentials=credentials)
