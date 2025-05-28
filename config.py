"""
Configuration module for user failure stats analysis.
Handles environment variables, database connection, and query execution.
"""

import os
import logging
import pandas as pd
import psycopg2
from dataclasses import dataclass
from contextlib import contextmanager
from typing import Dict, Any
from dotenv import load_dotenv

# Load environment variables from .env file (local dev only)
# In Azure Functions, environment variables are set in Application Settings
if os.path.exists(".env"):
    load_dotenv()
else:
    logging.info("No .env file found, using environment variables")

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ConfigError(Exception):
    """Custom exception for configuration-related errors."""

    pass


class DatabaseError(Exception):
    """Custom exception for database-related errors."""

    pass


@dataclass
class OutputConfig:
    """Output configuration for plots and files."""

    output_dir: str = "./output"

    def __post_init__(self):
        """Create output directory if it doesn't exist."""
        os.makedirs(self.output_dir, exist_ok=True)


class Config:
    """Configuration and database management class."""

    def __init__(self):
        """Initialize with configuration from environment variables."""
        # Get environment name
        self.environment = os.getenv("AZURE_FUNCTIONS_ENVIRONMENT", "development")
        logger.info(f"Initializing configuration for environment: {self.environment}")

        # Database connection string - look for Azure-specific connection first
        self.db_url = os.getenv(
            "AZURE_POSTGRESQL_CONNECTIONSTRING",  # Azure provided connection string
            os.getenv("DB_URL", ""),  # Fallback to custom DB_URL
        )

        # If using Key Vault, can retrieve connection string securely in Azure
        if not self.db_url and self.environment != "development":
            logger.warning("No database URL found in environment variables")

        # Query dates
        self.start_date = os.getenv("START_DATE", "2025-05-20 18:30:00")
        self.end_date = os.getenv("END_DATE", "2025-05-22 18:29:59")

        # Database connection settings
        self.db_connection_timeout = int(os.getenv("DB_CONNECTION_TIMEOUT", "30"))
        self.db_query_timeout = int(os.getenv("DB_QUERY_TIMEOUT", "60"))
        self.db_max_retries = int(os.getenv("DB_MAX_RETRIES", "3"))

        # Output configuration
        self.output = OutputConfig(output_dir=os.getenv("OUTPUT_DIR", "./output"))

        logger.debug("Configuration initialized with database and output settings")

    def validate(self) -> bool:
        """Validate that all required configurations are present."""
        try:
            if not self.db_url:
                logger.error("Database URL is not configured")
                return False

            if not self.start_date or not self.end_date:
                logger.error("Date range is not properly configured")
                return False

            # Test output directory
            if not os.path.exists(self.output.output_dir):
                os.makedirs(self.output.output_dir, exist_ok=True)

            logger.info("Configuration validation successful")
            return True
        except Exception as e:
            logger.error(f"Configuration validation failed: {e}")
            return False

    @property
    def sql_query(self) -> str:
        """Generate the failure rate analysis SQL query with configured date range."""
        return f"""
        SELECT
            c.session_id,
            u.name,
            c.exotel_call_sid
        FROM
            public.calllog c
        JOIN
            public."user" u
            ON c.user_id = u.user_id
        WHERE
            c.created_on >= '{self.start_date}'::timestamp
            AND c.created_on <= '{self.end_date}'::timestamp
        ORDER BY c.created_on DESC;
        """

    @contextmanager
    def get_connection(self):
        """
        Context manager for database connections.
        Ensures proper connection cleanup.
        """
        connection = None
        retries = 0

        while retries < self.db_max_retries:
            try:
                connection = psycopg2.connect(
                    self.db_url, connect_timeout=self.db_connection_timeout
                )
                logger.info("Database connection established successfully")
                yield connection
                break
            except psycopg2.Error as e:
                retries += 1
                if retries >= self.db_max_retries:
                    logger.error(
                        f"Database connection failed after {retries} attempts: {e}"
                    )
                    raise DatabaseError(f"Failed to connect to database: {e}")
                else:
                    logger.warning(
                        f"Database connection attempt {retries} failed: {e}. Retrying..."
                    )
            finally:
                if connection:
                    connection.close()
                    logger.info("Database connection closed")

    def execute_query(self) -> pd.DataFrame:
        """
        Execute the failure rate analysis SQL query and return results as DataFrame.

        Returns:
            pandas.DataFrame: Query results

        Raises:
            DatabaseError: If query execution fails
        """
        try:
            with self.get_connection() as conn:
                logger.info("Executing failure rate analysis query...")
                logger.debug(f"Query: {self.sql_query}")

                # Set statement timeout for the query
                with conn.cursor() as cursor:
                    cursor.execute(
                        f"SET statement_timeout = {self.db_query_timeout * 1000};"
                    )  # ms

                df = pd.read_sql_query(self.sql_query, conn)
                logger.info(f"Query executed successfully. Retrieved {len(df)} rows")
                return df

        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            raise DatabaseError(f"Failed to execute query: {e}")

    def test_connection(self) -> bool:
        """
        Test database connection.

        Returns:
            bool: True if connection successful, False otherwise
        """
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT 1;")
                    result = cursor.fetchone()
                    logger.info("Database connection test successful")
                    return result[0] == 1
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False

    def get_config_summary(self) -> Dict[str, Any]:
        """
        Get a summary of the current configuration.
        Sensitive information is masked.

        Returns:
            Dictionary with configuration summary
        """
        return {
            "environment": self.environment,
            "db_url_configured": bool(self.db_url),
            "start_date": self.start_date,
            "end_date": self.end_date,
            "output_dir": self.output.output_dir,
            "db_connection_timeout": self.db_connection_timeout,
            "db_query_timeout": self.db_query_timeout,
            "db_max_retries": self.db_max_retries,
        }
