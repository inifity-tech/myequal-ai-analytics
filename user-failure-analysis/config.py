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
from typing import Dict, Any, Optional
from datetime import datetime, timedelta
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

        # Query dates - default to last 2 days if not specified
        self._setup_default_dates()

        # Database connection settings
        self.db_connection_timeout = int(os.getenv("DB_CONNECTION_TIMEOUT", "30"))
        self.db_query_timeout = int(os.getenv("DB_QUERY_TIMEOUT", "60"))
        self.db_max_retries = int(os.getenv("DB_MAX_RETRIES", "3"))

        # Output configuration
        self.output = OutputConfig(output_dir=os.getenv("OUTPUT_DIR", "./output"))

        logger.debug("Configuration initialized with database and output settings")

    def _setup_default_dates(self):
        """Set up default date range (last 2 days)."""
        end_date = datetime.now()
        start_date = end_date - timedelta(days=2)

        self.start_date = os.getenv(
            "START_DATE", start_date.strftime("%Y-%m-%d %H:%M:%S")
        )
        self.end_date = os.getenv("END_DATE", end_date.strftime("%Y-%m-%d %H:%M:%S"))

        # Parse string dates to datetime objects for validation
        try:
            self._start_datetime = datetime.strptime(
                self.start_date, "%Y-%m-%d %H:%M:%S"
            )
            self._end_datetime = datetime.strptime(self.end_date, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            logger.warning(
                "Invalid date format. Using default date range (last 2 days)."
            )
            self._start_datetime = start_date
            self._end_datetime = end_date
            self.start_date = start_date.strftime("%Y-%m-%d %H:%M:%S")
            self.end_date = end_date.strftime("%Y-%m-%d %H:%M:%S")

    def set_date_range(
        self, from_date: Optional[str] = None, to_date: Optional[str] = None
    ):
        """
        Set custom date range for the analysis.

        Args:
            from_date: Start date in format 'YYYY-MM-DD'
            to_date: End date in format 'YYYY-MM-DD'

        Raises:
            ValueError: If date format is invalid or if from_date is after to_date
        """
        if from_date and to_date:
            try:
                # Parse dates and set time to start/end of day
                start_datetime = datetime.strptime(from_date, "%Y-%m-%d")
                start_datetime = start_datetime.replace(hour=0, minute=0, second=0)

                end_datetime = datetime.strptime(to_date, "%Y-%m-%d")
                end_datetime = end_datetime.replace(hour=23, minute=59, second=59)

                # Validate date range
                if start_datetime > end_datetime:
                    raise ValueError("Start date cannot be after end date")

                # Set date range
                self._start_datetime = start_datetime
                self._end_datetime = end_datetime
                self.start_date = start_datetime.strftime("%Y-%m-%d %H:%M:%S")
                self.end_date = end_datetime.strftime("%Y-%m-%d %H:%M:%S")

                logger.info(
                    f"Custom date range set: {self.start_date} to {self.end_date}"
                )
            except ValueError as e:
                logger.error(f"Invalid date format: {e}")
                raise ValueError(f"Invalid date format: {e}")
        elif from_date or to_date:
            # If only one date is provided, use default for the other
            self.set_date_range(
                from_date=from_date
                or (datetime.now() - timedelta(days=2)).strftime("%Y-%m-%d"),
                to_date=to_date or datetime.now().strftime("%Y-%m-%d"),
            )

    def get_date_range_description(self) -> str:
        """
        Get a formatted description of the date range.

        Returns:
            String description like 'from_20250520_to_20250522'
        """
        start_str = self._start_datetime.strftime("%Y%m%d")
        end_str = self._end_datetime.strftime("%Y%m%d")
        return f"from_{start_str}_to_{end_str}"

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
        Get summary of configuration for logging or debugging.

        Returns:
            Dict with configuration summary
        """
        return {
            "environment": self.environment,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "output_dir": self.output.output_dir,
            "connection_timeout": self.db_connection_timeout,
            "query_timeout": self.db_query_timeout,
            "max_retries": self.db_max_retries,
        }
