"""
Azure Function implementation for user failure rate analysis.
Can be deployed as a serverless function in Azure.
"""

import json
import logging
import os
import traceback
from datetime import datetime, timedelta
import azure.functions as func
from config import Config, DatabaseError
from analyzer import analyze_failure_data, AnalysisError

# Configure Azure Functions logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Constants
DEFAULT_TIMEOUT_SECONDS = 120
VERSION = "1.0.0"
SERVICE_NAME = "user-failure-analysis"


def main(req: func.HttpRequest) -> func.HttpResponse:
    """
    Azure Function entry point for failure rate analysis.

    Args:
        req: HTTP request object

    Returns:
        HTTP response with analysis results
    """
    start_time = datetime.now()
    logger.info(f"{SERVICE_NAME} function received a request")

    # Check request timeout
    timeout_seconds = int(os.getenv("REQUEST_TIMEOUT_SECONDS", DEFAULT_TIMEOUT_SECONDS))
    timeout = start_time + timedelta(seconds=timeout_seconds)

    try:
        # Parse request parameters
        request_data = _parse_request(req)

        # Validate request
        validation_result = _validate_request(request_data)
        if not validation_result["valid"]:
            return func.HttpResponse(
                json.dumps(
                    {
                        "status": "error",
                        "timestamp": datetime.now().isoformat(),
                        "error_message": validation_result["error"],
                        "error_type": "ValidationError",
                    }
                ),
                status_code=400,
                headers={"Content-Type": "application/json"},
            )

        # Initialize configuration
        config = Config()

        # Validate configuration
        if not config.validate():
            return func.HttpResponse(
                json.dumps(
                    {
                        "status": "error",
                        "timestamp": datetime.now().isoformat(),
                        "error_message": "Invalid configuration. Please check environment variables.",
                        "error_type": "ConfigurationError",
                    }
                ),
                status_code=500,
                headers={"Content-Type": "application/json"},
            )

        # Create output directory
        os.makedirs(config.output.output_dir, exist_ok=True)

        # Run analysis with database query
        results = _run_database_analysis(config, request_data, timeout)

        # Prepare response
        response_data = {
            "status": "success" if results["success"] else "error",
            "timestamp": datetime.now().isoformat(),
            "execution_time": results.get("execution_time"),
            "data_summary": results.get("data_summary", {}),
            "generated_files_count": len(results.get("generated_files", [])),
            "error_message": results.get("error_message"),
            "version": VERSION,
        }

        # Include detailed results if requested
        if request_data.get("include_details", False):
            response_data["generated_files"] = results.get("generated_files", [])

        return func.HttpResponse(
            json.dumps(response_data, indent=2),
            status_code=200 if results["success"] else 500,
            headers={"Content-Type": "application/json"},
        )

    except Exception as e:
        logger.error(f"Azure Function execution failed: {e}")
        logger.error(f"Traceback: {traceback.format_exc()}")

        error_response = {
            "status": "error",
            "timestamp": datetime.now().isoformat(),
            "error_message": str(e),
            "error_type": type(e).__name__,
            "version": VERSION,
        }

        return func.HttpResponse(
            json.dumps(error_response, indent=2),
            status_code=500,
            headers={"Content-Type": "application/json"},
        )


def _parse_request(req: func.HttpRequest) -> dict:
    """
    Parse and validate HTTP request parameters.

    Args:
        req: HTTP request object

    Returns:
        Dictionary containing parsed request data
    """
    request_data = {}

    try:
        # Try to parse JSON body
        if req.get_body():
            body_data = req.get_json()
            if body_data:
                request_data.update(body_data)
    except Exception:
        logger.warning("Failed to parse JSON body, using query parameters only")

    # Parse query parameters with defaults
    request_data.update(
        {
            "export_csv": req.params.get("export_csv", "true").lower() == "true",
            "include_details": req.params.get("include_details", "false").lower()
            == "true",
            "max_users": int(req.params.get("max_users", "15")),
        }
    )

    # Handle date parameters - both specific dates and date range
    if req.params.get("from_date") or req.params.get("to_date"):
        request_data["from_date"] = req.params.get("from_date")
        request_data["to_date"] = req.params.get("to_date")
    elif req.params.get("date_range_days"):
        request_data["date_range_days"] = int(req.params.get("date_range_days", "2"))

    logger.info(f"Parsed request data: {request_data}")
    return request_data


def _validate_request(request_data: dict) -> dict:
    """
    Validate the request parameters.

    Args:
        request_data: Parsed request data

    Returns:
        Dictionary with validation result
    """
    try:
        # Validate date range or specific dates
        if "from_date" in request_data or "to_date" in request_data:
            # Validate date format if provided
            date_format = "%Y-%m-%d"

            if "from_date" in request_data and request_data["from_date"]:
                try:
                    datetime.strptime(request_data["from_date"], date_format)
                except ValueError:
                    return {
                        "valid": False,
                        "error": "from_date must be in YYYY-MM-DD format",
                    }

            if "to_date" in request_data and request_data["to_date"]:
                try:
                    datetime.strptime(request_data["to_date"], date_format)
                except ValueError:
                    return {
                        "valid": False,
                        "error": "to_date must be in YYYY-MM-DD format",
                    }

            # If both dates are provided, validate to_date is not before from_date
            if (
                request_data.get("from_date")
                and request_data.get("to_date")
                and datetime.strptime(request_data["from_date"], date_format)
                > datetime.strptime(request_data["to_date"], date_format)
            ):
                return {
                    "valid": False,
                    "error": "from_date cannot be after to_date",
                }

        elif "date_range_days" in request_data:
            days = request_data["date_range_days"]
            if days <= 0 or days > 30:
                return {
                    "valid": False,
                    "error": "date_range_days must be between 1 and 30",
                }

        # Validate max users
        if "max_users" in request_data:
            max_users = request_data["max_users"]
            if max_users <= 0 or max_users > 100:
                return {"valid": False, "error": "max_users must be between 1 and 100"}

        return {"valid": True}
    except Exception as e:
        return {"valid": False, "error": f"Request validation error: {str(e)}"}


def _run_database_analysis(
    config: Config, request_data: dict, timeout: datetime
) -> dict:
    """
    Run analysis using database query.

    Args:
        config: Config instance
        request_data: Parsed request data
        timeout: Timeout datetime

    Returns:
        Analysis results dictionary
    """
    logger.info("Running database analysis in Azure Function")

    results = {
        "success": False,
        "data_summary": {},
        "generated_files": [],
        "error_message": None,
        "execution_time": None,
    }

    start_time = datetime.now()

    try:
        # Check for timeout
        if datetime.now() >= timeout:
            raise TimeoutError("Function timed out before processing could begin")

        # Apply date range parameters - prioritize from_date/to_date over date_range_days
        date_range_desc = None

        if "from_date" in request_data or "to_date" in request_data:
            # Use specific dates if provided
            from_date = request_data.get("from_date")
            to_date = request_data.get("to_date")

            config.set_date_range(from_date=from_date, to_date=to_date)
            date_range_desc = config.get_date_range_description()

            logger.info(
                f"Using custom date range: {config.start_date} to {config.end_date}"
            )
        elif "date_range_days" in request_data:
            # Otherwise use date_range_days if provided
            days = request_data["date_range_days"]
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days)

            config.end_date = end_date.strftime("%Y-%m-%d %H:%M:%S")
            config.start_date = start_date.strftime("%Y-%m-%d %H:%M:%S")

            # Create description for filenames
            date_range_desc = f"last_{days}days"

            logger.info(
                f"Using date range of last {days} days: {config.start_date} to {config.end_date}"
            )

        # Test database connection
        logger.info("Testing database connection...")
        if not config.test_connection():
            raise DatabaseError("Database connection test failed")

        # Execute query and fetch data
        data = config.execute_query()

        if data.empty:
            logger.warning("Query returned no results")
            results["error_message"] = "No data found for the specified date range"
            return results

        logger.info(f"Retrieved {len(data)} records from database")

        # Check for timeout before analysis
        if datetime.now() >= timeout:
            raise TimeoutError("Function timed out after retrieving data")

        # Analyze the data with date range description for filenames
        analyzer = analyze_failure_data(data, config.output.output_dir, date_range_desc)
        summary_stats = analyzer.get_summary_statistics()

        results["data_summary"] = summary_stats

        # Export to CSV if requested
        if request_data.get("export_csv", True):
            csv_filepath = analyzer.export_to_csv()
            results["generated_files"].append(csv_filepath)

        # Check for timeout before visualization
        if datetime.now() >= timeout:
            raise TimeoutError("Function timed out before visualization could complete")

        # Generate visualization with custom max users
        max_users = request_data.get("max_users", 15)
        plot_filepath = analyzer.create_interactive_bar_plot(top_n=max_users)
        results["generated_files"].append(plot_filepath)

        results["success"] = True
        execution_time = (datetime.now() - start_time).total_seconds()
        results["execution_time"] = execution_time

        logger.info(f"Analysis completed successfully in {execution_time:.2f} seconds")
        logger.info(f"Generated {len(results['generated_files'])} output files")

    except TimeoutError as e:
        error_msg = f"Timeout error: {e}"
        logger.error(error_msg)
        results["error_message"] = error_msg

    except DatabaseError as e:
        error_msg = f"Database error: {e}"
        logger.error(error_msg)
        results["error_message"] = error_msg

    except AnalysisError as e:
        error_msg = f"Analysis error: {e}"
        logger.error(error_msg)
        results["error_message"] = error_msg

    except Exception as e:
        error_msg = f"Unexpected error: {e}"
        logger.error(error_msg)
        logger.error(f"Traceback: {traceback.format_exc()}")
        results["error_message"] = error_msg

    return results


# Azure Function app instance
app = func.FunctionApp()


@app.route(route="failure_analysis", auth_level=func.AuthLevel.FUNCTION)
def failure_analysis_endpoint(req: func.HttpRequest) -> func.HttpResponse:
    """
    HTTP endpoint for failure rate analysis.

    Args:
        req: HTTP request object

    Returns:
        HTTP response with analysis results
    """
    return main(req)


@app.route(route="health", auth_level=func.AuthLevel.ANONYMOUS)
def health_check(req: func.HttpRequest) -> func.HttpResponse:
    """
    Health check endpoint for the Azure Function.

    Args:
        req: HTTP request object

    Returns:
        HTTP response indicating service health
    """
    try:
        # Basic health check
        health_data = {
            "status": "healthy",
            "timestamp": datetime.now().isoformat(),
            "version": VERSION,
            "service": SERVICE_NAME,
            "environment": os.getenv("AZURE_FUNCTIONS_ENVIRONMENT", "unknown"),
        }

        # Test configuration loading
        from config import Config

        config = Config()
        health_data["config_valid"] = config.validate()

        # Test database connection if requested
        test_db = req.params.get("test_db", "false").lower() == "true"
        if test_db:
            health_data["db_connection"] = config.test_connection()

        return func.HttpResponse(
            json.dumps(health_data, indent=2),
            status_code=200,
            headers={"Content-Type": "application/json"},
        )

    except Exception as e:
        error_data = {
            "status": "unhealthy",
            "timestamp": datetime.now().isoformat(),
            "error": str(e),
            "version": VERSION,
            "service": SERVICE_NAME,
        }

        return func.HttpResponse(
            json.dumps(error_data, indent=2),
            status_code=500,
            headers={"Content-Type": "application/json"},
        )
