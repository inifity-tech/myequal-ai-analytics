import os
from pydantic import BaseModel
from datetime import datetime, timedelta
from typing import Optional
import logging
from pathlib import Path
import azure.functions as func
import json

from main import run_analysis

# Configure logging
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)


class AnalysisRequest(BaseModel):
    from_date: str
    to_date: str
    max_users: Optional[int] = 15
    force_refresh: Optional[bool] = False


@app.route(route="http_trigger")
def http_trigger(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Python HTTP trigger function processed a request.")

    name = req.params.get("name")
    if not name:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            name = req_body.get("name")

    if name:
        return func.HttpResponse(
            f"Hello, {name}. This HTTP triggered function executed successfully."
        )
    else:
        return func.HttpResponse(
            "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.",
            status_code=200,
        )


@app.route(route="health")
def health_check(req: func.HttpRequest) -> func.HttpResponse:
    """Health check endpoint."""
    return func.HttpResponse(
        json.dumps(
            {
                "status": "healthy",
                "timestamp": datetime.utcnow().isoformat(),
                "version": "1.0.0",
            }
        ),
        status_code=200,
        mimetype="application/json",
    )


def is_cache_valid(file_path: str) -> bool:
    """Check if cached file exists and is within cache duration."""
    if not Path(file_path).exists():
        return False

    cache_duration = int(os.getenv("CACHE_DURATION_HOURS", "24"))
    file_age = datetime.now() - datetime.fromtimestamp(Path(file_path).stat().st_mtime)
    return file_age < timedelta(hours=cache_duration)


def get_report_paths(from_date: str, to_date: str) -> tuple:
    """Get paths for HTML and CSV report files."""
    # Use a standard output directory
    output_dir = os.getenv("OUTPUT_DIR", "./output")

    # Ensure the output directory exists
    os.makedirs(output_dir, exist_ok=True)

    # Generate file names based on date range
    date_range = f"from_{from_date.replace('-', '')}_to_{to_date.replace('-', '')}"

    # Generate file paths
    html_path = os.path.join(output_dir, f"user_failure_rates_{date_range}.html")
    csv_path = os.path.join(output_dir, f"user_failure_stats_{date_range}.csv")

    return (html_path, csv_path, html_path, csv_path)


@app.route(route="analyze", methods=[func.HttpMethod.POST])
def user_failure_analysis(req: func.HttpRequest) -> func.HttpResponse:
    """
    Analyze user failure data for the specified date range.
    Uses caching to avoid unnecessary regeneration of reports.
    """
    try:
        # Parse request body
        try:
            request_data = req.get_json()
            from_date = request_data.get("from_date")
            to_date = request_data.get("to_date")
            max_users = request_data.get("max_users", 15)
            force_refresh = request_data.get("force_refresh", False)

            if not from_date or not to_date:
                return func.HttpResponse(
                    json.dumps({"error": "from_date and to_date are required"}),
                    status_code=400,
                    mimetype="application/json",
                )

        except ValueError:
            return func.HttpResponse(
                json.dumps({"error": "Invalid JSON in request body"}),
                status_code=400,
                mimetype="application/json",
            )

        # Validate dates
        try:
            parsed_from_date = datetime.strptime(from_date, "%Y-%m-%d")
            parsed_to_date = datetime.strptime(to_date, "%Y-%m-%d")
            if parsed_from_date > parsed_to_date:
                return func.HttpResponse(
                    json.dumps({"error": "Start date cannot be after end date"}),
                    status_code=400,
                    mimetype="application/json",
                )
        except ValueError as e:
            return func.HttpResponse(
                json.dumps({"error": f"Invalid date format: {str(e)}"}),
                status_code=400,
                mimetype="application/json",
            )

        # Get report file paths
        (
            html_path,
            csv_path,
            output_html_path,
            output_csv_path,
        ) = get_report_paths(from_date, to_date)

        # Check if we need to regenerate the reports
        force_refresh = (
            force_refresh or os.getenv("FORCE_REFRESH", "").lower() == "true"
        )
        cache_valid = is_cache_valid(html_path) and is_cache_valid(csv_path)

        if not force_refresh and cache_valid:
            logger.info("Using cached reports")
            return func.HttpResponse(
                json.dumps(
                    {
                        "success": True,
                        "message": "Using cached reports",
                        "data": {
                            "from_date": from_date,
                            "to_date": to_date,
                            "html_path": html_path,
                            "csv_path": csv_path,
                            "cached": True,
                        },
                    }
                ),
                mimetype="application/json",
            )

        # Run analysis
        logger.info("Generating new reports...")
        result = run_analysis(
            from_date=from_date,
            to_date=to_date,
            max_users=max_users,
            output_dir=os.path.dirname(html_path),
        )

        if not result.get("success", False):
            return func.HttpResponse(
                json.dumps(
                    {"success": False, "error": result.get("error", "Unknown error")}
                ),
                status_code=500,
                mimetype="application/json",
            )

        return func.HttpResponse(
            json.dumps(
                {
                    "success": True,
                    "message": "Analysis completed successfully",
                    "data": {
                        "from_date": from_date,
                        "to_date": to_date,
                        "html_path": result["data"]["html_url"],
                        "csv_path": result["data"]["csv_url"],
                        "cached": False,
                        "analysis_result": result.get("data", {}),
                    },
                }
            ),
            mimetype="application/json",
        )

    except Exception as e:
        logger.error(f"Error in analyze_data: {str(e)}", exc_info=True)
        return func.HttpResponse(
            json.dumps({"success": False, "error": str(e)}),
            status_code=500,
            mimetype="application/json",
        )
