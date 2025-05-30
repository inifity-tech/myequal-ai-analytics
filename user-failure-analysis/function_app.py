import os
import uvicorn
from fastapi import HTTPException
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
    logging.info('Python HTTP trigger function processed a request.')

    name = req.params.get('name')
    if not name:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            name = req_body.get('name')

    if name:
        return func.HttpResponse(f"Hello, {name}. This HTTP triggered function executed successfully.")
    else:
        return func.HttpResponse(
             "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.",
             status_code=200
        )
@app.route(route="health")
def health_check(req: func.HttpRequest) -> func.HttpResponse:
    """Health check endpoint."""
    return func.HttpResponse(
        json.dumps({
            "status": "healthy",
            "timestamp": datetime.utcnow().isoformat(),
            "version": "1.0.0"
        }),
        status_code=200,
        mimetype="application/json"
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
        # Validate dates
        print("Received request:", req.get_body())
        try:
            from_date = datetime.strptime(req.from_date, "%Y-%m-%d")
            to_date = datetime.strptime(req.to_date, "%Y-%m-%d")
            if from_date > to_date:
                raise ValueError("Start date cannot be after end date")
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))

        # Get report file paths
        (
            html_path,
            csv_path,
            output_html_path,
            output_csv_path,
        ) = get_report_paths(req.from_date, req.to_date)

        # Check if we need to regenerate the reports
        force_refresh = (
            req.force_refresh or os.getenv("FORCE_REFRESH", "").lower() == "true"
        )
        cache_valid = is_cache_valid(html_path) and is_cache_valid(csv_path)

        if not force_refresh and cache_valid:
            logger.info("Using cached reports")
            return {
                "success": True,
                "message": "Using cached reports",
                "data": {
                    "from_date": req.from_date,
                    "to_date": req.to_date,
                    "html_path": html_path,
                    "csv_path": csv_path,
                    "cached": True,
                },
            }

        # Run analysis
        logger.info("Generating new reports...")
        result = run_analysis(
            from_date=req.from_date,
            to_date=req.to_date,
            max_users=req.max_users,
            output_dir=os.path.dirname(html_path),
        )

        if not result.get("success", False):
            raise HTTPException(
                status_code=500,
                detail=f"Analysis failed: {result.get('error', 'Unknown error')}",
            )

        return {
            "success": True,
            "message": "Analysis completed successfully",
            "data": {
                "from_date": req.from_date,
                "to_date": req.to_date,
                "html_path": result["data"]["html_url"],
                "csv_path": result["data"]["csv_url"],
                "cached": False,
                "analysis_result": result.get("data", {}),
            },
        }

    except Exception as e:
        logger.error(f"Error in analyze_data: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
