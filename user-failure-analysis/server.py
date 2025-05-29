#!/usr/bin/env python3
"""
FastAPI server for user failure analysis with caching support.
"""

import os
import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from datetime import datetime, timedelta
from typing import Optional
import logging
from pathlib import Path

from main import run_analysis

# Configure logging
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

app = FastAPI(title="User Failure Analysis API")

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class AnalysisRequest(BaseModel):
    from_date: str
    to_date: str
    max_users: Optional[int] = 15
    force_refresh: Optional[bool] = False


def is_cache_valid(file_path: str) -> bool:
    """Check if cached file exists and is within cache duration."""
    if not Path(file_path).exists():
        return False

    cache_duration = int(os.getenv("CACHE_DURATION_HOURS", "24"))
    file_age = datetime.now() - datetime.fromtimestamp(Path(file_path).stat().st_mtime)
    return file_age < timedelta(hours=cache_duration)


def get_report_paths(from_date: str, to_date: str) -> tuple:
    """Get paths for HTML and CSV report files."""
    # Get the analysis output directory
    analysis_output_dir = os.getenv("OUTPUT_DIR", "./output")

    # Get the dashboard's public/reports directory
    dashboard_dir = os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
        "Internal-Dashboard",
        "public",
        "reports",
    )

    # Create both directories if they don't exist
    os.makedirs(analysis_output_dir, exist_ok=True)
    os.makedirs(dashboard_dir, exist_ok=True)

    date_range = f"from_{from_date.replace('-', '')}_to_{to_date.replace('-', '')}"

    # Analysis paths
    analysis_html_path = os.path.join(
        analysis_output_dir, f"user_failure_rates_{date_range}.html"
    )
    analysis_csv_path = os.path.join(
        analysis_output_dir, f"user_failure_stats_{date_range}.csv"
    )

    # Dashboard paths
    dashboard_html_path = os.path.join(
        dashboard_dir, f"user_failure_rates_{date_range}.html"
    )
    dashboard_csv_path = os.path.join(
        dashboard_dir, f"user_failure_stats_{date_range}.csv"
    )

    return (
        analysis_html_path,
        analysis_csv_path,
        dashboard_html_path,
        dashboard_csv_path,
    )


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy", "timestamp": datetime.now().isoformat()}


@app.post("/api/analyze")
async def analyze_data(request: AnalysisRequest):
    """
    Analyze user failure data for the specified date range.
    Uses caching to avoid unnecessary regeneration of reports.
    """
    try:
        # Validate dates
        try:
            from_date = datetime.strptime(request.from_date, "%Y-%m-%d")
            to_date = datetime.strptime(request.to_date, "%Y-%m-%d")
            if from_date > to_date:
                raise ValueError("Start date cannot be after end date")
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))

        # Get report file paths
        (
            analysis_html_path,
            analysis_csv_path,
            dashboard_html_path,
            dashboard_csv_path,
        ) = get_report_paths(request.from_date, request.to_date)

        # Check if we need to regenerate the reports
        force_refresh = (
            request.force_refresh or os.getenv("FORCE_REFRESH", "").lower() == "true"
        )
        cache_valid = (
            is_cache_valid(analysis_html_path)
            and is_cache_valid(analysis_csv_path)
            and is_cache_valid(dashboard_html_path)
            and is_cache_valid(dashboard_csv_path)
        )

        if not force_refresh and cache_valid:
            logger.info("Using cached reports")
            return {
                "success": True,
                "message": "Using cached reports",
                "data": {
                    "from_date": request.from_date,
                    "to_date": request.to_date,
                    "html_path": dashboard_html_path,
                    "csv_path": dashboard_csv_path,
                    "cached": True,
                },
            }

        # Set output directory for analysis
        os.environ["OUTPUT_DIR"] = os.path.dirname(analysis_html_path)

        # Run analysis
        logger.info("Generating new reports...")
        result = run_analysis(
            from_date=request.from_date,
            to_date=request.to_date,
            max_users=request.max_users,
        )

        if not result.get("success", False):
            raise HTTPException(
                status_code=500,
                detail=f"Analysis failed: {result.get('error', 'Unknown error')}",
            )

        # Copy files to dashboard directory
        try:
            import shutil

            shutil.copy2(analysis_html_path, dashboard_html_path)
            shutil.copy2(analysis_csv_path, dashboard_csv_path)
            logger.info("Copied report files to dashboard directory")
        except Exception as e:
            logger.error(f"Error copying files to dashboard: {str(e)}")
            raise HTTPException(
                status_code=500,
                detail="Failed to copy report files to dashboard",
            )

        return {
            "success": True,
            "message": "Analysis completed successfully",
            "data": {
                "from_date": request.from_date,
                "to_date": request.to_date,
                "html_path": dashboard_html_path,
                "csv_path": dashboard_csv_path,
                "cached": False,
                "analysis_result": result.get("data", {}),
            },
        }

    except Exception as e:
        logger.error(f"Error in analyze_data: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    port = int(os.getenv("PORT", "8000"))
    log_level = os.getenv("LOG_LEVEL", "info").lower()

    logger.info(f"Starting server on http://localhost:{port}")
    uvicorn.run(app, host="0.0.0.0", port=port, log_level=log_level)
