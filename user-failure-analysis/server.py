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
            html_path,
            csv_path,
            output_html_path,
            output_csv_path,
        ) = get_report_paths(request.from_date, request.to_date)

        # Check if we need to regenerate the reports
        force_refresh = (
            request.force_refresh or os.getenv("FORCE_REFRESH", "").lower() == "true"
        )
        cache_valid = is_cache_valid(html_path) and is_cache_valid(csv_path)

        if not force_refresh and cache_valid:
            logger.info("Using cached reports")
            return {
                "success": True,
                "message": "Using cached reports",
                "data": {
                    "from_date": request.from_date,
                    "to_date": request.to_date,
                    "html_path": html_path,
                    "csv_path": csv_path,
                    "cached": True,
                },
            }

        # Run analysis
        logger.info("Generating new reports...")
        result = run_analysis(
            from_date=request.from_date,
            to_date=request.to_date,
            max_users=request.max_users,
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
                "from_date": request.from_date,
                "to_date": request.to_date,
                "html_path": result["data"]["html_url"],
                "csv_path": result["data"]["csv_url"],
                "cached": False,
                "analysis_result": result.get("data", {}),
            },
        }

    except Exception as e:
        logger.error(f"Error in analyze_data: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    logger.info("Starting server on http://localhost:8000")
    uvicorn.run(app, host="0.0.0.0", port=8000)