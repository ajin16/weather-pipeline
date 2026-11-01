"""
main.py
-------
FastAPI web application exposing endpoints to trigger the Weather ETL pipeline,
check database health, and view historical ETL audit logs.

Endpoints:
  - GET  /              : Root service summary
  - GET  /health        : Health check verifying database connection and ping
  - POST /trigger-etl   : Triggers the ETL pipeline (supports sync or background mode)
  - GET  /etl/runs      : Retrieves recent ETL pipeline execution logs
  - GET  /etl/latest    : Retrieves latest recorded weather data per city
"""

from __future__ import annotations

import logging
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from fastapi import BackgroundTasks, Depends, FastAPI, Header, HTTPException, Query, Request, status
from fastapi.responses import HTMLResponse, JSONResponse
from sqlalchemy import desc, select, text

from config import settings
from etl import etl_pipeline_runs_table, get_db_engine, run_etl, weather_hourly_table
from web_ui import get_dashboard_html

logger = logging.getLogger("weather_etl.api")


# ------------------------------------------------------------------------------
# Security Dependency: API Key Authentication (Optional / Configurable)
# ------------------------------------------------------------------------------
async def verify_api_key(
    x_api_key: Optional[str] = Header(None, alias="X-API-Key", description="API Secret Key"),
) -> None:
    """
    Validates incoming requests against settings.API_SECRET_KEY if configured.
    If API_SECRET_KEY is None or empty, requests are permitted.
    """
    if settings.API_SECRET_KEY:
        if not x_api_key or x_api_key != settings.API_SECRET_KEY:
            logger.warning("Unauthorized access attempt to secured endpoint.")
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid or missing X-API-Key header.",
            )


# ------------------------------------------------------------------------------
# Application Lifespan Context Manager
# ------------------------------------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Actions performed on application startup and shutdown."""
    logger.info(f"Starting Weather ETL Service [Env: {settings.ENVIRONMENT}]")
    logger.info(f"Target cities configured: {[c.name for c in settings.get_cities()]}")
    # Verify DB connectivity on boot
    try:
        engine = get_db_engine()
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        logger.info("Successfully established connection to MySQL database.")
    except Exception as e:
        logger.warning(f"Could not connect to database on startup: {e}. Check DATABASE_URL.")
    yield
    logger.info("Shutting down Weather ETL Service.")


# ------------------------------------------------------------------------------
# FastAPI App Definition
# ------------------------------------------------------------------------------
app = FastAPI(
    title="Open-Meteo Weather ETL Pipeline",
    description="Production-grade ETL pipeline extracting weather data, transforming metrics, and loading to MySQL.",
    version="1.0.0",
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
)


# ------------------------------------------------------------------------------
# Routes
# ------------------------------------------------------------------------------
@app.get("/", tags=["General"], response_class=HTMLResponse)
async def root(request: Request, format: Optional[str] = Query(None)):
    """
    Python Web Application entry point:
      - Returns the interactive HTML dashboard when visited in a web browser.
      - Returns service JSON metadata if queried with format=json or Accept: application/json.
    """
    accept = request.headers.get("accept", "")
    if format == "json" or ("application/json" in accept and "text/html" not in accept):
        return JSONResponse({
            "service": "Open-Meteo Weather ETL Pipeline",
            "version": "1.0.0",
            "status": "online",
            "documentation": "/docs",
            "trigger_endpoint": "POST /trigger-etl",
            "health_check": "GET /health",
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        })
    return HTMLResponse(content=get_dashboard_html(), status_code=200)


@app.post(
    "/web-trigger-etl",
    tags=["WebUI"],
    summary="Trigger ETL Pipeline from Web Application Button",
)
async def web_trigger_etl(
    background_tasks: BackgroundTasks,
    background: bool = Query(
        False,
        description="If True, runs asynchronously in background. If False, runs synchronously and returns full result.",
    ),
) -> Dict[str, Any]:
    """
    Dedicated endpoint called directly by the interactive web application button.
    Allows single-click execution with real-time feedback.
    """
    if background:
        job_id = f"bg_{uuid.uuid4().hex[:8]}"
        logger.info(f"Web UI requested background ETL execution [{job_id}]")
        background_tasks.add_task(run_etl, trigger_type="web_ui_background")
        return {
            "status": "ACCEPTED",
            "message": "ETL pipeline scheduled to run asynchronously in the background.",
            "job_id": job_id,
            "scheduled_at_utc": datetime.now(timezone.utc).isoformat(),
        }

    logger.info("Web UI triggered synchronous ETL pipeline execution.")
    result = run_etl(trigger_type="web_ui")

    if result.get("status") == "FAILED":
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content=result,
        )

    return result


@app.get(
    "/etl/summary",
    tags=["ETL"],
    summary="Dashboard Summary Metrics",
)
async def get_etl_summary() -> Dict[str, Any]:
    """Retrieves live aggregate statistics for the web application dashboard."""
    try:
        engine = get_db_engine()
        with engine.connect() as conn:
            total_records = conn.execute(text("SELECT COUNT(*) FROM weather_hourly")).scalar() or 0
            total_runs = conn.execute(text("SELECT COUNT(*) FROM etl_pipeline_runs")).scalar() or 0
            latest_run = conn.execute(
                text("SELECT run_id, status, duration_seconds, started_at FROM etl_pipeline_runs ORDER BY started_at DESC LIMIT 1")
            ).mappings().first()

            return {
                "status": "healthy",
                "database_name": settings.DB_NAME,
                "database_host": settings.DB_HOST,
                "cities_count": len(settings.get_cities()),
                "total_records": int(total_records),
                "total_runs": int(total_runs),
                "latest_run": dict(latest_run) if latest_run else None,
            }
    except Exception as e:
        logger.error(f"Failed to fetch summary: {e}")
        return {
            "status": "degraded",
            "database_name": settings.DB_NAME,
            "database_host": settings.DB_HOST,
            "cities_count": len(settings.get_cities()),
            "total_records": 0,
            "total_runs": 0,
            "error": str(e),
        }


@app.get("/health", tags=["Health"])
async def health_check() -> Dict[str, Any]:
    """
    Health check endpoint verifying application uptime and live database connectivity.
    Returns HTTP 200 if database is reachable, HTTP 503 if unavailable.
    """
    db_status = "unhealthy"
    db_error = None
    try:
        engine = get_db_engine()
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        db_status = "connected"
    except Exception as e:
        db_error = str(e)
        logger.error(f"Health check failed: {db_error}")

    response_payload = {
        "status": "healthy" if db_status == "connected" else "degraded",
        "database": db_status,
        "database_host": settings.DB_HOST,
        "database_name": settings.DB_NAME,
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
    }
    if db_error:
        response_payload["database_error"] = db_error
        return JSONResponse(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, content=response_payload)

    return response_payload


@app.post(
    "/trigger-etl",
    tags=["ETL"],
    summary="Trigger ETL Pipeline Run",
    dependencies=[Depends(verify_api_key)],
)
async def trigger_etl(
    background_tasks: BackgroundTasks,
    background: bool = Query(
        False,
        description="If True, runs asynchronously in background and returns immediately with 202. If False, runs synchronously and returns full result.",
    ),
) -> Dict[str, Any]:
    """
    Triggers the ETL pipeline to extract weather metrics, transform records, and upsert into MySQL.
    
    Can be scheduled by external cron services (cron-job.org, Render Cron, GitHub Actions).
    """
    if background:
        job_id = f"bg_{uuid.uuid4().hex[:8]}"
        logger.info(f"Received request to schedule background ETL run [{job_id}]")
        background_tasks.add_task(run_etl, trigger_type="fastapi_background")
        return {
            "status": "ACCEPTED",
            "message": "ETL pipeline scheduled to run asynchronously in the background.",
            "job_id": job_id,
            "scheduled_at_utc": datetime.now(timezone.utc).isoformat(),
        }

    logger.info("Received request to execute synchronous ETL run.")
    result = run_etl(trigger_type="fastapi_sync")

    if result.get("status") == "FAILED":
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content=result,
        )

    return result


@app.get(
    "/etl/runs",
    tags=["ETL"],
    summary="List recent ETL pipeline runs",
)
async def list_etl_runs(
    limit: int = Query(10, ge=1, le=100, description="Number of recent runs to return"),
) -> List[Dict[str, Any]]:
    """Retrieves the history of recent ETL pipeline executions from the audit table."""
    try:
        engine = get_db_engine()
        with engine.connect() as conn:
            stmt = select(etl_pipeline_runs_table).order_by(desc(etl_pipeline_runs_table.c.started_at)).limit(limit)
            result = conn.execute(stmt)
            rows = result.mappings().all()

            runs = []
            for row in rows:
                runs.append({
                    "run_id": row["run_id"],
                    "trigger_type": row["trigger_type"],
                    "status": row["status"],
                    "cities_count": row["cities_count"],
                    "records_extracted": row["records_extracted"],
                    "records_transformed": row["records_transformed"],
                    "records_loaded": row["records_loaded"],
                    "duration_seconds": row["duration_seconds"],
                    "started_at": row["started_at"].isoformat() if row["started_at"] else None,
                    "completed_at": row["completed_at"].isoformat() if row["completed_at"] else None,
                    "error_message": row["error_message"],
                })
            return runs
    except Exception as e:
        logger.error(f"Failed to fetch ETL runs history: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@app.get(
    "/etl/latest",
    tags=["Weather"],
    summary="Get latest weather record per city",
)
async def get_latest_weather() -> List[Dict[str, Any]]:
    """Retrieves the most recent weather observation recorded in MySQL for each city."""
    try:
        engine = get_db_engine()
        with engine.connect() as conn:
            # Query the latest record for each city using a subquery
            query = text("""
                SELECT w.city_name, w.latitude, w.longitude, w.recorded_at, 
                       w.temperature_2m, w.relative_humidity_2m, w.precipitation, w.updated_at
                FROM weather_hourly w
                INNER JOIN (
                    SELECT city_name, MAX(recorded_at) AS max_recorded_at
                    FROM weather_hourly
                    GROUP BY city_name
                ) latest ON w.city_name = latest.city_name AND w.recorded_at = latest.max_recorded_at
                ORDER BY w.city_name ASC;
            """)
            result = conn.execute(query)
            rows = result.mappings().all()

            latest_records = []
            for r in rows:
                latest_records.append({
                    "city_name": r["city_name"],
                    "latitude": float(r["latitude"]),
                    "longitude": float(r["longitude"]),
                    "recorded_at": r["recorded_at"].isoformat() if r["recorded_at"] else None,
                    "temperature_2m": r["temperature_2m"],
                    "relative_humidity_2m": r["relative_humidity_2m"],
                    "precipitation": r["precipitation"],
                    "updated_at": r["updated_at"].isoformat() if r["updated_at"] else None,
                })
            return latest_records
    except Exception as e:
        logger.error(f"Failed to fetch latest weather metrics: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


# ------------------------------------------------------------------------------
# Local CLI Runner
# ------------------------------------------------------------------------------
if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host=settings.HOST, port=settings.PORT, reload=True)
