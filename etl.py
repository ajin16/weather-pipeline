"""
etl.py
------
Modular, production-ready ETL pipeline for extracting Open-Meteo weather data,
transforming it into normalized records, and incrementally loading it into MySQL.

Architecture:
  - Extract:   Concurrent/resilient HTTP requests via requests.Session with exponential retries.
  - Transform: ISO timestamp normalization (UTC), schema validation, type casting, null handling.
  - Load:      Incremental bulk upsert using MySQL's `INSERT ... ON DUPLICATE KEY UPDATE`.
  - Runner:    Execution coordinator with metrics tracking, run auditing, and structured logging.
"""

from __future__ import annotations

import logging
import sys
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests
from requests.adapters import HTTPAdapter
from sqlalchemy import (
    DECIMAL,
    Column,
    DateTime,
    Float,
    Integer,
    MetaData,
    SmallInteger,
    String,
    Table,
    Text,
    UniqueConstraint,
    create_engine,
    func,
    select,
)
from sqlalchemy.engine import Engine
from urllib3.util.retry import Retry

from config import CityConfig, settings

# ------------------------------------------------------------------------------
# Structured Logging Configuration
# ------------------------------------------------------------------------------
logger = logging.getLogger("weather_etl")
if not logger.handlers:
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(
        fmt="[%(asctime)s] [%(levelname)s] [%(name)s]: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
logger.setLevel(getattr(logging, settings.LOG_LEVEL.upper(), logging.INFO))


# ------------------------------------------------------------------------------
# SQLAlchemy Schema Definitions (Metadata & Tables)
# ------------------------------------------------------------------------------
metadata = MetaData()

weather_hourly_table = Table(
    "weather_hourly",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("city_name", String(64), nullable=False),
    Column("latitude", DECIMAL(8, 5), nullable=False),
    Column("longitude", DECIMAL(8, 5), nullable=False),
    Column("recorded_at", DateTime, nullable=False),
    Column("temperature_2m", Float, nullable=True),
    Column("relative_humidity_2m", SmallInteger, nullable=True),
    Column("precipitation", Float, nullable=True),
    Column("extracted_at", DateTime, server_default=func.now(), nullable=False),
    Column("updated_at", DateTime, server_default=func.now(), onupdate=func.now(), nullable=False),
    UniqueConstraint("city_name", "recorded_at", name="uq_city_recorded"),
)

etl_pipeline_runs_table = Table(
    "etl_pipeline_runs",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("run_id", String(64), nullable=False, unique=True),
    Column("trigger_type", String(32), nullable=False, default="cron"),
    Column("status", String(32), nullable=False),
    Column("cities_count", Integer, nullable=False, default=0),
    Column("records_extracted", Integer, nullable=False, default=0),
    Column("records_transformed", Integer, nullable=False, default=0),
    Column("records_loaded", Integer, nullable=False, default=0),
    Column("error_message", Text, nullable=True),
    Column("started_at", DateTime, server_default=func.now(), nullable=False),
    Column("completed_at", DateTime, nullable=True),
    Column("duration_seconds", Float, nullable=True),
)


# ------------------------------------------------------------------------------
# Engine & Session Management
# ------------------------------------------------------------------------------
_engine: Optional[Engine] = None


def get_db_engine() -> Engine:
    """
    Returns a singleton SQLAlchemy engine with production-grade connection pooling
    and keep-alive pre-ping to prevent stale socket disconnects on cloud providers.
    """
    global _engine
    if _engine is None:
        db_url = settings.get_sqlalchemy_database_url()
        connect_args = settings.get_connect_args()
        logger.info(f"Initializing database engine for host: {settings.DB_HOST}, db: {settings.DB_NAME}")
        _engine = create_engine(
            db_url,
            pool_size=settings.DB_POOL_SIZE,
            max_overflow=settings.DB_MAX_OVERFLOW,
            pool_recycle=settings.DB_POOL_RECYCLE,
            pool_pre_ping=settings.DB_POOL_PRE_PING,
            connect_args=connect_args,
        )
    return _engine


def init_db(engine: Engine) -> None:
    """
    Creates database tables if they do not exist.
    Complements schema.sql for seamless deployment and local testing.
    """
    logger.info("Verifying and initializing database tables...")
    metadata.create_all(engine)
    logger.info("Database tables initialized successfully.")


def get_http_session() -> requests.Session:
    """
    Creates an HTTP requests session configured with exponential backoff retries
    for handling transient Open-Meteo API rate limits and network drops.
    """
    session = requests.Session()
    retry_strategy = Retry(
        total=settings.MAX_RETRIES,
        backoff_factor=settings.RETRY_BACKOFF_FACTOR,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(
        max_retries=retry_strategy,
        pool_connections=10,
        pool_maxsize=10,
    )
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers.update({"User-Agent": "Weather-ETL-Pipeline/1.0.0"})
    return session


# ------------------------------------------------------------------------------
# 1. EXTRACT STEP
# ------------------------------------------------------------------------------
def extract_weather_for_city(
    city: CityConfig,
    session: requests.Session,
) -> Optional[Dict[str, Any]]:
    """
    Fetches raw hourly metrics for a single city from the Open-Meteo REST API.
    
    Args:
        city: CityConfig with name, latitude, longitude, and timezone.
        session: Active HTTP requests session with retries.

    Returns:
        Parsed JSON dictionary payload or None if extraction failed.
    """
    params = {
        "latitude": city.latitude,
        "longitude": city.longitude,
        "hourly": "temperature_2m,relative_humidity_2m,precipitation",
        "timezone": city.timezone,
        "past_days": settings.PAST_DAYS,
        "forecast_days": settings.FORECAST_DAYS,
    }

    logger.info(f"Extracting weather data for {city.name} (lat: {city.latitude}, lon: {city.longitude})...")
    try:
        response = session.get(
            settings.OPEN_METEO_BASE_URL,
            params=params,
            timeout=settings.REQUEST_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
        data = response.json()

        if "hourly" not in data or "time" not in data["hourly"]:
            logger.warning(f"Unexpected API response format for {city.name}: missing 'hourly.time'")
            return None

        total_hours = len(data["hourly"].get("time", []))
        logger.info(f"Successfully extracted {total_hours} hourly points for {city.name}.")
        return data

    except requests.exceptions.RequestException as e:
        logger.error(f"HTTP extraction error for city '{city.name}': {e}", exc_info=False)
        return None
    except Exception as e:
        logger.error(f"Unexpected error extracting data for '{city.name}': {e}", exc_info=True)
        return None


def extract(cities: List[CityConfig]) -> List[Tuple[CityConfig, Dict[str, Any]]]:
    """
    Extracts hourly weather metrics across all configured target cities.
    
    Returns:
        List of tuples: (CityConfig, raw_json_payload).
    """
    logger.info(f"--- Starting Extract Phase: {len(cities)} cities targeted ---")
    results: List[Tuple[CityConfig, Dict[str, Any]]] = []

    with get_http_session() as session:
        for city in cities:
            payload = extract_weather_for_city(city, session)
            if payload:
                results.append((city, payload))

    logger.info(f"--- Extract Phase Complete: Successfully fetched data for {len(results)}/{len(cities)} cities ---")
    return results


# ------------------------------------------------------------------------------
# 2. TRANSFORM STEP
# ------------------------------------------------------------------------------
def _parse_iso_timestamp(ts_str: str) -> Optional[datetime]:
    """
    Parses Open-Meteo ISO 8601 timestamp string into a standardized, naive UTC datetime
    for MySQL DATETIME storage.
    """
    try:
        # Open-Meteo returns 'YYYY-MM-DDTHH:MM' or 'YYYY-MM-DDTHH:MM:SSZ'
        if ts_str.endswith("Z"):
            ts_str = ts_str[:-1] + "+00:00"
        dt = datetime.fromisoformat(ts_str)
        if dt.tzinfo is not None:
            # Convert to UTC and drop tzinfo for standard MySQL DATETIME
            dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
        return dt
    except Exception as e:
        logger.warning(f"Failed to parse timestamp '{ts_str}': {e}")
        return None


def transform_city_payload(
    city: CityConfig,
    payload: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """
    Transforms raw JSON payload from Open-Meteo into a list of normalized, validated dictionaries.
    
    Data Validation Rules:
      - recorded_at: valid parsed datetime
      - temperature_2m: float or None (-90.0°C to +70.0°C sanity check)
      - relative_humidity_2m: int or None (0 to 100 percentage check)
      - precipitation: float or None (non-negative)
      - coordinates: clamped to city definition
    """
    transformed_records: List[Dict[str, Any]] = []
    hourly = payload.get("hourly", {})

    times = hourly.get("time", [])
    temps = hourly.get("temperature_2m", [])
    humidities = hourly.get("relative_humidity_2m", [])
    precips = hourly.get("precipitation", [])

    array_len = len(times)
    if not (len(temps) == array_len and len(humidities) == array_len and len(precips) == array_len):
        logger.error(
            f"Array length mismatch in Open-Meteo response for {city.name}: "
            f"time={len(times)}, temps={len(temps)}, hum={len(humidities)}, precip={len(precips)}"
        )
        return transformed_records

    for i in range(array_len):
        # 1. Parse and validate timestamp
        recorded_at = _parse_iso_timestamp(times[i])
        if recorded_at is None:
            continue

        # 2. Validate temperature
        raw_temp = temps[i]
        valid_temp: Optional[float] = None
        if raw_temp is not None:
            try:
                t = float(raw_temp)
                if -90.0 <= t <= 70.0:
                    valid_temp = round(t, 2)
            except (ValueError, TypeError):
                valid_temp = None

        # 3. Validate relative humidity
        raw_hum = humidities[i]
        valid_hum: Optional[int] = None
        if raw_hum is not None:
            try:
                h = int(round(float(raw_hum)))
                if 0 <= h <= 100:
                    valid_hum = h
            except (ValueError, TypeError):
                valid_hum = None

        # 4. Validate precipitation
        raw_precip = precips[i]
        valid_precip: Optional[float] = None
        if raw_precip is not None:
            try:
                p = float(raw_precip)
                if p >= 0.0:
                    valid_precip = round(p, 2)
            except (ValueError, TypeError):
                valid_precip = None

        record = {
            "city_name": city.name,
            "latitude": round(city.latitude, 5),
            "longitude": round(city.longitude, 5),
            "recorded_at": recorded_at,
            "temperature_2m": valid_temp,
            "relative_humidity_2m": valid_hum,
            "precipitation": valid_precip,
        }
        transformed_records.append(record)

    return transformed_records


def transform(raw_payloads: List[Tuple[CityConfig, Dict[str, Any]]]) -> List[Dict[str, Any]]:
    """
    Transforms raw JSON responses from all cities into normalized tabular records.
    """
    logger.info(f"--- Starting Transform Phase: Processing {len(raw_payloads)} city payloads ---")
    all_clean_records: List[Dict[str, Any]] = []

    for city, payload in raw_payloads:
        records = transform_city_payload(city, payload)
        all_clean_records.extend(records)
        logger.debug(f"Transformed {len(records)} clean records for {city.name}.")

    logger.info(f"--- Transform Phase Complete: Total validated records: {len(all_clean_records)} ---")
    return all_clean_records


# ------------------------------------------------------------------------------
# 3. LOAD STEP (INCREMENTAL UPSERT)
# ------------------------------------------------------------------------------
def load(records: List[Dict[str, Any]], engine: Engine, batch_size: int = 500) -> int:
    """
    Performs an incremental load into the MySQL `weather_hourly` table.
    Uses `INSERT ... ON DUPLICATE KEY UPDATE` on composite key (city_name, recorded_at)
    to update latest weather forecasts/actuals and avoid duplicates.
    
    Supports chunked batch transactions for high performance and low memory footprint.
    
    Returns:
        Number of processed records.
    """
    if not records:
        logger.info("Load Phase: No records to load.")
        return 0

    logger.info(f"--- Starting Load Phase: Upserting {len(records)} records in batches of {batch_size} ---")
    total_loaded = 0
    dialect_name = engine.dialect.name

    for i in range(0, len(records), batch_size):
        batch = records[i : i + batch_size]

        with engine.begin() as conn:
            if dialect_name == "mysql":
                from sqlalchemy.dialects.mysql import insert as mysql_insert

                stmt = mysql_insert(weather_hourly_table).values(batch)
                upsert_stmt = stmt.on_duplicate_key_update(
                    temperature_2m=stmt.inserted.temperature_2m,
                    relative_humidity_2m=stmt.inserted.relative_humidity_2m,
                    precipitation=stmt.inserted.precipitation,
                    latitude=stmt.inserted.latitude,
                    longitude=stmt.inserted.longitude,
                    updated_at=func.now(),
                )
                conn.execute(upsert_stmt)

            elif dialect_name == "sqlite":
                # Fallback support for local mock/unit testing without running MySQL
                from sqlalchemy.dialects.sqlite import insert as sqlite_insert

                stmt = sqlite_insert(weather_hourly_table).values(batch)
                upsert_stmt = stmt.on_conflict_do_update(
                    index_elements=["city_name", "recorded_at"],
                    set_={
                        "temperature_2m": stmt.excluded.temperature_2m,
                        "relative_humidity_2m": stmt.excluded.relative_humidity_2m,
                        "precipitation": stmt.excluded.precipitation,
                        "latitude": stmt.excluded.latitude,
                        "longitude": stmt.excluded.longitude,
                        "updated_at": func.now(),
                    },
                )
                conn.execute(upsert_stmt)

            else:
                # Generic fallback using merge / insert
                for record in batch:
                    conn.execute(weather_hourly_table.insert().values(record))

        total_loaded += len(batch)
        logger.debug(f"Loaded batch {i // batch_size + 1}: {len(batch)} records (Cumulative: {total_loaded})")

    logger.info(f"--- Load Phase Complete: Successfully upserted {total_loaded} records into {dialect_name} ---")
    return total_loaded


# ------------------------------------------------------------------------------
# 4. PIPELINE RUNNER & AUDIT LOGGING
# ------------------------------------------------------------------------------
def _record_audit_start(engine: Engine, run_id: str, trigger_type: str, cities_count: int) -> None:
    """Inserts a run audit row with status STARTED."""
    try:
        with engine.begin() as conn:
            conn.execute(
                etl_pipeline_runs_table.insert().values(
                    run_id=run_id,
                    trigger_type=trigger_type,
                    status="STARTED",
                    cities_count=cities_count,
                    records_extracted=0,
                    records_transformed=0,
                    records_loaded=0,
                    error_message=None,
                )
            )
    except Exception as e:
        logger.warning(f"Could not record audit run start: {e}")


def _record_audit_complete(
    engine: Engine,
    run_id: str,
    status: str,
    extracted: int,
    transformed: int,
    loaded: int,
    duration: float,
    error: Optional[str] = None,
) -> None:
    """Updates the run audit row with final status and metrics."""
    try:
        with engine.begin() as conn:
            conn.execute(
                etl_pipeline_runs_table.update()
                .where(etl_pipeline_runs_table.c.run_id == run_id)
                .values(
                    status=status,
                    records_extracted=extracted,
                    records_transformed=transformed,
                    records_loaded=loaded,
                    duration_seconds=round(duration, 3),
                    completed_at=func.now(),
                    error_message=error,
                )
            )
    except Exception as e:
        logger.warning(f"Could not update audit run completion: {e}")


def run_etl(trigger_type: str = "manual") -> Dict[str, Any]:
    """
    Main entry point for executing the Weather ETL pipeline.
    
    Steps:
      1. Initializes DB tables if not present.
      2. Starts audit logging with unique run_id.
      3. Extracts hourly weather metrics from Open-Meteo API.
      4. Normalizes and validates records.
      5. Loads records incrementally using MySQL ON DUPLICATE KEY UPDATE.
      6. Records execution summary and returns metrics.
      
    Returns:
        Dictionary containing run_id, status, counts, duration, and error (if any).
    """
    start_time = time.time()
    run_id = f"run_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"
    target_cities = settings.get_cities()
    engine = get_db_engine()

    logger.info(f"================================================================")
    logger.info(f"Starting ETL Pipeline Run [{run_id}] | Trigger: {trigger_type}")
    logger.info(f"Target Cities: {[c.name for c in target_cities]}")
    logger.info(f"================================================================")

    # 1. Initialize tables if needed
    try:
        init_db(engine)
    except Exception as e:
        logger.error(f"Failed to verify/initialize database schema: {e}")
        return {
            "run_id": run_id,
            "status": "FAILED",
            "trigger_type": trigger_type,
            "error": f"Database initialization error: {str(e)}",
            "duration_seconds": round(time.time() - start_time, 2),
        }

    # 2. Record audit start
    _record_audit_start(engine, run_id, trigger_type, len(target_cities))

    records_extracted = 0
    records_transformed = 0
    records_loaded = 0

    try:
        # Phase 1: Extract
        raw_payloads = extract(target_cities)
        for _, payload in raw_payloads:
            records_extracted += len(payload.get("hourly", {}).get("time", []))

        # Phase 2: Transform
        transformed_records = transform(raw_payloads)
        records_transformed = len(transformed_records)

        # Phase 3: Load
        records_loaded = load(transformed_records, engine)

        duration = time.time() - start_time
        status = "SUCCESS"
        _record_audit_complete(
            engine,
            run_id=run_id,
            status=status,
            extracted=records_extracted,
            transformed=records_transformed,
            loaded=records_loaded,
            duration=duration,
        )

        logger.info(f"================================================================")
        logger.info(f"ETL Pipeline Run [{run_id}] SUCCESS in {duration:.2f}s")
        logger.info(f"Extracted: {records_extracted} | Transformed: {records_transformed} | Loaded: {records_loaded}")
        logger.info(f"================================================================")

        return {
            "run_id": run_id,
            "status": status,
            "trigger_type": trigger_type,
            "cities_targeted": len(target_cities),
            "cities_successful": len(raw_payloads),
            "records_extracted": records_extracted,
            "records_transformed": records_transformed,
            "records_loaded": records_loaded,
            "duration_seconds": round(duration, 2),
            "completed_at": datetime.now(timezone.utc).isoformat(),
        }

    except Exception as e:
        duration = time.time() - start_time
        err_msg = str(e)
        logger.error(f"ETL Pipeline Run [{run_id}] FAILED: {err_msg}", exc_info=True)
        _record_audit_complete(
            engine,
            run_id=run_id,
            status="FAILED",
            extracted=records_extracted,
            transformed=records_transformed,
            loaded=records_loaded,
            duration=duration,
            error=err_msg,
        )
        return {
            "run_id": run_id,
            "status": "FAILED",
            "trigger_type": trigger_type,
            "error": err_msg,
            "duration_seconds": round(duration, 2),
            "completed_at": datetime.now(timezone.utc).isoformat(),
        }


if __name__ == "__main__":
    # Allows manual testing via CLI: python etl.py
    print(run_etl(trigger_type="manual_cli"))
