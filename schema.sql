-- =============================================================================
-- Open-Meteo Weather ETL Pipeline - Database Schema (MySQL 8.0+)
-- =============================================================================

-- 1. Create database if not exists (for local or self-managed deployments)
CREATE DATABASE IF NOT EXISTS weather_db
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_unicode_ci;

USE weather_db;

-- -----------------------------------------------------------------------------
-- Table: weather_hourly
-- Stores hourly weather observation and forecast metrics per city.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS weather_hourly (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    city_name VARCHAR(64) NOT NULL COMMENT 'Standardized city name (e.g. Tokyo, London)',
    latitude DECIMAL(8, 5) NOT NULL COMMENT 'Latitude coordinate (-90.0 to 90.0)',
    longitude DECIMAL(8, 5) NOT NULL COMMENT 'Longitude coordinate (-180.0 to 180.0)',
    recorded_at DATETIME NOT NULL COMMENT 'Observation/forecast timestamp in UTC',
    temperature_2m FLOAT NULL COMMENT 'Air temperature at 2 meters above ground in Celsius',
    relative_humidity_2m SMALLINT NULL COMMENT 'Relative humidity at 2 meters (0 to 100 percentage)',
    precipitation FLOAT NULL COMMENT 'Total precipitation (rain, showers, snow) in millimeters',
    extracted_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Timestamp when the record was ingested',
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Timestamp of last record update',

    -- Composite unique constraint to support idempotent upsert (ON DUPLICATE KEY UPDATE)
    CONSTRAINT uq_city_recorded UNIQUE (city_name, recorded_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Secondary indexes for high-performance analytical and time-series queries
CREATE INDEX idx_weather_recorded_at ON weather_hourly (recorded_at);
CREATE INDEX idx_weather_city_name ON weather_hourly (city_name);
CREATE INDEX idx_weather_city_time ON weather_hourly (city_name, recorded_at DESC);

-- -----------------------------------------------------------------------------
-- Table: etl_pipeline_runs
-- Audit log table tracking pipeline execution status, counts, and errors.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS etl_pipeline_runs (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    run_id VARCHAR(64) NOT NULL UNIQUE COMMENT 'Unique UUID identifying this pipeline run',
    trigger_type VARCHAR(32) NOT NULL DEFAULT 'cron' COMMENT 'Source trigger: cron, manual, fastapi_sync, fastapi_bg',
    status VARCHAR(32) NOT NULL COMMENT 'Execution status: STARTED, SUCCESS, FAILED',
    cities_count INT NOT NULL DEFAULT 0 COMMENT 'Number of configured cities targeted',
    records_extracted INT NOT NULL DEFAULT 0 COMMENT 'Total raw hourly records fetched from Open-Meteo API',
    records_transformed INT NOT NULL DEFAULT 0 COMMENT 'Total clean records produced after validation',
    records_loaded INT NOT NULL DEFAULT 0 COMMENT 'Total records successfully inserted or updated in MySQL',
    error_message TEXT NULL COMMENT 'Exception traceback or failure details if status is FAILED',
    started_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Timestamp when the pipeline started',
    completed_at DATETIME NULL COMMENT 'Timestamp when the pipeline completed',
    duration_seconds FLOAT NULL COMMENT 'Total execution time in seconds'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE INDEX idx_etl_started_at ON etl_pipeline_runs (started_at DESC);
CREATE INDEX idx_etl_status ON etl_pipeline_runs (status);
