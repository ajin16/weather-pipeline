"""
config.py
---------
Configuration management for the Open-Meteo Weather ETL Pipeline.
Loads settings from environment variables with strong typing, sensible defaults,
and cloud deployment (Render, AWS, GCP) compatibility.
Supports pydantic-settings with a graceful fallback to standard pydantic + python-dotenv.
"""

from __future__ import annotations

import json
import logging
import os
from typing import List, Optional
from urllib.parse import quote_plus

from pydantic import BaseModel, Field

# Pre-load local .env file if available
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass


class CityConfig(BaseModel):
    """Configuration model for a target city to extract weather metrics for."""
    name: str = Field(..., description="City name identifier")
    latitude: float = Field(..., ge=-90.0, le=90.0, description="Latitude in decimal degrees")
    longitude: float = Field(..., ge=-180.0, le=180.0, description="Longitude in decimal degrees")
    timezone: str = Field(default="UTC", description="Timezone for returned hourly data (defaults to UTC)")


# Default global cities for the pipeline (25 Major Global Cities)
DEFAULT_CITIES: List[CityConfig] = [
    CityConfig(name="Tokyo", latitude=35.6762, longitude=139.6503, timezone="UTC"),
    CityConfig(name="London", latitude=51.5074, longitude=-0.1278, timezone="UTC"),
    CityConfig(name="New York", latitude=40.7128, longitude=-74.0060, timezone="UTC"),
    CityConfig(name="Berlin", latitude=52.5200, longitude=13.4050, timezone="UTC"),
    CityConfig(name="Paris", latitude=48.8566, longitude=2.3522, timezone="UTC"),
    CityConfig(name="Sydney", latitude=-33.8688, longitude=151.2093, timezone="UTC"),
    CityConfig(name="Singapore", latitude=1.3521, longitude=103.8198, timezone="UTC"),
    CityConfig(name="Dubai", latitude=25.2048, longitude=55.2708, timezone="UTC"),
    CityConfig(name="Mumbai", latitude=19.0760, longitude=72.8777, timezone="UTC"),
    CityConfig(name="Sao Paulo", latitude=-23.5505, longitude=-46.6333, timezone="UTC"),
    CityConfig(name="Toronto", latitude=43.6532, longitude=-79.3832, timezone="UTC"),
    CityConfig(name="Cairo", latitude=30.0444, longitude=31.2357, timezone="UTC"),
    CityConfig(name="Seoul", latitude=37.5665, longitude=126.9780, timezone="UTC"),
    CityConfig(name="Mexico City", latitude=19.4326, longitude=-99.1332, timezone="UTC"),
    CityConfig(name="Johannesburg", latitude=-26.2041, longitude=28.0473, timezone="UTC"),
    CityConfig(name="Buenos Aires", latitude=-34.6037, longitude=-58.3816, timezone="UTC"),
    CityConfig(name="Istanbul", latitude=41.0082, longitude=28.9784, timezone="UTC"),
    CityConfig(name="Bangkok", latitude=13.7563, longitude=100.5018, timezone="UTC"),
    CityConfig(name="Los Angeles", latitude=34.0522, longitude=-118.2437, timezone="UTC"),
    CityConfig(name="Rome", latitude=41.9028, longitude=12.4964, timezone="UTC"),
    CityConfig(name="Madrid", latitude=40.4168, longitude=-3.7038, timezone="UTC"),
    CityConfig(name="Nairobi", latitude=-1.2921, longitude=36.8219, timezone="UTC"),
    CityConfig(name="Jakarta", latitude=-6.2088, longitude=106.8456, timezone="UTC"),
    CityConfig(name="Chicago", latitude=41.8781, longitude=-87.6298, timezone="UTC"),
    CityConfig(name="Auckland", latitude=-36.8485, longitude=174.7633, timezone="UTC"),
]


try:
    from pydantic_settings import BaseSettings, SettingsConfigDict

    class _BaseSettingsConfig(BaseSettings):
        model_config = SettingsConfigDict(
            env_file=".env",
            env_file_encoding="utf-8",
            extra="ignore",
            case_sensitive=False,
        )
except ImportError:
    class _BaseSettingsConfig(BaseModel):  # type: ignore[no-redef]
        def __init__(self, **data):
            # Fallback initialization using os.environ
            env_data = {}
            for field_name in self.model_fields.keys():
                val = os.getenv(field_name.upper()) or os.getenv(field_name.lower())
                if val is not None:
                    env_data[field_name] = val
            env_data.update(data)
            super().__init__(**env_data)


class Settings(_BaseSettingsConfig):
    """
    Application Settings loaded from environment variables or .env file.
    """
    # --------------------------------------------------------------------------
    # Environment & Logging
    # --------------------------------------------------------------------------
    ENVIRONMENT: str = Field(default="production", description="Runtime environment: development or production")
    LOG_LEVEL: str = Field(default="INFO", description="Logging verbosity: DEBUG, INFO, WARNING, ERROR")

    # --------------------------------------------------------------------------
    # Server Settings (FastAPI / Uvicorn)
    # --------------------------------------------------------------------------
    HOST: str = Field(default="0.0.0.0", description="Host to bind Uvicorn server")
    PORT: int = Field(default=8000, description="Port to bind Uvicorn server (Render sets PORT env)")
    API_SECRET_KEY: Optional[str] = Field(
        default=None,
        description="Optional API key to protect POST /trigger-etl via X-API-Key header",
    )

    # --------------------------------------------------------------------------
    # Database Settings (MySQL / Render)
    # --------------------------------------------------------------------------
    DATABASE_URL: Optional[str] = Field(
        default=None,
        description="Full MySQL connection URL (e.g., mysql+pymysql://user:pass@host:3306/db)",
    )
    DB_HOST: str = Field(default="localhost", description="MySQL Hostname")
    DB_PORT: int = Field(default=3306, description="MySQL Port")
    DB_USER: str = Field(default="root", description="MySQL Username")
    DB_PASSWORD: str = Field(default="", description="MySQL Password")
    DB_NAME: str = Field(default="Weather", description="MySQL Database name")
    DB_SSL_DISABLED: bool = Field(default=False, description="Whether SSL is disabled (False = SSL enabled)")
    DB_SSL_CA: Optional[str] = Field(default=None, description="Path to SSL CA certificate bundle")
    DB_SSL_MODE: Optional[str] = Field(default=None, description="SSL mode: REQUIRED, PREFERRED, DISABLED")

    # Connection Pool Settings
    DB_POOL_SIZE: int = Field(default=5, description="SQLAlchemy connection pool size")
    DB_MAX_OVERFLOW: int = Field(default=10, description="SQLAlchemy connection pool max overflow")
    DB_POOL_RECYCLE: int = Field(default=1800, description="Recycle connections after N seconds (prevents MySQL drop)")
    DB_POOL_PRE_PING: bool = Field(default=True, description="Test connection validity before checkout")

    # --------------------------------------------------------------------------
    # Open-Meteo API Settings
    # --------------------------------------------------------------------------
    OPEN_METEO_BASE_URL: str = Field(
        default="https://api.open-meteo.com/v1/forecast",
        description="Base REST URL for Open-Meteo Forecast API",
    )
    REQUEST_TIMEOUT_SECONDS: int = Field(default=15, description="HTTP request timeout in seconds")
    MAX_RETRIES: int = Field(default=3, description="Max retries for transient HTTP failures")
    RETRY_BACKOFF_FACTOR: float = Field(default=0.5, description="Exponential backoff factor for retries")

    # Metrics range: past_days=1 captures yesterday's verified readings; forecast_days=1 covers today
    PAST_DAYS: int = Field(default=1, ge=0, le=92, description="Number of past days to extract for backfill/updates")
    FORECAST_DAYS: int = Field(default=1, ge=1, le=16, description="Number of forecast days to extract")

    # --------------------------------------------------------------------------
    # Configurable Cities (JSON string or default list)
    # --------------------------------------------------------------------------
    CITIES_JSON: Optional[str] = Field(
        default=None,
        description="Optional JSON array string to override target cities via env var",
    )

    def get_cities(self) -> List[CityConfig]:
        """Parse custom cities JSON if provided, else return DEFAULT_CITIES."""
        if self.CITIES_JSON:
            try:
                parsed = json.loads(self.CITIES_JSON)
                if isinstance(parsed, list):
                    return [CityConfig(**city) for city in parsed]
            except Exception as e:
                logging.getLogger(__name__).warning(
                    f"Failed to parse CITIES_JSON environment variable: {e}. Falling back to DEFAULT_CITIES."
                )
        return DEFAULT_CITIES

    def get_sqlalchemy_database_url(self) -> str:
        """
        Builds and normalizes the SQLAlchemy connection string.
        Ensures the pymysql driver prefix is present (e.g. mysql+pymysql://).
        Resolves host to IP to prevent intermittent OS DNS [Errno 11001] getaddrinfo issues.
        """
        if self.DATABASE_URL:
            raw_url = self.DATABASE_URL.strip()
            # If scheme is 'mysql://', replace with 'mysql+pymysql://'
            if raw_url.startswith("mysql://"):
                return raw_url.replace("mysql://", "mysql+pymysql://", 1)
            return raw_url

        # Build from individual components with proper URL escaping
        safe_password = quote_plus(str(self.DB_PASSWORD))
        safe_user = quote_plus(str(self.DB_USER))

        # Resolve hostname to avoid intermittent socket getaddrinfo failures
        host = self.DB_HOST
        try:
            import socket
            resolved_ip = socket.gethostbyname(self.DB_HOST)
            if resolved_ip:
                host = resolved_ip
        except Exception:
            pass

        return f"mysql+pymysql://{safe_user}:{safe_password}@{host}:{self.DB_PORT}/{self.DB_NAME}"

    def get_connect_args(self) -> dict:
        """
        Builds dictionary of engine connect_args, handling SSL parameters for cloud MySQL (Aiven, Render, AWS).
        """
        connect_args: dict = {
            "connect_timeout": 15,
        }

        # If SSL is enabled (ssl_disabled is False)
        if not self.DB_SSL_DISABLED:
            ssl_dict: dict = {}
            if self.DB_SSL_CA and os.path.exists(self.DB_SSL_CA):
                ssl_dict["ca"] = self.DB_SSL_CA
            if self.DB_SSL_MODE:
                ssl_dict["ssl_mode"] = self.DB_SSL_MODE
            # PyMySQL accepts a dictionary (empty or with ca/ssl_mode) to negotiate TLS
            connect_args["ssl"] = ssl_dict

        return connect_args


# Global settings singleton
settings = Settings()
