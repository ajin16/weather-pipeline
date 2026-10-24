"""
tests/test_etl.py
-----------------
Unit tests for the Open-Meteo Weather ETL Pipeline.
Tests data extraction mocks, timestamp parsing, transformations, null handling,
and incremental upsert loading.
"""

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import create_engine, select

from config import CityConfig
from etl import (
    _parse_iso_timestamp,
    extract,
    extract_weather_for_city,
    init_db,
    load,
    transform,
    transform_city_payload,
    weather_hourly_table,
)


@pytest.fixture
def sample_city() -> CityConfig:
    return CityConfig(name="Tokyo", latitude=35.6762, longitude=139.6503, timezone="UTC")


@pytest.fixture
def mock_open_meteo_payload():
    return {
        "latitude": 35.6762,
        "longitude": 139.6503,
        "generationtime_ms": 0.12,
        "utc_offset_seconds": 0,
        "timezone": "UTC",
        "hourly": {
            "time": [
                "2026-10-24T00:00",
                "2026-10-24T01:00",
                "2026-10-24T02:00",
                "2026-10-24T03:00",
            ],
            "temperature_2m": [15.42, 14.89, None, 13.20],
            "relative_humidity_2m": [78, 82, 85, None],
            "precipitation": [0.0, 0.25, 1.5, 0.0],
        },
    }


@pytest.fixture
def in_memory_sqlite_engine():
    """Provides an isolated in-memory SQLite database for testing load & upsert logic."""
    engine = create_engine("sqlite:///:memory:", echo=False)
    init_db(engine)
    return engine


class TestExtract:
    def test_extract_weather_for_city_success(self, sample_city, mock_open_meteo_payload):
        mock_session = MagicMock()
        mock_response = MagicMock()
        mock_response.json.return_value = mock_open_meteo_payload
        mock_response.raise_for_status.return_value = None
        mock_session.get.return_value = mock_response

        payload = extract_weather_for_city(sample_city, mock_session)
        assert payload is not None
        assert "hourly" in payload
        assert len(payload["hourly"]["time"]) == 4

    def test_extract_weather_for_city_network_failure(self, sample_city):
        import requests

        mock_session = MagicMock()
        mock_session.get.side_effect = requests.exceptions.ConnectionError("Connection timed out")

        payload = extract_weather_for_city(sample_city, mock_session)
        assert payload is None


class TestTransform:
    def test_parse_iso_timestamp(self):
        # Test standard ISO without seconds
        dt1 = _parse_iso_timestamp("2026-10-24T12:00")
        assert dt1 == datetime(2026, 10, 24, 12, 0, 0)

        # Test ISO with Z suffix
        dt2 = _parse_iso_timestamp("2026-10-24T12:00:00Z")
        assert dt2 == datetime(2026, 10, 24, 12, 0, 0)

        # Test invalid string returns None
        assert _parse_iso_timestamp("invalid-date") is None

    def test_transform_city_payload_valid_and_nulls(self, sample_city, mock_open_meteo_payload):
        records = transform_city_payload(sample_city, mock_open_meteo_payload)
        assert len(records) == 4

        # Record 0: All valid
        assert records[0]["city_name"] == "Tokyo"
        assert records[0]["latitude"] == 35.6762
        assert records[0]["longitude"] == 139.6503
        assert records[0]["recorded_at"] == datetime(2026, 10, 24, 0, 0, 0)
        assert records[0]["temperature_2m"] == 15.42
        assert records[0]["relative_humidity_2m"] == 78
        assert records[0]["precipitation"] == 0.0

        # Record 2: Null temperature
        assert records[2]["temperature_2m"] is None
        assert records[2]["relative_humidity_2m"] == 85
        assert records[2]["precipitation"] == 1.5

        # Record 3: Null humidity
        assert records[3]["temperature_2m"] == 13.20
        assert records[3]["relative_humidity_2m"] is None

    def test_transform_mismatched_array_lengths(self, sample_city):
        corrupted_payload = {
            "hourly": {
                "time": ["2026-10-24T00:00", "2026-10-24T01:00"],
                "temperature_2m": [15.0],  # Length 1 vs 2
                "relative_humidity_2m": [70, 75],
                "precipitation": [0.0, 0.0],
            }
        }
        records = transform_city_payload(sample_city, corrupted_payload)
        assert len(records) == 0  # Should cleanly reject mismatched payloads


class TestLoad:
    def test_incremental_load_and_upsert(self, in_memory_sqlite_engine):
        # 1. Initial insert of 2 records
        records_initial = [
            {
                "city_name": "Tokyo",
                "latitude": 35.6762,
                "longitude": 139.6503,
                "recorded_at": datetime(2026, 10, 24, 12, 0, 0),
                "temperature_2m": 18.5,
                "relative_humidity_2m": 60,
                "precipitation": 0.0,
            },
            {
                "city_name": "London",
                "latitude": 51.5074,
                "longitude": -0.1278,
                "recorded_at": datetime(2026, 10, 24, 12, 0, 0),
                "temperature_2m": 12.0,
                "relative_humidity_2m": 80,
                "precipitation": 0.5,
            },
        ]

        loaded_1 = load(records_initial, in_memory_sqlite_engine)
        assert loaded_1 == 2

        with in_memory_sqlite_engine.connect() as conn:
            rows = conn.execute(select(weather_hourly_table)).all()
            assert len(rows) == 2

        # 2. Incremental update: Re-insert Tokyo for the same timestamp with revised temperature and rain
        records_update = [
            {
                "city_name": "Tokyo",
                "latitude": 35.6762,
                "longitude": 139.6503,
                "recorded_at": datetime(2026, 10, 24, 12, 0, 0),
                "temperature_2m": 19.8,  # Updated from 18.5
                "relative_humidity_2m": 65,  # Updated from 60
                "precipitation": 1.2,  # Updated from 0.0
            },
            {
                "city_name": "New York",  # New record
                "latitude": 40.7128,
                "longitude": -74.0060,
                "recorded_at": datetime(2026, 10, 24, 12, 0, 0),
                "temperature_2m": 10.0,
                "relative_humidity_2m": 50,
                "precipitation": 0.0,
            },
        ]

        loaded_2 = load(records_update, in_memory_sqlite_engine)
        assert loaded_2 == 2

        # 3. Verify that total rows is now 3 (not 4), and Tokyo record was updated in place!
        with in_memory_sqlite_engine.connect() as conn:
            stmt = select(weather_hourly_table).where(weather_hourly_table.c.city_name == "Tokyo")
            tokyo_row = conn.execute(stmt).mappings().one()
            assert tokyo_row["temperature_2m"] == 19.8
            assert tokyo_row["relative_humidity_2m"] == 65
            assert tokyo_row["precipitation"] == 1.2

            total_rows = conn.execute(select(weather_hourly_table)).all()
            assert len(total_rows) == 3
