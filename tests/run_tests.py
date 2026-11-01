"""
tests/run_tests.py
------------------
Standard library unittest test suite to verify ETL extraction, transformation,
validation, and incremental upsert loading.
"""

import os
import sys
import unittest
from datetime import datetime
from unittest.mock import MagicMock, patch

# Ensure project root is in sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from sqlalchemy import create_engine, select

import etl
from config import CityConfig, settings


class TestWeatherETL(unittest.TestCase):
    def setUp(self):
        self.sample_city = CityConfig(name="Tokyo", latitude=35.6762, longitude=139.6503, timezone="UTC")
        self.mock_payload = {
            "latitude": 35.6762,
            "longitude": 139.6503,
            "hourly": {
                "time": [
                    "2026-10-24T00:00",
                    "2026-10-24T01:00",
                    "2026-10-24T02:00",
                ],
                "temperature_2m": [15.42, None, 13.20],
                "relative_humidity_2m": [78, 85, None],
                "precipitation": [0.0, 1.5, 0.0],
            },
        }

    def test_parse_iso_timestamp(self):
        dt1 = etl._parse_iso_timestamp("2026-10-24T12:00")
        self.assertEqual(dt1, datetime(2026, 10, 24, 12, 0, 0))

        dt2 = etl._parse_iso_timestamp("2026-10-24T12:00:00Z")
        self.assertEqual(dt2, datetime(2026, 10, 24, 12, 0, 0))

        self.assertIsNone(etl._parse_iso_timestamp("invalid-date"))

    def test_transform_valid_and_nulls(self):
        records = etl.transform_city_payload(self.sample_city, self.mock_payload)
        self.assertEqual(len(records), 3)

        # Record 0
        self.assertEqual(records[0]["city_name"], "Tokyo")
        self.assertEqual(records[0]["temperature_2m"], 15.42)
        self.assertEqual(records[0]["relative_humidity_2m"], 78)
        self.assertEqual(records[0]["precipitation"], 0.0)

        # Record 1: Null temperature
        self.assertIsNone(records[1]["temperature_2m"])
        self.assertEqual(records[1]["relative_humidity_2m"], 85)

        # Record 2: Null humidity
        self.assertEqual(records[2]["temperature_2m"], 13.20)
        self.assertIsNone(records[2]["relative_humidity_2m"])

    def test_transform_mismatched_lengths(self):
        corrupt = {
            "hourly": {
                "time": ["2026-10-24T00:00"],
                "temperature_2m": [15.0, 16.0],
                "relative_humidity_2m": [70],
                "precipitation": [0.0],
            }
        }
        records = etl.transform_city_payload(self.sample_city, corrupt)
        self.assertEqual(len(records), 0)

    def test_extract_mocked_success(self):
        mock_session = MagicMock()
        mock_response = MagicMock()
        mock_response.json.return_value = self.mock_payload
        mock_response.raise_for_status.return_value = None
        mock_session.get.return_value = mock_response

        res = etl.extract_weather_for_city(self.sample_city, mock_session)
        self.assertIsNotNone(res)
        self.assertIn("hourly", res)

    def test_incremental_load_upsert(self):
        # In-memory SQLite for verifying load & upsert
        engine = create_engine("sqlite:///:memory:")
        etl.init_db(engine)

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

        count1 = etl.load(records_initial, engine)
        self.assertEqual(count1, 2)

        with engine.connect() as conn:
            rows = conn.execute(select(etl.weather_hourly_table)).all()
            self.assertEqual(len(rows), 2)

        # Upsert: Re-insert Tokyo for same timestamp with revised values, plus New York
        records_update = [
            {
                "city_name": "Tokyo",
                "latitude": 35.6762,
                "longitude": 139.6503,
                "recorded_at": datetime(2026, 10, 24, 12, 0, 0),
                "temperature_2m": 21.0,  # Revised
                "relative_humidity_2m": 65,  # Revised
                "precipitation": 2.5,  # Revised
            },
            {
                "city_name": "New York",
                "latitude": 40.7128,
                "longitude": -74.0060,
                "recorded_at": datetime(2026, 10, 24, 12, 0, 0),
                "temperature_2m": 9.5,
                "relative_humidity_2m": 55,
                "precipitation": 0.0,
            },
        ]

        count2 = etl.load(records_update, engine)
        self.assertEqual(count2, 2)

        with engine.connect() as conn:
            stmt = select(etl.weather_hourly_table).where(etl.weather_hourly_table.c.city_name == "Tokyo")
            tokyo_row = conn.execute(stmt).mappings().one()
            self.assertEqual(tokyo_row["temperature_2m"], 21.0)
            self.assertEqual(tokyo_row["relative_humidity_2m"], 65)
            self.assertEqual(tokyo_row["precipitation"], 2.5)

            # Total rows must be 3 (Tokyo updated, London preserved, New York added)
            all_rows = conn.execute(select(etl.weather_hourly_table)).all()
            self.assertEqual(len(all_rows), 3)

    @patch("etl.get_db_engine")
    @patch("etl.extract")
    def test_run_etl_end_to_end(self, mock_extract, mock_get_engine):
        # Create an in-memory test engine
        engine = create_engine("sqlite:///:memory:")
        mock_get_engine.return_value = engine
        mock_extract.return_value = [(self.sample_city, self.mock_payload)]

        result = etl.run_etl(trigger_type="test_suite")
        self.assertEqual(result["status"], "SUCCESS")
        self.assertEqual(result["records_extracted"], 3)
        self.assertEqual(result["records_transformed"], 3)
        self.assertEqual(result["records_loaded"], 3)

        # Verify audit run log
        with engine.connect() as conn:
            runs = conn.execute(select(etl.etl_pipeline_runs_table)).mappings().all()
            self.assertEqual(len(runs), 1)
            self.assertEqual(runs[0]["status"], "SUCCESS")
            self.assertEqual(runs[0]["records_loaded"], 3)


class TestWebApp(unittest.TestCase):
    def setUp(self):
        from fastapi.testclient import TestClient
        import main
        self.client = TestClient(main.app)

    def test_dashboard_html_served(self):
        response = self.client.get("/", headers={"Accept": "text/html"})
        self.assertEqual(response.status_code, 200)
        self.assertIn("Weather ETL Pipeline", response.text)
        self.assertIn("Trigger ETL Pipeline", response.text)

    def test_root_json_format(self):
        response = self.client.get("/?format=json")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(data.get("service"), "Open-Meteo Weather ETL Pipeline")
        self.assertEqual(data.get("status"), "online")

    @patch("main.run_etl")
    def test_web_trigger_etl_button(self, mock_run_etl):
        mock_run_etl.return_value = {
            "run_id": "test_web_run_123",
            "status": "SUCCESS",
            "records_loaded": 1200,
            "duration_seconds": 4.5,
        }
        response = self.client.post("/web-trigger-etl")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(data["status"], "SUCCESS")
        self.assertEqual(data["records_loaded"], 1200)
        mock_run_etl.assert_called_once_with(trigger_type="web_ui")


if __name__ == "__main__":
    unittest.main()
