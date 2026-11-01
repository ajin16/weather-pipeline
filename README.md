# Production Open-Meteo Weather ETL Pipeline

[![Python Version](https://img.shields.io/badge/python-3.10%20%7C%203.11%20%7C%203.12-blue.svg)](https://www.python.org/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.110+-009688.svg?logo=fastapi&logoColor=white)](https://fastapi.tiangolo.com/)
[![SQLAlchemy](https://img.shields.io/badge/SQLAlchemy-2.0+-red.svg)](https://www.sqlalchemy.org/)
[![MySQL](https://img.shields.io/badge/MySQL-8.0+-orange.svg?logo=mysql&logoColor=white)](https://www.mysql.com/)
[![Render](https://img.shields.io/badge/Deploy-Render-46E3B7.svg?logo=render&logoColor=white)](https://render.com/)

> [!NOTE]
> For an exhaustive, in-depth technical manual explaining the internal mechanics, architecture, data dictionary, failure recovery protocols, and code flow, see **[DOCUMENTATION.md](file:///c:/Users/USER/OneDrive/Documents/Antigravity%20Projects/Pipeline%201/DOCUMENTATION.md)**.

A production-grade, enterprise-ready Python ETL (Extract, Transform, Load) pipeline that extracts hourly meteorological measurements from the **Open-Meteo REST API** across **25 global cities**, performs validation, sanitization, and timestamp normalization, and incrementally upserts records into a **MySQL** database hosted on **Aiven / Render** (or any cloud database).

The pipeline is wrapped in a high-performance **FastAPI** web service featuring API-key authentication, synchronous/asynchronous triggers, live connection pooling, health checks, and run auditing.

---

## 1. System Architecture

```mermaid
flowchart TD
    subgraph Trigger ["Automation & Triggers"]
        CronJob["External Cron (cron-job.org / Render Cron)"]
        ManualCLI["CLI Runner (etl.py)"]
        CurlTrigger["Admin Webhook / API Client"]
    end

    subgraph API_Layer ["FastAPI Service (Render Web Service)"]
        AuthMiddleware["API Key Auth (X-API-Key)"]
        TriggerEndpoint["POST /trigger-etl"]
        HealthEndpoint["GET /health"]
        AuditEndpoint["GET /etl/runs"]
    end

    subgraph ETL_Core ["ETL Core Engine (etl.py)"]
        Extract["1. Extract (Open-Meteo REST API)<br/>• Session Pooling & Exponential Retries<br/>• Tokyo, London, New York, etc."]
        Transform["2. Transform & Validate<br/>• ISO 8601 to Naive UTC Datetime<br/>• Range validation & null handling"]
        Load["3. Incremental Load (MySQL)<br/>• INSERT ... ON DUPLICATE KEY UPDATE<br/>• Chunked Batch Execution"]
    end

    subgraph Storage ["Cloud MySQL Database (Render / Aiven / RDS)"]
        TableWeather[("weather_hourly<br/>Unique: city_name + recorded_at")]
        TableAudit[("etl_pipeline_runs<br/>Run metadata & latency logs")]
    end

    CronJob -->|HTTP POST| AuthMiddleware
    CurlTrigger -->|HTTP POST| AuthMiddleware
    AuthMiddleware --> TriggerEndpoint
    TriggerEndpoint -->|Execute| ETL_Core
    ManualCLI --> ETL_Core

    Extract --> Transform --> Load
    Load -->|Idempotent Upsert| TableWeather
    ETL_Core -.->|Audit Run Status| TableAudit
    HealthEndpoint -.->|Ping Check| Storage
    AuditEndpoint -.->|Query History| TableAudit
```

---

## 2. Project Directory Structure

```text
.
├── DOCUMENTATION.md        # Complete in-and-out technical & architecture manual
├── web_ui.py               # Interactive Python Web Application Dashboard UI
├── schema.sql              # MySQL DDL: tables, indexes, constraints & audit logs
├── config.py               # Pydantic Settings & environment variable configuration
├── etl.py                  # Core modular ETL pipeline (Extract, Transform, Load)
├── main.py                 # FastAPI application hosting web dashboard & API routes
├── requirements.txt        # Pinned production Python dependencies
├── .env.example            # Environment variables configuration template
├── Dockerfile              # Non-root, optimized container definition for cloud deployment
├── .dockerignore           # Excluded files for lightweight Docker builds
├── render.yaml             # Render Blueprint specification for 1-click deployment
└── tests/
    ├── __init__.py
    ├── run_tests.py        # Automated test suite (ETL & WebApp tests)
    └── test_etl.py         # Pytest unit tests for mock extraction, parsing & upserts
```

---

## 3. Database Schema Design (`schema.sql`)

The database architecture is designed specifically for **incremental idempotency** and **high-throughput analytics**:

### Key Features:
- **`weather_hourly`**:
  - `id`: Auto-incrementing surrogate primary key.
  - `city_name`: Standardized city identifier.
  - `latitude`, `longitude`: Precise coordinates (5 decimal places).
  - `recorded_at`: Standardized UTC observation/forecast timestamp.
  - `temperature_2m`: Air temperature in Celsius (`FLOAT NULL`).
  - `relative_humidity_2m`: Relative humidity percentage (`SMALLINT NULL`, 0-100).
  - `precipitation`: Precipitation in millimeters (`FLOAT NULL`).
  - `extracted_at`: Default ingestion timestamp.
  - `updated_at`: Automatically updated via MySQL `ON UPDATE CURRENT_TIMESTAMP`.
  - **`uq_city_recorded`**: `UNIQUE KEY (city_name, recorded_at)`. Ensures that re-extracting overlapping time windows updates existing data rather than inserting duplicates.
  - **Composite Index**: `INDEX idx_weather_city_time (city_name, recorded_at DESC)` for fast time-series retrieval.

- **`etl_pipeline_runs`**:
  - Audit logging table recording execution status (`STARTED`, `SUCCESS`, `FAILED`), row counts (`extracted`, `transformed`, `loaded`), duration in seconds, trigger source, and error stack traces.

---

## 4. Local Development Setup

### Prerequisites
- Python 3.10, 3.11, or 3.12
- MySQL Server 8.0+ (or a local Docker MySQL container)
- Git

### Step 1: Clone and Create Virtual Environment

```bash
# Clone the repository
git clone <your-repo-url>
cd "Pipeline 1"

# Create a virtual environment
python -m venv venv

# Activate virtual environment
# On Linux/macOS:
source venv/bin/activate
# On Windows (PowerShell):
.\venv\Scripts\Activate.ps1
```

### Step 2: Install Dependencies

```bash
pip install --upgrade pip
pip install -r requirements.txt
```

### Step 3: Configure Environment Variables

Copy `.env.example` to `.env`:

```bash
cp .env.example .env
```

Edit `.env` to match your local or remote MySQL credentials:

```ini
ENVIRONMENT=development
LOG_LEVEL=INFO
PORT=8000
API_SECRET_KEY=local-dev-secret-key

# Option 1: Full Database URL
DATABASE_URL=mysql+pymysql://root:password@localhost:3306/weather_db

# Option 2: Individual variables (if DATABASE_URL is omitted)
DB_HOST=localhost
DB_PORT=3306
DB_USER=root
DB_PASSWORD=password
DB_NAME=weather_db
```

### Step 4: Initialize the MySQL Database

Using the MySQL CLI:

```bash
mysql -u root -p < schema.sql
```

*(Note: If you run `python etl.py` or start FastAPI, the pipeline will also auto-detect and create missing tables via SQLAlchemy metadata if granted `CREATE TABLE` permissions).*

### Step 5: Run the Automated Unit Test Suite

Run the zero-dependency test suite (uses in-memory SQLite and mock APIs):

```bash
python tests/run_tests.py
```

Expected output:
```text
Ran 6 tests in 0.113s
OK
```

### Step 6: Test ETL Extraction Locally via CLI

You can execute a manual ETL run directly from the command line:

```bash
python etl.py
```

---

## 5. Running the Python Web Application & API Service

Start the local development server:

```bash
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

### 5.1 Interactive Web Dashboard (Trigger on Button Click)

Open your web browser and navigate to:

👉 **[http://localhost:8000/](http://localhost:8000/)**

The application provides a modern web interface:
- **🚀 "Trigger ETL Pipeline" Button**: Click to run the pipeline on-demand. Features a live spinner, real-time status updates, and instant receipt showing records extracted, transformed, and loaded.
- **⚡ Execution Mode Toggle**: Option to run synchronously (immediate results) or in the background.
- **📊 Real-Time Metrics Grid**: Live cards showing total rows in `weather_hourly`, active target cities (25), total pipeline runs, and latest execution duration.
- **📍 Live Weather Explorer**: Searchable and filterable table displaying the latest meteorological readings across all 25 cities.
- **📜 Pipeline Audit History**: Instant view of recent execution runs from `etl_pipeline_runs`.

### 5.2 API & Automated Triggers

All existing programmatic endpoints remain identical:
- **Interactive API Documentation (Swagger)**: [http://localhost:8000/docs](http://localhost:8000/docs)
- **Alternative ReDoc UI**: [http://localhost:8000/redoc](http://localhost:8000/redoc)
- **Health Check**: [http://localhost:8000/health](http://localhost:8000/health)

#### Triggering via cURL / External Cron (Unchanged):

**Synchronous execution:**
```bash
curl -X POST "http://localhost:8000/trigger-etl" \
  -H "X-API-Key: local-dev-secret-key"
```

**Asynchronous background execution:**
```bash
curl -X POST "http://localhost:8000/trigger-etl?background=true" \
  -H "X-API-Key: local-dev-secret-key"
```

---

## 6. Deploying to Render

Render is an ideal cloud platform to host this pipeline. You can deploy it as a **Web Service** or use a **Render Cron Job**.

### Option A: Deploying via GitHub & Render Dashboard

1. **Push your code to GitHub / GitLab**.
2. **Log into [Render](https://render.com/)**.
3. **Set up MySQL Database**:
   - Render provides native PostgreSQL; for MySQL, you can connect an external cloud MySQL provider (such as **Aiven MySQL**, **PlanetScale**, **AWS RDS**, or a Dockerized MySQL instance on Render).
   - Obtain your MySQL connection string, e.g.:
     `mysql+pymysql://admin:StrongPassword123@mysql-aiven.example.com:3306/weather_db?ssl_mode=REQUIRED`
4. **Create a New Web Service on Render**:
   - Click **New +** -> **Web Service**.
   - Connect your GitHub repository.
   - Configure the following settings:
     - **Name**: `weather-etl-service`
     - **Region**: Choose the region closest to your database (e.g., Oregon or Frankfurt).
     - **Runtime**: `Python 3` (or `Docker` using the included `Dockerfile`).
     - **Build Command**: `pip install -r requirements.txt`
     - **Start Command**: `uvicorn main:app --host 0.0.0.0 --port $PORT`
     - **Health Check Path**: `/health`
5. **Add Environment Variables**:
   In the Render service settings under the **Environment** tab, add:
   - `DATABASE_URL`: `mysql+pymysql://<user>:<password>@<host>:<port>/<dbname>`
   - `DB_SSL_MODE`: `REQUIRED`
   - `API_SECRET_KEY`: `<Generate a 32+ character random secret>`
   - `ENVIRONMENT`: `production`
   - `LOG_LEVEL`: `INFO`
6. Click **Create Web Service**. Render will build and deploy the container.
7. Once deployed, test your public URL:
   ```bash
   curl -f https://weather-etl-service.onrender.com/health
   ```

---

## 7. Setting Up the Automated Hourly Trigger

To ensure incremental data collection without manual intervention, schedule an hourly call to `POST /trigger-etl`.

### Method 1: External Cron Service (Recommended: [cron-job.org](https://cron-job.org/))

[cron-job.org](https://cron-job.org/) is a free, reliable web cron service:

1. Create a free account at [cron-job.org](https://cron-job.org/).
2. Click **Create Cronjob**.
3. Fill in the parameters:
   - **Title**: `Weather ETL Hourly Ingestion`
   - **URL**: `https://weather-etl-service.onrender.com/trigger-etl`
   - **Execution Schedule**: `User-defined` -> Select **Every 1 hour** (or cron expression `0 * * * *`).
   - **Request Method**: `POST`
   - **Headers**:
     - Key: `X-API-Key`
     - Value: `<YOUR_API_SECRET_KEY>`
     - Key: `Content-Type`
     - Value: `application/json`
   - **Request Timeout**: `30 seconds`
4. Click **Create Job**.
5. The service will now reliably ping your Render endpoint every hour on the hour.

> [!TIP]
> On Render's Free tier, services spin down after 15 minutes of inactivity. An hourly cron ping automatically keeps the service active and ensures fresh hourly forecasts!

---

### Method 2: Render Cron Job

If you prefer keeping everything inside Render:
1. In the Render Dashboard, click **New +** -> **Cron Job**.
2. Connect your repository.
3. Configure:
   - **Schedule**: `0 * * * *` (Every hour)
   - **Command**: `python -c "from etl import run_etl; run_etl('render_cron')"`
   - Add the same `DATABASE_URL` environment variable.

---

### Method 3: GitHub Actions Scheduled Workflow

You can also run the trigger via GitHub Actions. Add `.github/workflows/hourly_etl.yml`:

```yaml
name: Hourly Weather ETL Trigger

on:
  schedule:
    # Runs at the start of every hour (UTC)
    - cron: '0 * * * *'
  workflow_dispatch: # Allows manual trigger from GitHub UI

jobs:
  trigger-etl:
    runs-on: ubuntu-latest
    steps:
      - name: Trigger Render ETL Endpoint
        run: |
          curl -s -S -X POST "${{ secrets.RENDER_SERVICE_URL }}/trigger-etl" \
            -H "X-API-Key: ${{ secrets.ETL_API_KEY }}" \
            -H "Content-Type: application/json" \
            --fail
```

---

## 8. API Reference

| Method | Endpoint | Description | Auth Required |
|---|---|---|---|
| `GET` | `/` | API status and endpoint directory | No |
| `GET` | `/health` | Live database connectivity & ping test | No |
| `POST` | `/trigger-etl` | Runs the ETL pipeline (query param: `background=true\|false`) | `X-API-Key` |
| `GET` | `/etl/runs` | List the last *N* pipeline run audit logs | No |
| `GET` | `/etl/latest` | Get the latest recorded weather metrics for each city | No |

---

## 9. Useful Analytical SQL Queries

Once your pipeline begins streaming hourly records, you can execute analytics directly in MySQL:

### 1. View Current Conditions Across All Cities
```sql
SELECT 
    city_name,
    recorded_at,
    temperature_2m AS temp_celsius,
    relative_humidity_2m AS humidity_pct,
    precipitation AS precip_mm,
    updated_at
FROM weather_hourly
WHERE (city_name, recorded_at) IN (
    SELECT city_name, MAX(recorded_at)
    FROM weather_hourly
    GROUP BY city_name
)
ORDER BY city_name;
```

### 2. Daily Average, Min, and Max Temperature per City
```sql
SELECT 
    city_name,
    DATE(recorded_at) AS observation_date,
    ROUND(AVG(temperature_2m), 2) AS avg_temperature,
    ROUND(MIN(temperature_2m), 2) AS min_temperature,
    ROUND(MAX(temperature_2m), 2) AS max_temperature,
    ROUND(SUM(precipitation), 2) AS total_daily_precip_mm
FROM weather_hourly
GROUP BY city_name, DATE(recorded_at)
ORDER BY observation_date DESC, city_name;
```

### 3. Pipeline Health & Execution Metrics
```sql
SELECT 
    run_id,
    trigger_type,
    status,
    records_extracted,
    records_loaded,
    duration_seconds,
    started_at,
    error_message
FROM etl_pipeline_runs
ORDER BY started_at DESC
LIMIT 10;
```

---

## 10. Senior Engineering Best Practices Implemented

1. **Resilience & Fault Tolerance**: Open-Meteo requests are protected with exponential backoff retries (`429`, `500`, `502`, `503`, `504`).
2. **Idempotent Incremental Upserts**: Utilizes MySQL's native `INSERT ... ON DUPLICATE KEY UPDATE` to seamlessly update overlapping historical or forecast windows without producing duplicate rows.
3. **Cloud Connection Pooling**: SQLAlchemy engine is configured with `pool_pre_ping=True` and `pool_recycle=1800` to prevent dropped TCP connections common on cloud hosting providers like Render.
4. **Data Cleansing & Validation**: String timestamps are normalized to naive UTC datetimes, numeric values are clamped to physical sanity thresholds, and payloads with corrupted array lengths are safely isolated.
5. **Observability & Auditing**: Every pipeline invocation records operational metadata in `etl_pipeline_runs`.
6. **Zero-Trust Security**: Secured trigger endpoint with configurable `X-API-Key` headers.
7. **Containerization**: Includes an optimized multi-stage `Dockerfile` running as a non-privileged user (`appuser`).
