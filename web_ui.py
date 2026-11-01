"""
web_ui.py
---------
Interactive, modern web dashboard for the Open-Meteo Weather ETL Pipeline.
Provides a clean graphical user interface with an on-demand trigger button,
real-time system status, summary statistics, recent audit logs, and live weather metrics.
"""

def get_dashboard_html() -> str:
    return """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Weather ETL Pipeline Dashboard</title>
  <link rel="icon" href="data:image/svg+xml,<svg xmlns=%22http://www.w3.org/2000/svg%22 viewBox=%220 0 100 100%22><text y=%22.9em%22 font-size=%2290%22>🌦️</text></svg>">
  <style>
    :root {
      --bg: #0f172a;
      --card-bg: #1e293b;
      --card-border: #334155;
      --text-main: #f8fafc;
      --text-muted: #94a3b8;
      --primary: #38bdf8;
      --primary-hover: #0284c7;
      --success: #10b981;
      --success-bg: rgba(16, 185, 129, 0.15);
      --warning: #f59e0b;
      --danger: #ef4444;
      --danger-bg: rgba(239, 68, 68, 0.15);
      --accent: #6366f1;
    }
    * {
      box-sizing: border-box;
      margin: 0;
      padding: 0;
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
    }
    body {
      background-color: var(--bg);
      color: var(--text-main);
      padding: 24px;
      min-height: 100vh;
      display: flex;
      flex-direction: column;
      gap: 24px;
    }
    .container {
      max-width: 1200px;
      margin: 0 auto;
      width: 100%;
      display: flex;
      flex-direction: column;
      gap: 24px;
    }
    /* Header */
    header {
      display: flex;
      flex-wrap: wrap;
      justify-content: space-between;
      align-items: center;
      gap: 16px;
      padding-bottom: 20px;
      border-bottom: 1px solid var(--card-border);
    }
    .header-title {
      display: flex;
      align-items: center;
      gap: 12px;
    }
    .header-title h1 {
      font-size: 24px;
      font-weight: 700;
      background: linear-gradient(to right, #38bdf8, #818cf8);
      -webkit-background-clip: text;
      -webkit-text-fill-color: transparent;
    }
    .badges {
      display: flex;
      gap: 10px;
      flex-wrap: wrap;
    }
    .badge {
      display: inline-flex;
      align-items: center;
      gap: 6px;
      padding: 6px 12px;
      border-radius: 9999px;
      font-size: 12px;
      font-weight: 600;
      border: 1px solid var(--card-border);
      background: var(--card-bg);
      color: var(--text-muted);
    }
    .badge-success {
      border-color: rgba(16, 185, 129, 0.4);
      background: var(--success-bg);
      color: var(--success);
    }
    .badge-dot {
      width: 8px;
      height: 8px;
      border-radius: 50%;
      background: currentColor;
    }

    /* Action Trigger Hero Card */
    .hero-card {
      background: linear-gradient(145deg, #1e293b, #0f172a);
      border: 1px solid #3b82f6;
      border-radius: 16px;
      padding: 28px;
      box-shadow: 0 10px 25px -5px rgba(59, 130, 246, 0.1), 0 8px 10px -6px rgba(59, 130, 246, 0.1);
      display: flex;
      flex-direction: column;
      gap: 20px;
    }
    .hero-header {
      display: flex;
      justify-content: space-between;
      align-items: flex-start;
      flex-wrap: wrap;
      gap: 16px;
    }
    .hero-info h2 {
      font-size: 20px;
      font-weight: 600;
      margin-bottom: 6px;
    }
    .hero-info p {
      font-size: 14px;
      color: var(--text-muted);
      max-width: 650px;
      line-height: 1.5;
    }
    .controls-wrapper {
      display: flex;
      flex-wrap: wrap;
      align-items: center;
      gap: 16px;
    }
    .btn-trigger {
      background: linear-gradient(135deg, #0284c7, #2563eb);
      color: #ffffff;
      border: none;
      padding: 14px 28px;
      border-radius: 10px;
      font-size: 16px;
      font-weight: 600;
      cursor: pointer;
      display: inline-flex;
      align-items: center;
      gap: 10px;
      transition: all 0.2s ease;
      box-shadow: 0 4px 14px 0 rgba(37, 99, 235, 0.39);
    }
    .btn-trigger:hover:not(:disabled) {
      transform: translateY(-2px);
      box-shadow: 0 6px 20px rgba(37, 99, 235, 0.5);
      background: linear-gradient(135deg, #0369a1, #1d4ed8);
    }
    .btn-trigger:disabled {
      opacity: 0.6;
      cursor: not-allowed;
      transform: none;
      box-shadow: none;
    }
    .checkbox-label {
      display: inline-flex;
      align-items: center;
      gap: 8px;
      font-size: 13px;
      color: var(--text-muted);
      cursor: pointer;
      user-select: none;
    }
    .checkbox-label input {
      accent-color: var(--primary);
      width: 16px;
      height: 16px;
      cursor: pointer;
    }

    /* Live Progress / Result Box */
    .status-box {
      display: none;
      border-radius: 12px;
      padding: 16px 20px;
      font-size: 14px;
      animation: fadeIn 0.3s ease;
    }
    .status-box.loading {
      display: flex;
      align-items: center;
      gap: 12px;
      background: rgba(56, 189, 248, 0.1);
      border: 1px solid rgba(56, 189, 248, 0.3);
      color: #38bdf8;
    }
    .status-box.success {
      display: block;
      background: var(--success-bg);
      border: 1px solid rgba(16, 185, 129, 0.4);
      color: #a7f3d0;
    }
    .status-box.error {
      display: block;
      background: var(--danger-bg);
      border: 1px solid rgba(239, 68, 68, 0.4);
      color: #fca5a5;
    }
    .spinner {
      width: 20px;
      height: 20px;
      border: 3px solid rgba(56, 189, 248, 0.3);
      border-radius: 50%;
      border-top-color: #38bdf8;
      animation: spin 0.8s linear infinite;
    }
    @keyframes spin {
      to { transform: rotate(360deg); }
    }
    @keyframes fadeIn {
      from { opacity: 0; transform: translateY(-6px); }
      to { opacity: 1; transform: translateY(0); }
    }

    /* Metrics Grid */
    .stats-grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
      gap: 16px;
    }
    .stat-card {
      background: var(--card-bg);
      border: 1px solid var(--card-border);
      border-radius: 12px;
      padding: 20px;
      display: flex;
      flex-direction: column;
      gap: 8px;
      transition: border-color 0.2s ease;
    }
    .stat-card:hover {
      border-color: #475569;
    }
    .stat-title {
      font-size: 13px;
      color: var(--text-muted);
      text-transform: uppercase;
      letter-spacing: 0.5px;
      font-weight: 600;
    }
    .stat-value {
      font-size: 28px;
      font-weight: 700;
      color: var(--text-main);
    }
    .stat-footer {
      font-size: 12px;
      color: var(--text-muted);
    }

    /* Section & Tabs */
    .section-card {
      background: var(--card-bg);
      border: 1px solid var(--card-border);
      border-radius: 14px;
      overflow: hidden;
      display: flex;
      flex-direction: column;
    }
    .tabs-header {
      display: flex;
      justify-content: space-between;
      align-items: center;
      flex-wrap: wrap;
      gap: 12px;
      padding: 16px 20px;
      border-bottom: 1px solid var(--card-border);
      background: rgba(15, 23, 42, 0.4);
    }
    .tab-buttons {
      display: flex;
      gap: 8px;
    }
    .tab-btn {
      background: transparent;
      border: none;
      color: var(--text-muted);
      font-size: 14px;
      font-weight: 600;
      padding: 8px 16px;
      border-radius: 8px;
      cursor: pointer;
      transition: all 0.2s ease;
    }
    .tab-btn.active {
      background: rgba(56, 189, 248, 0.15);
      color: var(--primary);
    }
    .tab-btn:hover:not(.active) {
      color: var(--text-main);
      background: rgba(255, 255, 255, 0.05);
    }
    .search-input {
      background: var(--bg);
      border: 1px solid var(--card-border);
      color: var(--text-main);
      padding: 8px 14px;
      border-radius: 8px;
      font-size: 13px;
      outline: none;
      width: 220px;
      transition: border-color 0.2s;
    }
    .search-input:focus {
      border-color: var(--primary);
    }

    /* Tables */
    .table-container {
      overflow-x: auto;
      max-height: 480px;
    }
    table {
      width: 100%;
      border-collapse: collapse;
      text-align: left;
      font-size: 13px;
    }
    th {
      background: #0f172a;
      color: var(--text-muted);
      font-weight: 600;
      padding: 12px 16px;
      position: sticky;
      top: 0;
      z-index: 10;
      border-bottom: 1px solid var(--card-border);
      text-transform: uppercase;
      font-size: 11px;
      letter-spacing: 0.5px;
    }
    td {
      padding: 12px 16px;
      border-bottom: 1px solid rgba(51, 65, 85, 0.5);
      color: var(--text-main);
    }
    tr:hover td {
      background: rgba(255, 255, 255, 0.02);
    }
    .temp-badge {
      display: inline-block;
      padding: 2px 8px;
      border-radius: 6px;
      font-weight: 600;
      font-size: 12px;
    }
    .temp-warm {
      background: rgba(245, 158, 11, 0.15);
      color: #fbbf24;
    }
    .temp-cool {
      background: rgba(56, 189, 248, 0.15);
      color: #38bdf8;
    }
    .temp-cold {
      background: rgba(99, 102, 241, 0.15);
      color: #818cf8;
    }

    /* Footer */
    footer {
      display: flex;
      justify-content: space-between;
      align-items: center;
      flex-wrap: wrap;
      gap: 12px;
      font-size: 13px;
      color: var(--text-muted);
      padding-top: 16px;
      border-top: 1px solid var(--card-border);
      margin-top: auto;
    }
    footer a {
      color: var(--primary);
      text-decoration: none;
    }
    footer a:hover {
      text-decoration: underline;
    }

    /* Receipts grid */
    .receipt-grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(130px, 1fr));
      gap: 12px;
      margin-top: 10px;
    }
    .receipt-item {
      background: rgba(0, 0, 0, 0.2);
      padding: 8px 12px;
      border-radius: 8px;
    }
    .receipt-label {
      font-size: 11px;
      color: var(--text-muted);
      text-transform: uppercase;
    }
    .receipt-val {
      font-size: 16px;
      font-weight: 700;
      color: #ffffff;
      margin-top: 2px;
    }
  </style>
</head>
<body>
  <div class="container">
    <!-- Header -->
    <header>
      <div class="header-title">
        <span style="font-size: 32px;">🌦️</span>
        <div>
          <h1>Open-Meteo Weather ETL Pipeline</h1>
          <p style="font-size: 13px; color: var(--text-muted);">Production Data Ingestion & MySQL Cloud Loader</p>
        </div>
      </div>
      <div class="badges">
        <span id="dbBadge" class="badge badge-success">
          <span class="badge-dot"></span> <span id="dbBadgeText">MySQL Connected</span>
        </span>
        <span class="badge">
          <span>🌍</span> <span id="citiesBadgeText">25 Target Cities</span>
        </span>
        <a href="/docs" target="_blank" class="badge" style="text-decoration: none;">
          <span>📖</span> Swagger API Docs
        </a>
      </div>
    </header>

    <!-- Trigger Action Card -->
    <div class="hero-card">
      <div class="hero-header">
        <div class="hero-info">
          <h2>Run Pipeline On Demand</h2>
          <p>
            Fetches 48 hours of weather readings (24h past actuals + 24h forecasts) for 25 major world cities 
            from Open-Meteo REST API, validates physical boundaries, and idempotently upserts into your MySQL database.
          </p>
        </div>
        <div class="controls-wrapper">
          <label class="checkbox-label" title="Return immediately and execute asynchronously in background">
            <input type="checkbox" id="bgCheckbox">
            <span>Run in Background</span>
          </label>
          <button id="triggerBtn" class="btn-trigger" onclick="runPipeline()">
            <span>🚀</span>
            <span id="triggerBtnText">Trigger ETL Pipeline</span>
          </button>
        </div>
      </div>

      <!-- Live Notification / Receipt Box -->
      <div id="statusBox" class="status-box"></div>
    </div>

    <!-- Summary Metrics Grid -->
    <div class="stats-grid">
      <div class="stat-card">
        <div class="stat-title">Total Records in DB</div>
        <div id="statTotalRows" class="stat-value">--</div>
        <div class="stat-footer">Table: <code style="color: var(--primary);">weather_hourly</code></div>
      </div>
      <div class="stat-card">
        <div class="stat-title">Target Cities</div>
        <div id="statCitiesCount" class="stat-value">25</div>
        <div class="stat-footer">Global coverage across all continents</div>
      </div>
      <div class="stat-card">
        <div class="stat-title">Total Pipeline Runs</div>
        <div id="statTotalRuns" class="stat-value">--</div>
        <div class="stat-footer">Tracked in <code style="color: var(--primary);">etl_pipeline_runs</code></div>
      </div>
      <div class="stat-card">
        <div class="stat-title">Last Execution Duration</div>
        <div id="statLastDuration" class="stat-value">--</div>
        <div id="statLastRunId" class="stat-footer">Status: Idle</div>
      </div>
    </div>

    <!-- Data Tables Card with Tabs -->
    <div class="section-card">
      <div class="tabs-header">
        <div class="tab-buttons">
          <button id="tabWeatherBtn" class="tab-btn active" onclick="switchTab('weather')">
            📍 Latest City Observations
          </button>
          <button id="tabRunsBtn" class="tab-btn" onclick="switchTab('runs')">
            📜 Pipeline Audit Runs
          </button>
        </div>
        <div style="display: flex; gap: 8px; align-items: center;">
          <input type="text" id="citySearch" class="search-input" placeholder="Search city (e.g. Tokyo)..." oninput="filterWeatherTable()">
          <button onclick="refreshAllData()" class="badge" style="cursor: pointer; background: var(--bg);" title="Refresh Tables">
            🔄 Refresh
          </button>
        </div>
      </div>

      <!-- Tab 1: Weather Observations Table -->
      <div id="weatherTabContent" class="table-container">
        <table>
          <thead>
            <tr>
              <th>City</th>
              <th>Coordinates</th>
              <th>Recorded Time (UTC)</th>
              <th>Temperature</th>
              <th>Humidity</th>
              <th>Precipitation</th>
              <th>Last Updated (UTC)</th>
            </tr>
          </thead>
          <tbody id="weatherTableBody">
            <tr><td colspan="7" style="text-align: center; color: var(--text-muted); padding: 30px;">Loading observations...</td></tr>
          </tbody>
        </table>
      </div>

      <!-- Tab 2: Audit Runs Table -->
      <div id="runsTabContent" class="table-container" style="display: none;">
        <table>
          <thead>
            <tr>
              <th>Run ID</th>
              <th>Trigger</th>
              <th>Status</th>
              <th>Cities</th>
              <th>Loaded</th>
              <th>Duration</th>
              <th>Started (UTC)</th>
            </tr>
          </thead>
          <tbody id="runsTableBody">
            <tr><td colspan="7" style="text-align: center; color: var(--text-muted); padding: 30px;">Loading audit runs...</td></tr>
          </tbody>
        </table>
      </div>
    </div>

    <!-- Footer -->
    <footer>
      <div>
        Running on <strong>FastAPI</strong> + <strong>MySQL 8.4 (Aiven Cloud)</strong>
      </div>
      <div>
        API Endpoints: 
        <a href="/trigger-etl" target="_blank">POST /trigger-etl</a> | 
        <a href="/health" target="_blank">GET /health</a> | 
        <a href="/etl/latest" target="_blank">GET /etl/latest</a>
      </div>
    </footer>
  </div>

  <script>
    let weatherData = [];

    // Switch between Weather and Runs tabs
    function switchTab(tab) {
      const weatherTab = document.getElementById('weatherTabContent');
      const runsTab = document.getElementById('runsTabContent');
      const weatherBtn = document.getElementById('tabWeatherBtn');
      const runsBtn = document.getElementById('tabRunsBtn');
      const searchInput = document.getElementById('citySearch');

      if (tab === 'weather') {
        weatherTab.style.display = 'block';
        runsTab.style.display = 'none';
        weatherBtn.classList.add('active');
        runsBtn.classList.remove('active');
        searchInput.style.display = 'block';
      } else {
        weatherTab.style.display = 'none';
        runsTab.style.display = 'block';
        weatherBtn.classList.remove('active');
        runsBtn.classList.add('active');
        searchInput.style.display = 'none';
        loadRuns();
      }
    }

    // Fetch and update top summary statistics
    async function loadSummary() {
      try {
        const res = await fetch('/etl/summary');
        if (!res.ok) throw new Error('Failed to load summary');
        const data = await res.json();

        document.getElementById('statTotalRows').innerText = (data.total_records || 0).toLocaleString();
        document.getElementById('statCitiesCount').innerText = data.cities_count || 25;
        document.getElementById('statTotalRuns').innerText = (data.total_runs || 0).toLocaleString();

        if (data.latest_run) {
          const dur = data.latest_run.duration_seconds ? data.latest_run.duration_seconds + 's' : '--';
          document.getElementById('statLastDuration').innerText = dur;
          document.getElementById('statLastRunId').innerText = 'Latest: ' + data.latest_run.run_id + ' (' + data.latest_run.status + ')';
        }

        // Database status pill
        const dbBadge = document.getElementById('dbBadge');
        const dbText = document.getElementById('dbBadgeText');
        if (data.status === 'healthy') {
          dbBadge.className = 'badge badge-success';
          dbText.innerText = 'MySQL: ' + data.database_name + ' (Connected)';
        } else {
          dbBadge.className = 'badge';
          dbBadge.style.color = '#ef4444';
          dbText.innerText = 'MySQL Degraded';
        }
      } catch (err) {
        console.warn('Could not fetch summary stats:', err);
      }
    }

    // Fetch latest weather observations across 25 cities
    async function loadLatestWeather() {
      const tbody = document.getElementById('weatherTableBody');
      try {
        const res = await fetch('/etl/latest');
        if (!res.ok) throw new Error('HTTP ' + res.status);
        weatherData = await res.json();
        renderWeatherTable(weatherData);
      } catch (err) {
        tbody.innerHTML = `<tr><td colspan="7" style="text-align: center; color: var(--danger); padding: 24px;">Failed to load weather: ${err.message}</td></tr>`;
      }
    }

    // Render weather rows
    function renderWeatherTable(items) {
      const tbody = document.getElementById('weatherTableBody');
      if (!items || items.length === 0) {
        tbody.innerHTML = `<tr><td colspan="7" style="text-align: center; color: var(--text-muted); padding: 30px;">No weather records found. Click 'Trigger ETL Pipeline' above to load data.</td></tr>`;
        return;
      }

      tbody.innerHTML = items.map(item => {
        let tempBadgeClass = 'temp-cool';
        const temp = item.temperature_2m;
        if (temp !== null && temp !== undefined) {
          if (temp >= 22) tempBadgeClass = 'temp-warm';
          else if (temp < 10) tempBadgeClass = 'temp-cold';
        }

        const tempStr = temp !== null && temp !== undefined ? `${temp}°C` : 'N/A';
        const humStr = item.relative_humidity_2m !== null ? `${item.relative_humidity_2m}%` : 'N/A';
        const precStr = item.precipitation !== null ? `${item.precipitation} mm` : '0 mm';
        const timeStr = item.recorded_at ? item.recorded_at.replace('T', ' ') : '--';
        const updatedStr = item.updated_at ? item.updated_at.replace('T', ' ') : '--';

        return `
          <tr>
            <td style="font-weight: 600; color: #ffffff;">${item.city_name}</td>
            <td style="color: var(--text-muted); font-size: 12px;">${item.latitude}, ${item.longitude}</td>
            <td style="color: var(--primary); font-family: monospace;">${timeStr}</td>
            <td><span class="temp-badge ${tempBadgeClass}">${tempStr}</span></td>
            <td>${humStr}</td>
            <td>${precStr}</td>
            <td style="color: var(--text-muted); font-size: 12px;">${updatedStr}</td>
          </tr>
        `;
      }).join('');
    }

    // Filter weather table via search input
    function filterWeatherTable() {
      const term = document.getElementById('citySearch').value.toLowerCase().trim();
      if (!term) {
        renderWeatherTable(weatherData);
        return;
      }
      const filtered = weatherData.filter(c => c.city_name.toLowerCase().includes(term));
      renderWeatherTable(filtered);
    }

    // Fetch and render pipeline execution runs
    async function loadRuns() {
      const tbody = document.getElementById('runsTableBody');
      try {
        const res = await fetch('/etl/runs?limit=15');
        if (!res.ok) throw new Error('HTTP ' + res.status);
        const runs = await res.json();

        if (!runs || runs.length === 0) {
          tbody.innerHTML = `<tr><td colspan="7" style="text-align: center; color: var(--text-muted); padding: 30px;">No audit runs found.</td></tr>`;
          return;
        }

        tbody.innerHTML = runs.map(r => {
          const isSuccess = r.status === 'SUCCESS';
          const statusStyle = isSuccess 
            ? 'background: rgba(16,185,129,0.15); color: #10b981;' 
            : 'background: rgba(239,68,68,0.15); color: #ef4444;';
          const durStr = r.duration_seconds ? `${r.duration_seconds}s` : '--';
          const startStr = r.started_at ? r.started_at.replace('T', ' ') : '--';

          return `
            <tr>
              <td style="font-family: monospace; font-size: 12px; color: var(--primary);">${r.run_id}</td>
              <td><span style="font-size: 12px; color: var(--text-muted);">${r.trigger_type}</span></td>
              <td><span style="padding: 2px 8px; border-radius: 6px; font-weight: 600; font-size: 11px; ${statusStyle}">${r.status}</span></td>
              <td>${r.cities_count || 25}</td>
              <td style="font-weight: 600;">${(r.records_loaded || 0).toLocaleString()}</td>
              <td style="font-family: monospace;">${durStr}</td>
              <td style="color: var(--text-muted); font-size: 12px;">${startStr}</td>
            </tr>
          `;
        }).join('');
      } catch (err) {
        tbody.innerHTML = `<tr><td colspan="7" style="text-align: center; color: var(--danger); padding: 24px;">Failed to load runs: ${err.message}</td></tr>`;
      }
    }

    // Trigger Pipeline Execution when Button is Clicked
    async function runPipeline() {
      const btn = document.getElementById('triggerBtn');
      const btnText = document.getElementById('triggerBtnText');
      const statusBox = document.getElementById('statusBox');
      const isBackground = document.getElementById('bgCheckbox').checked;

      // Lock UI
      btn.disabled = true;
      btnText.innerText = isBackground ? 'Scheduling Background Job...' : 'Ingesting 25 Cities...';
      statusBox.className = 'status-box loading';
      statusBox.innerHTML = `
        <div class="spinner"></div>
        <div>
          <strong>ETL Execution in Progress...</strong><br>
          <span style="font-size: 12px; opacity: 0.85;">Connecting to Open-Meteo REST API, transforming hourly metrics, and upserting into MySQL.</span>
        </div>
      `;

      try {
        const url = `/web-trigger-etl?background=${isBackground}`;
        const startTime = Date.now();
        const res = await fetch(url, { method: 'POST' });
        const elapsed = ((Date.now() - startTime) / 1000).toFixed(2);
        const data = await res.json();

        if (!res.ok || data.status === 'FAILED') {
          throw new Error(data.error || ('Server returned HTTP ' + res.status));
        }

        if (isBackground) {
          statusBox.className = 'status-box success';
          statusBox.innerHTML = `
            <strong>✅ Pipeline Job Dispatched Successfully!</strong>
            <p style="margin-top: 4px; font-size: 13px;">Job ID: <code>${data.job_id}</code>. The pipeline is running asynchronously in the background.</p>
          `;
        } else {
          statusBox.className = 'status-box success';
          statusBox.innerHTML = `
            <div style="display: flex; justify-content: space-between; align-items: center; flex-wrap: wrap; gap: 8px;">
              <strong>✅ Ingestion Complete: ${data.records_loaded || 0} Records Successfully Upserted!</strong>
              <span style="font-family: monospace; font-size: 12px; background: rgba(0,0,0,0.25); padding: 2px 8px; border-radius: 4px;">Run ID: ${data.run_id}</span>
            </div>
            <div class="receipt-grid">
              <div class="receipt-item">
                <div class="receipt-label">Cities Ingested</div>
                <div class="receipt-val">${data.cities_successful || 25} / ${data.cities_targeted || 25}</div>
              </div>
              <div class="receipt-item">
                <div class="receipt-label">Extracted</div>
                <div class="receipt-val">${(data.records_extracted || 0).toLocaleString()}</div>
              </div>
              <div class="receipt-item">
                <div class="receipt-label">Loaded to MySQL</div>
                <div class="receipt-val">${(data.records_loaded || 0).toLocaleString()}</div>
              </div>
              <div class="receipt-item">
                <div class="receipt-label">Execution Time</div>
                <div class="receipt-val">${data.duration_seconds || elapsed}s</div>
              </div>
            </div>
          `;
        }

        // Refresh all tables and summary numbers automatically!
        await refreshAllData();

      } catch (err) {
        statusBox.className = 'status-box error';
        statusBox.innerHTML = `
          <strong>❌ ETL Execution Error</strong>
          <p style="margin-top: 4px; font-size: 13px;">${err.message}</p>
        `;
      } finally {
        btn.disabled = false;
        btnText.innerText = 'Trigger ETL Pipeline';
      }
    }

    // Refresh all page components
    async function refreshAllData() {
      await Promise.all([
        loadSummary(),
        loadLatestWeather(),
        loadRuns()
      ]);
    }

    // Initialize on page load
    window.addEventListener('DOMContentLoaded', () => {
      refreshAllData();
    });
  </script>
</body>
</html>
"""
