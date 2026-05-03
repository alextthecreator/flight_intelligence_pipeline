# ✈️ Flight Intelligence Pipeline (Duffel-Postgres ETL)

## 📋 Project Overview
This project is a production-ready **Data Engineering Pipeline** designed to monitor, validate, and analyze flight price volatility. It demonstrates the ability to build automated workflows that transform raw API data into actionable market insights.

### 🏗️ Architecture & Tools
- **Source:** Duffel API (NDC-Standard)
- **Validation:** **Pydantic** (Strict type checking and data integrity)
- **Storage:** **Supabase / PostgreSQL** (Time-series data management)
- **Automation:** **GitHub Actions** (Daily Batch Processing)
- **Simulation:** Custom-built **Market Simulator** to bypass static sandbox data.

## ⚙️ Engineering Features

### 1. Automated ETL Flow
The pipeline executes a daily batch job that fetches data, cleans it, and performs a comparative analysis against historical records.

### 2. Data Integrity (The Pydantic Layer)
Every record is validated before ingestion. If the API returns unexpected data formats or null values, the pipeline logs the error and prevents database corruption.

### 3. Market Volatility Simulation
To demonstrate the alerting logic in a sandbox environment, this project includes a **Market Simulator**. It injects controlled variance (`± 50.00`) into the static test prices, allowing the system to calculate:
- **Price Change Percentage:** `((New - Old) / Old) * 100`
- **Market Trends:** Categorization into `UP`, `DOWN`, or `STABLE`.

### 4. Enterprise Security
- Zero hardcoded credentials.
- Environment variables managed via `.env` (local) and **GitHub Secrets** (cloud).

## 🚀 Future Enhancements
- [x] **Smart Alerting:** Integration with **Resend API** to trigger HTML email alerts when a `DOWN` trend exceeds a threshold.
- [ ] **Data Visualization:** A dashboard to visualize price trends over time.

## 🔔 Notification Module (Implemented)
The pipeline now includes an email notification layer:
1. Uses the `resend` Python library.
2. If trend is `DOWN` and price drop is greater than `3%`, it sends an alert.
3. Sends a professional HTML email with route, old/new price, percentage drop, and booking link.
4. Embeds a QuickChart line chart generated from recent route history (up to the last 7 days plus the current run).
5. Reads `RESEND_API_KEY` from environment variables (`.env` locally and GitHub Secrets in CI).
System notifications run in Sandbox mode (Resend API) and send alerts to the administrator's verified email address.

## 🛠️ Installation & Setup
1. Clone the repository.
2. Install dependencies: `pip install -r requirements.txt`.
3. Configure `.env` with your Duffel, Supabase, and Resend keys.
4. Run the pipeline manually: `python main.py` or wait for the GitHub Action trigger.

## ▶️ Run from This Folder
```bash
cd stdnt/flight_intelligence_pipeline
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
python main.py
```

## 🔐 Required Environment Variables
- `DUFFEL_TOKEN`
- `SUPABASE_URL`
- `SUPABASE_KEY` (backend secret/service key)
- `RESEND_API_KEY`
- `ALERT_EMAIL_TO`
- `RESEND_FROM_EMAIL` (optional if you keep default `onboarding@resend.dev`)

In GitHub Actions, add these values under repository **Secrets and variables → Actions → New repository secret**.
