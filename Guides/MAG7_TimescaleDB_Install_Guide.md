
# MAG7 TimescaleDB Installation Guide (Local / Self-Hosted)

## ✅ Prerequisites
- PostgreSQL 14 or higher installed (Windows version)
- TimescaleDB extension installed for matching PostgreSQL version
- `psql` available in PATH

## ✅ Step 1: Create the MAG7 Database
From a PowerShell terminal or Command Prompt, open `psql`:

```bash
psql -U postgres
```

Then, at the `psql` prompt, run:

```sql
CREATE DATABASE "Mag7_Data";
\c Mag7_Data
CREATE EXTENSION IF NOT EXISTS timescaledb;
```

> If you see an error that TimescaleDB must be preloaded, do the following:

### 🔧 Edit postgresql.conf:
Add the following line (anywhere, preferably near the bottom):

```
shared_preload_libraries = 'timescaledb'
```

Then **restart PostgreSQL service**:

```powershell
Restart-Service postgresql-x64-17
```

---

## ✅ Step 2: Create Required Tables

Run the following in `psql` after connecting to `Mag7_Data`:

```sql
-- Tick-by-tick market data
CREATE TABLE mag7_ticks (
    timestamp TIMESTAMPTZ NOT NULL,
    symbol TEXT NOT NULL,
    bid DOUBLE PRECISION,
    ask DOUBLE PRECISION,
    last DOUBLE PRECISION,
    volume BIGINT,
    PRIMARY KEY (timestamp, symbol)
);
SELECT create_hypertable('mag7_ticks', 'timestamp', if_not_exists => TRUE);

-- 1-minute OHLCV candles
CREATE TABLE mag7_ohlcv_1m (
    bucket_time TIMESTAMPTZ NOT NULL,
    symbol TEXT NOT NULL,
    open DOUBLE PRECISION,
    high DOUBLE PRECISION,
    low DOUBLE PRECISION,
    close DOUBLE PRECISION,
    volume BIGINT,
    PRIMARY KEY (bucket_time, symbol)
);
SELECT create_hypertable('mag7_ohlcv_1m', 'bucket_time', if_not_exists => TRUE);

-- Strategy signal logging
CREATE TABLE mag7_signals (
    event_time TIMESTAMPTZ NOT NULL,
    symbol TEXT NOT NULL,
    strategy TEXT NOT NULL,
    signal_type TEXT NOT NULL,
    value DOUBLE PRECISION,
    note TEXT,
    PRIMARY KEY (event_time, symbol, strategy)
);
SELECT create_hypertable('mag7_signals', 'event_time', if_not_exists => TRUE);
```

---

## ✅ Step 3: Confirm It Worked

To list tables:
```sql
\dt
```

Expected:
- mag7_ticks
- mag7_ohlcv_1m
- mag7_signals

---

You're now ready to begin streaming real-time market data into TimescaleDB.
