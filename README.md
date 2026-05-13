# Crypto Pipeline

A production-grade real-time cryptocurrency price streaming pipeline built with Python, Kafka, and PostgreSQL.

## Architecture

```
CoinGecko API
     │  (every 15s)
     ▼
  Producer  ──────────────────────────────────────────►  Dead-Letter Queue
     │                                                    (crypto-prices-dlq)
     │  Kafka topic: crypto-prices
     ▼
Smart Consumer
     │  retry + spike detection
     ▼
PostgreSQL
  ├── price_ticks    (every price, every coin)
  └── price_alerts   (spike alerts only)
     │
     ▼
FastAPI
  ├── REST endpoints  (latest prices, history, alerts)
  └── WebSocket       (live price stream)
```

## Tech Stack

| Layer | Technology |
|---|---|
| Message broker | Apache Kafka |
| Database | PostgreSQL 16 |
| API framework | FastAPI + Uvicorn |
| Config management | pydantic-settings |
| Logging | structlog (JSON or console) |
| Metrics | Prometheus client |
| Retries | Tenacity (exponential backoff) |
| DB migrations | Alembic |
| Testing | pytest + testcontainers |

## Project Structure

```
crypto-pipeline/
├── src/
│   └── crypto_pipeline/
│       ├── config.py            # All settings via .env
│       ├── logging_config.py    # structlog setup
│       ├── metrics.py           # Prometheus metrics
│       ├── producer.py          # Fetches prices → publishes to Kafka
│       ├── smart_consumer.py    # Kafka → PostgreSQL + spike detection
│       ├── consumer.py          # Basic price printer
│       ├── schemas/
│       │   └── price.py         # Pydantic PriceTick model
│       ├── db/
│       │   ├── connection.py    # ThreadedConnectionPool
│       │   └── queries.py       # All SQL queries
│       └── api/
│           └── app.py           # FastAPI app + WebSocket
├── alembic/                     # Database migrations
│   └── versions/
├── tests/
│   ├── conftest.py              # Postgres testcontainer fixture
│   ├── test_spike_detector.py   # Unit tests
│   ├── test_db_queries.py       # DB integration tests
│   └── test_api.py              # API endpoint tests
├── docker-compose.yml           # Kafka + Zookeeper + PostgreSQL
├── alembic.ini
├── pyproject.toml
└── .env.example
```

## Getting Started

### Prerequisites

- Python 3.11+
- Docker Desktop

### 1. Clone and install

```bash
git clone https://github.com/lokeshwaranpr/crypto-pipeline.git
cd crypto-pipeline
pip install -e .
```

### 2. Configure environment

```bash
cp .env.example .env
# Edit .env if needed — defaults work out of the box
```

### 3. Start infrastructure

```bash
docker compose up -d
```

> If Kafka fails to start, run `docker compose down && docker compose up -d` to clear stale Zookeeper state.

### 4. Apply database migrations

```bash
alembic upgrade head
```

### 5. Run the pipeline

Open three terminals:

**Terminal 1 — Producer** (fetches live prices → Kafka):
```bash
crypto-producer
```

**Terminal 2 — Consumer** (Kafka → PostgreSQL + spike alerts):
```bash
crypto-consumer
```

**Terminal 3 — API** (REST + WebSocket):
```bash
crypto-api
```

## API Endpoints

Base URL: `http://localhost:8080`

| Method | Endpoint | Description |
|---|---|---|
| GET | `/health` | Health check |
| GET | `/prices/latest` | Latest price for all coins |
| GET | `/prices/{coin}/history` | Price history (default: last 50) |
| GET | `/alerts` | Recent spike alerts |
| WS | `/ws/prices` | Live price stream |

Interactive docs: `http://localhost:8080/docs`

### Examples

```bash
# Latest prices
curl http://localhost:8080/prices/latest

# Bitcoin history (last 10)
curl http://localhost:8080/prices/bitcoin/history?limit=10

# Alerts for ethereum only
curl http://localhost:8080/alerts?coin=ethereum
```

## Observability

| Endpoint | Description |
|---|---|
| `http://localhost:8000/metrics` | Producer Prometheus metrics |
| `http://localhost:8001/metrics` | Consumer Prometheus metrics |

Key metrics:
- `crypto_messages_published_total` — messages sent per coin
- `crypto_messages_consumed_total` — messages processed per coin
- `crypto_db_write_duration_seconds` — DB write latency histogram
- `crypto_spike_alerts_total` — alerts triggered per coin + direction

## Running Tests

```bash
PYTHONPATH=src python -m pytest tests/ -v
```

Tests use [testcontainers](https://testcontainers.com/) to spin up a real PostgreSQL instance — no mocks.

## Configuration

All settings are read from `.env`. See `.env.example` for the full list.

| Variable | Default | Description |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker address |
| `KAFKA_TOPIC` | `crypto-prices` | Main topic |
| `POSTGRES_HOST` | `localhost` | DB host |
| `POSTGRES_PORT` | `5433` | DB port |
| `POSTGRES_DB` | `cryptodb` | Database name |
| `POSTGRES_USER` | `crypto` | DB user |
| `POSTGRES_PASSWORD` | `crypto123` | DB password |
| `FETCH_INTERVAL_SECONDS` | `15` | How often to fetch prices |
| `SPIKE_THRESHOLD_PCT` | `0.5` | Price change % to trigger alert |
| `MAX_RETRIES` | `5` | Retry attempts before DLQ |
| `LOG_FORMAT` | `console` | `console` or `json` |
| `API_PORT` | `8080` | FastAPI server port |

## Stopping the Pipeline

```bash
# Stop all services
docker compose down

# Stop and wipe database (fresh start)
docker compose down -v
```
