# Polymarket Insider Trading Detector

A highly technical, real-time surveillance engine designed to detect insider trading and anomalous wallet behavior on Polymarket prediction markets. It combines deterministic rule-based analysis with Machine Learning (Isolation Forest) to surface highly suspicious wallets, presenting the intelligence in a real-time, responsive dashboard.

## Table of Contents
1. [Tech Stack](#tech-stack)
2. [System Architecture](#system-architecture)
   - [Components Overview](#components-overview)
   - [Data Flow](#data-flow)
   - [WebSocket Subsystem](#websocket-subsystem)
3. [API Documentation](#api-documentation)
4. [Installation & Getting Started](#installation--getting-started)

---

## Tech Stack
- **Backend**: Python 3.12, FastAPI, TaskIQ, Web3.py, asyncpg
- **Frontend**: HTML5, Vanilla JavaScript, CSS3, Chart.js
- **Databases**: PostgreSQL 16 (Relational/Transactional), ClickHouse (OLAP), Redis (Pub/Sub & Cache)
- **Infrastructure**: Docker Compose, RabbitMQ (Message Broker)

---

## System Architecture

The application is built on a distributed, microservices-inspired architecture designed for high throughput and real-time processing.

### Components Overview

1. **API Engine (FastAPI)**
   - **Function**: Acts as the primary ingress/egress point. It serves the static dashboard frontend, exposes RESTful API endpoints for historical data queries, and maintains persistent WebSocket connections with clients for real-time alerts.
   - **Key Technologies**: `FastAPI` (ASGI), `uvicorn`, `asyncio`.

2. **Indexer Service**
   - **Function**: Orchestrates the data ingestion pipeline. On startup, it performs a bulk backfill of historical trades via Polymarket's Subgraph (GraphQL). It then transitions to live mode, continuously polling Alchemy's RPC for live Polygon blockchain logs matching the `OrderFilled` smart contract event.
   - **Key Technologies**: `Web3.py`, GraphQL HTTP clients, `asyncpg` for batch database insertion.

3. **Background Worker (TaskIQ)**
   - **Function**: Handles asynchronous, compute-heavy jobs detached from the API's event loop. It manages the enrichment of wallet profiles (e.g., resolving first deposit timestamps), training the ML model, and scoring wallets.
   - **Key Technologies**: `TaskIQ`, `scikit-learn` (Isolation Forest).

4. **PostgreSQL**
   - **Function**: The primary relational source of truth. Stores wallet profiles, final computed risk scores (`insider_score`, `anomaly_score`, `global_score`), market metadata, and normalized trade events.

5. **ClickHouse**
   - **Function**: Specialized OLAP database used strictly for high-speed analytical queries. The ML engine queries ClickHouse to instantly aggregate millions of trades into behavioral feature matrices without lagging the transactional Postgres DB.

6. **Redis**
   - **Function**: Provides volatile caching (e.g., API statistics) and serves as the Pub/Sub backbone. Both the Indexer and Worker publish JSON alert payloads to a Redis channel, which the FastAPI engine consumes and broadcasts to WebSocket clients.

7. **RabbitMQ**
   - **Function**: Message broker that reliably queues and routes background task requests (e.g., "Run full ML pipeline") from the API engine to available TaskIQ workers.

### Data Flow

1. **Ingestion (Historical & Live)**
   - The `Indexer` service queries The Graph for past trades and Alchemy's RPC for live `OrderFilled` events.
   - It parses raw blockchain hex data into human-readable amounts (e.g., converting token decimals to USDC).
   - Cleaned trade records are batch-inserted into both **PostgreSQL** and **ClickHouse** simultaneously.
   
2. **Event Broadcasting**
   - After a batch insertion, the Indexer publishes a `historical_batch` or `live_trade` event to **Redis Pub/Sub**.
   - The FastAPI Engine, listening on this Redis channel, forwards the event to all connected UI clients via **WebSockets**, forcing the dashboard to dynamically fetch the new trades.

3. **Enrichment**
   - The TaskIQ `Worker` identifies wallets missing creation timestamps and queries Polygon for their first inbound transaction, updating Postgres.

4. **Detection Pipeline**
   - **Machine Learning**: The worker queries ClickHouse to aggregate wallet behaviors (trade counts, diversification, max exposure). It trains an `IsolationForest` model to detect outliers, assigning an `anomaly_score` to Postgres.
   - **Deterministic Rules**: The rule-based engine assigns an `insider_score` based on risk vectors like trading huge volume exclusively hours before a market resolves.

5. **Actionable Intelligence**
   - A unified `global_score` is computed (60% Rules, 40% ML) and saved to PostgreSQL. 
   - The frontend consumes these scores via the `/api/flagged` REST endpoint to visualize a ranked list of suspicious wallets.

### WebSocket Subsystem

The application relies heavily on WebSockets to achieve a "live" feel without aggressive HTTP polling.

- **Redis Pub/Sub Integration**: Because FastAPI runs in an isolated container, the background `Indexer` and `Worker` containers cannot communicate directly with the WebSocket connections held by the API. To bridge this, all services publish JSON messages to a Redis channel named `alerts`.
- **Async Broadcast**: A dedicated `asyncio.create_task` loop inside the FastAPI lifespan continuously listens to the Redis channel. When a message arrives, it serializes the payload and pushes it to an in-memory `set` of all active `WebSocket` client connections.
- **Client Handling**: The frontend connects to `ws://localhost:8000/ws/alerts` on load. Upon receiving a `live_trade` or `pipeline_complete` message, the UI selectively invalidates its cache and re-fetches the necessary data segments.

---

## API Documentation

### 1. Retrieve Flagged Wallets
**Endpoint**: `GET /api/flagged`
**Description**: Fetches a paginated list of wallets sorted by risk score.
**Query Parameters**:
- `page` (int): Page number (default: 1)
- `per_page` (int): Results per page (default: 25)
- `sort_by` (str): Sort field (`global_score`, `insider_score`, `anomaly_score`)
- `search` (str): Optional wallet address prefix to filter by.

**Expected Output**:
```json
{
  "total": 124,
  "page": 1,
  "per_page": 25,
  "pages": 5,
  "wallets": [
    {
      "address": "0x123abc...",
      "insider_score": 0.85,
      "anomaly_score": 0.91,
      "global_score": 0.88,
      "flagged": true,
      "first_deposit_at": "2023-10-14T08:30:00",
      "scored_at": "2023-11-01T12:00:00"
    }
  ]
}
```

### 2. Retrieve Wallet Details
**Endpoint**: `GET /api/wallets/{address}`
**Description**: Fetches deep diagnostic details and a chronological trade ledger for a specific wallet.

**Expected Output**:
```json
{
  "address": "0x123abc...",
  "global_score": 0.88,
  "verdict": "High-risk anomaly. Extreme concentration prior to resolution.",
  "breakdown": {
    "entry_timing": 1.0,
    "trade_concentration": 0.8,
    "trade_size": 0.7,
    "wallet_age": 1.0,
    "market_count": 1.0
  },
  "trade_count": 4,
  "max_trade_usdc": 25000.50,
  "trades": [
    {
      "tx_hash": "0xabc123...",
      "condition_id": "0x567def...",
      "usdc_amount": 25000.50,
      "price": 0.99,
      "traded_at": "2023-10-31T23:55:00"
    }
  ]
}
```

### 3. Retrieve Live Trades
**Endpoint**: `GET /api/trades/live`
**Description**: Fetches the most recently detected on-chain trades.
**Query Parameters**:
- `limit` (int): Max records to return (default: 50)

**Expected Output**:
```json
[
  {
    "tx_hash": "0xdef456...",
    "maker": "0x123abc...",
    "taker": "0x987zyx...",
    "usdc_amount": 1500.00,
    "price": 0.55,
    "traded_at": "2023-11-01T12:05:00"
  }
]
```

### 4. Admin Diagnostics
**Endpoint**: `GET /api/admin/health-check`
**Description**: Performs connectivity checks against all internal distributed systems and external RPCs.

**Expected Output**:
```json
{
  "postgres": { "status": "ok", "message": "Connected" },
  "redis": { "status": "ok", "message": "Connected" },
  "clickhouse": { "status": "ok", "message": "Connected" },
  "rabbitmq": { "status": "ok", "message": "Connected" },
  "alchemy": { "status": "ok", "message": "Block: 56789123" },
  "the_graph": { "status": "ok", "message": "Subgraph Responsive" }
}
```

---

## Installation & Getting Started

### Prerequisites
- Docker and Docker Compose
- Alchemy API Key (Polygon Mainnet)
- The Graph API Key

### Installation
1. Clone the repository and navigate into the directory.
2. Configure your `.env` file with your API keys:
   ```env
   ALCHEMY_RPC_URL=https://polygon-mainnet.g.alchemy.com/v2/YOUR_KEY
   SUBGRAPH_URL=https://gateway.thegraph.com/api/YOUR_KEY/...
   ```
3. Start the system:
   ```bash
   docker compose up -d --build
   ```
4. Access the dashboard at `http://localhost:8000`.
