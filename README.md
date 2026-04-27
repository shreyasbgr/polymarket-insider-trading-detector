# Polymarket Insider Trading Detector

> **Deployed Version**: [http://fireplace.shreyasbanagar.com:8000/](http://fireplace.shreyasbanagar.com:8000/)

A highly technical, real-time surveillance engine designed to detect insider trading and anomalous wallet behavior on Polymarket prediction markets. It combines deterministic rule-based analysis with Machine Learning (Isolation Forest) to surface highly suspicious wallets, presenting the intelligence in a real-time, responsive dashboard.

## Table of Contents
1. [Tech Stack](#tech-stack)
2. [System Architecture Diagram](#system-architecture-diagram)
3. [Service Breakdown](#service-breakdown)
4. [Data Flow Blueprint](#data-flow-blueprint)
5. [WebSocket Subsystem](#websocket-subsystem)
6. [API Documentation](#api-documentation)
7. [Installation & Getting Started](#installation--getting-started)

---

## Tech Stack
- **Backend**: Python 3.12, FastAPI, TaskIQ, Web3.py, asyncpg, aiohttp
- **Frontend**: HTML5, Vanilla JavaScript, CSS3, Chart.js
- **Databases**: PostgreSQL 16 (Relational/Transactional), ClickHouse (OLAP), Redis (Pub/Sub & Cache)
- **Infrastructure**: Docker Compose, RabbitMQ (Message Broker)

---

## System Architecture Diagram

```mermaid
graph TD
    %% External Data Sources
    subgraph External Sources
        AlchemyRPC[Alchemy RPC <br/> Polygon Node]
        TheGraph[The Graph <br/> Subgraph API]
    end

    %% Internal Microservices
    subgraph Docker Ecosystem
        API[API Engine <br/> FastAPI]
        Indexer[Indexer Service <br/> Web3.py]
        Worker[Background Worker <br/> TaskIQ]
        
        %% Databases & Brokers
        Postgres[(PostgreSQL <br/> Primary DB)]
        Clickhouse[(ClickHouse <br/> OLAP DB)]
        Redis[(Redis <br/> Cache & Pub/Sub)]
        RabbitMQ[(RabbitMQ <br/> Message Broker)]
    end

    %% Client Layer
    subgraph Client
        Browser[Dashboard UI <br/> HTML/JS/WebSockets]
    end

    %% Flow External to Internal
    AlchemyRPC -->|Live OrderFilled Logs| Indexer
    TheGraph -->|Historical Queries| Indexer
    AlchemyRPC -->|First Deposit Txs| Worker

    %% Indexer Storage
    Indexer -->|Batch Insert Trades| Postgres
    Indexer -->|Batch Insert Trades| Clickhouse
    Indexer -->|Publish 'historical_batch' / 'live_trade'| Redis

    %% Worker Operations
    API -->|Queue ML/Enrich Tasks| RabbitMQ
    RabbitMQ -->|Consume Tasks| Worker
    Worker -->|Fetch Wallet Profiles| Postgres
    Worker -->|Execute ML Aggregations| Clickhouse
    Worker -->|Update Anomaly Scores| Postgres
    Worker -->|Publish 'pipeline_complete'| Redis

    %% API Layer
    Redis -->|Subscribe 'alerts' channel| API
    API <-->|REST API Queries| Postgres
    API -->|Broadcast WebSocket Events| Browser
    Browser -->|Fetch UI Data| API
```

---

## Service Breakdown

### 1. API Engine (FastAPI)
The primary interface for client applications, running via Uvicorn on an ASGI server.
- **Responsibilities**: Serves the static assets (HTML/JS/CSS), manages RESTful routing for the dashboard, and handles lifecycle events. 
- **Connection Management**: Instantiates the global connection pool for Postgres (`asyncpg`) and maintains an active `asyncio.Task` listening to the Redis Pub/Sub channel. When messages arrive, they are instantly fanned out to all connected `WebSocket` clients.

### 2. Indexer Service
A continuously running Python process tasked with blockchain synchronization.
- **Backfill Phase**: Uses `aiohttp` to query The Graph's Polymarket subgraph, paginating through historical `OrderFilled` events using timestamp cursors. Trades are mapped into Postgres/ClickHouse schemas.
- **Live Phase**: Connects to Alchemy via `Web3.py` with PoA middleware injected. It uses `eth_getLogs` to poll blocks for the specific `OrderFilled` signature hash. 
- **Performance**: To prevent DB locks and connection exhaustion, the indexer groups transactions and executes bulk inserts using `executemany` against connection pools.

### 3. Background Worker (TaskIQ)
An asynchronous worker process mapped to consume queues from RabbitMQ.
- **Wallet Enrichment**: Calls Polygon RPCs to fetch the very first transaction hash for a wallet, establishing wallet creation age.
- **Machine Learning**: Uses `scikit-learn`'s `IsolationForest`. Instead of querying millions of rows from Postgres, it routes aggregation queries (`sum`, `count`, `max`) to **ClickHouse**, which calculates the multi-dimensional feature matrix in milliseconds. It then trains the model and flags statistical outliers.
- **Deterministic Rules Engine**: Calculates an `insider_score` via hardcoded risk heuristics (e.g., trade concentration >90%, entry timing <2 hours to resolution).

### 4. PostgreSQL 16
The source of truth for application state and relationships.
- **Schema**: Houses the `wallets` table (containing all risk scores and timestamps), the `markets` table (resolutions and questions), and the transactional `trades` table.
- **Concurrency**: Specifically tuned with connection pooling (`DB_POOL_MIN`, `DB_POOL_MAX`) to handle massive concurrent `INSERT ON CONFLICT DO NOTHING` statements from the Indexer without blocking API reads.

### 5. ClickHouse
A columnar database heavily optimized for OLAP workloads.
- **Function**: Replicates the `trades` table structure. Designed exclusively for the ML Worker to perform blazing-fast analytical queries over the entire history of trades, circumventing the traditional index bottlenecks of B-Tree relational databases.

### 6. Redis
- **Volatile Storage**: Used for temporary API request caching (e.g., global stats) to prevent database hammering during traffic spikes.
- **Pub/Sub**: The backbone of the real-time alerting system. The isolated Docker containers (`Indexer`, `Worker`) use Redis `PUBLISH` to send events (like trade insertions or pipeline completions), which the API engine `SUBSCRIBE`s to.

### 7. RabbitMQ
- **Message Broker**: Serves as the durable queue holding serialized background tasks created by the API. Ensures tasks like `run_full_pipeline` or `score_all_wallets` are robustly distributed to the TaskIQ worker and guarantees delivery even if a container restarts.

---

## Data Flow Blueprint

1. **Ingestion Loop**: 
   - The `Indexer` service queries The Graph for past trades and Alchemy's RPC for live `OrderFilled` events.
   - It parses raw blockchain hex data and ABI arguments into human-readable structures (e.g., converting token amounts based on decimals to standard USDC formats).
   - Cleaned trade records are batch-inserted into both **PostgreSQL** and **ClickHouse** simultaneously.
   
2. **Event Broadcasting**:
   - Following a successful batch insertion, the Indexer publishes a `historical_batch` or `live_trade` event to **Redis Pub/Sub**.
   - The FastAPI Engine listens to this channel, parses the JSON payload, and forwards it to active **WebSocket** clients. The Dashboard UI intercepts this message and forces a selective re-fetch of trade data to maintain real-time parity.

3. **Wallet Profiling (Enrichment)**:
   - When new wallets are detected, the API enqueues a task to RabbitMQ. The `Worker` consumes it, queries Polygon for the wallet's first inbound transaction, and updates the `first_deposit_at` column in Postgres.

4. **Threat Detection Cycle**:
   - The ML worker queries ClickHouse to extract aggregated wallet behaviors (total trades, market diversification, maximum single-trade exposure). It trains an `IsolationForest` model to detect outliers and assigns an `anomaly_score` to Postgres.
   - Concurrently, the rule-based engine assigns an `insider_score` based on deterministic vectors (e.g., entering maximum capital strictly 2 hours before a market resolves).
   - A unified `global_score` is computed (60% Rules, 40% ML) and saved to Postgres. 

5. **Actionable Intelligence**:
   - The frontend consumes these scores via the `/api/flagged` REST endpoint to visualize a ranked, paginated list of suspicious wallets, continuously re-hydrated by WebSocket prompts.

---

## WebSocket Subsystem

The application leverages WebSockets to achieve zero-latency UI updates, negating the need for aggressive, resource-intensive HTTP polling.

- **Cross-Container Pub/Sub**: Because FastAPI operates in an isolated Docker container, background services like the `Indexer` cannot directly push to the API's WebSocket connections. To bridge this, all services serialize events to JSON and publish them to a Redis channel named `alerts`.
- **Async Fan-out**: A dedicated background loop inside FastAPI's startup lifecycle (`asyncio.create_task`) continuously listens to the Redis channel. When a message is detected, it pushes it to an in-memory `set` containing all currently active `WebSocket` client connections.
- **Client Cache Invalidation**: The dashboard connects to `ws://{HOST}/ws/alerts`. Upon receiving a `live_trade` or `pipeline_complete` socket message, the UI selectively invalidates its HTML DOM state and queries the API for fresh datasets.

---

## API Documentation

### GET Endpoints

#### 1. Retrieve Flagged Wallets
**Endpoint**: `GET /api/flagged`
**Description**: Fetches a paginated, sorted list of wallets that meet the minimum risk thresholds.
**Parameters**:
- `page` (int): Page index.
- `per_page` (int): Results per page (max 100).
- `sort_by` (str): Column to rank by (`global_score`, `insider_score`, `anomaly_score`).
- `sort_dir` (str): Sort direction (`asc`, `desc`).
- `search` (str): Wallet address filter.

#### 2. Retrieve Wallet Deep Dive
**Endpoint**: `GET /api/wallets/{address}`
**Description**: Fetches granular diagnostic details, trade ledger, and scoring breakdown.

#### 3. Retrieve Trade Feeds
**Endpoints**: `GET /api/trades/historical` & `GET /api/trades/live`
**Description**: Fetches recent trades by ingestion source.

#### 4. Diagnostic Health Checks
**Endpoint**: `GET /api/admin/health-check`
**Description**: Live TCP/HTTP ping across internal and external services.

### POST Endpoints (Admin Triggers)

> **Security Note**: Admin actions (Pause, Reset) triggered via the Dashboard UI require the administrative password (`admin123`).

#### 1. Rescore All Wallets
**Endpoint**: `POST /api/admin/rescore-all`
**Description**: Re-evaluates all existing wallets in the database (Rules + ML) without fetching new trades.

#### 2. Trigger Pipeline Sync
**Endpoint**: `POST /api/admin/sync`
**Description**: Flushes cache and queues a full backfill/scoring task.

#### 3. Toggle Live Polling
**Endpoint**: `POST /api/admin/toggle-pause`
**Description**: Pauses/Resumes the on-chain live trade indexer.

#### 4. Nuclear Reset
**Endpoint**: `POST /api/admin/reset`
**Description**: **Destructive.** Wipes all database tables, ClickHouse data, and Redis caches.

---

## Installation & Getting Started

### Prerequisites
- Docker and Docker Compose
- Alchemy API Key (Polygon Mainnet)
- The Graph API Key

### Configuration (`.env`)
Create a `.env` file in the root directory:
```env
# Essential RPC & API Keys
ALCHEMY_RPC_URL=https://polygon-mainnet.g.alchemy.com/v2/YOUR_KEY
SUBGRAPH_URL=https://gateway.thegraph.com/api/YOUR_KEY/subgraphs/id/...

# Database Connections (Docker defaults)
DATABASE_URL=postgresql+asyncpg://postgres:postgres@db:5432/polymarket
REDIS_URL=redis://redis:6379/0
RABBITMQ_URL=amqp://guest:guest@rabbitmq:5672/
CLICKHOUSE_HOST=clickhouse

# Scoring & Tuning
INSIDER_THRESHOLD=0.65
POLL_INTERVAL_SECONDS=15
```

### Installation
1. Clone the repository.
2. Configure `.env` as shown above.
3. Start the system:
   ```bash
   docker compose up -d --build
   ```
4. Access the dashboard at `http://localhost:8000`.

---

## License
MIT License.
