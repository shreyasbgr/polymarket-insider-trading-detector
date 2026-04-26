# Codebase Architecture Guide (Fireplace Standard)

This document explains the technical implementation of the professional stack used in the Insider Detector.

---

## 🏛️ Component Breakdown

### 1. The Databases: Postgres vs. ClickHouse
We use a **Hybrid Database Strategy** to handle both transaction processing and massive analytics.

- **Postgres (`db/pool.py`)**:
    - **Usage**: Stores "Entities" (Wallets, Markets).
    - **Why**: Excellent for relational integrity and single-row lookups. When you click a wallet on the dashboard, Postgres handles the metadata.
- **ClickHouse (`db/clickhouse.py`)**:
    - **Usage**: Stores "Events" (Trades).
    - **Why**: An OLAP (Columnar) database. It stores trades in a way that makes aggregation (summing volume, counting unique markets) 100x faster than Postgres.
    - **Engine**: We use `ReplacingMergeTree` to automatically handle duplicate trades based on their transaction hash.

### 2. The Messaging: RabbitMQ & TaskIQ
To keep the UI responsive, we never run long jobs in the API process.

- **RabbitMQ (`core/broker.py`)**:
    - **Role**: The message broker. It stores "job tickets" until a worker is ready to process them.
- **TaskIQ (`core/tasks.py`)**:
    - **Role**: The task orchestrator. It allows us to define functions as `@broker.task` and run them asynchronously using `.kiq()`.
    - **Worker**: A separate process (`worker_entrypoint.py`) that waits for tasks from RabbitMQ.

### 3. The Fast Layer: Redis
- **Role**: Memory-resident cache.
- **Usage**: Caches the dashboard stats (`/api/stats`) for 10 seconds. This prevents the server from querying the database on every page refresh, ensuring sub-millisecond response times for the terminal dashboard.

---

## 🛠️ Logic Folders

### `indexers/`
- **`trades.py`**: Fetches trades from The Graph (Polygon). Writes to **both** Postgres (for UI) and ClickHouse (for scoring).
- **`deposits.py`**: Enriches wallets with their "First Deposit" timestamp to calculate wallet age.

### `detection/`
- **`scorer.py`**: Layer 1 Detection. Pulls aggregated stats from **ClickHouse** and applies the 5-factor deterministic rules.
- **`anomaly.py`**: Layer 2 Detection. Pulls a high-dimensional feature matrix from **ClickHouse** and runs the **Isolation Forest** ML model.

### `api/`
- **`main.py`**: The entry point. Handles FastAPI routes, WebSockets, and TaskIQ initialization.

---

## 🔄 The Data Lifecycle

1.  **Ingestion**: `trades.py` pulls data → Writes to Postgres & ClickHouse.
2.  **Task Trigger**: User clicks "Sync" → API sends a task to RabbitMQ.
3.  **Processing**: Worker receives task → Runs `score_all_wallets()` in `scorer.py`.
4.  **Analytics**: `scorer.py` asks ClickHouse for aggregated wallet stats.
5.  **Output**: Final scores are written back to Postgres.
6.  **Notification**: Worker (future) or API broadcasts update via WebSockets to the Dashboard.
