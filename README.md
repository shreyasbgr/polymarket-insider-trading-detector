# Polymarket Insider Trading Detector

A real-time surveillance engine designed to detect insider trading and anomalous wallet behavior on Polymarket prediction markets. It combines deterministic rule-based analysis with Machine Learning (Isolation Forest) to surface highly suspicious wallets, presenting the intelligence in a real-time, responsive dashboard.

## Features
- **Live On-Chain Indexing**: Monitors Polymarket's `OrderFilled` events on the Polygon blockchain in near real-time.
- **Historical Backfill**: Aggregates past trading activity using Polymarket's subgraph on The Graph.
- **Hybrid Scoring Engine**: 
  - *Deterministic Rules*: Evaluates trade concentration, timing relative to market resolution, trade sizes, and wallet age.
  - *ML Anomaly Detection*: Uses an Isolation Forest algorithm to detect statistically improbable patterns across multiple dimensions.
- **Unified Risk Score**: Generates a composite global risk score for each wallet.
- **Real-Time Dashboard**: A fast, WebSocket-powered frontend that streams live trades, backfill progress, and flagged anomalies directly to the user.

## System Architecture

The application is built on a microservices-inspired architecture managed by Docker Compose. 

### Core Components
1. **API Engine (FastAPI)**: Serves the dashboard frontend, exposes REST endpoints, and manages WebSocket connections to push real-time alerts and state updates to clients.
2. **Indexer**: A dedicated service that orchestrates the data pipeline. It backfills historical trades from The Graph and transitions seamlessly into polling Alchemy RPCs for live blockchain logs.
3. **Worker (TaskIQ)**: Handles asynchronous background jobs. It manages the enrichment of wallet data (e.g., finding deposit timestamps), training the ML model, and scoring all active wallets.
4. **PostgreSQL**: The primary relational database for persistent state. Stores wallet profiles, risk scores, market metadata, and normalized trade events.
5. **ClickHouse**: An OLAP database utilized for high-speed analytical queries. The ML engine queries ClickHouse to rapidly extract and aggregate wallet behavioral features.
6. **Redis**: Serves as the caching layer and the Pub/Sub backbone for propagating WebSocket alerts from the background worker and indexer to the API engine.
7. **RabbitMQ**: The message broker that queues and routes background tasks between the API engine and the TaskIQ worker.

### Data Flow
1. **Ingestion**: The `Indexer` service queries The Graph for historical trades and Alchemy's RPC for live `OrderFilled` events, inserting raw trade events into PostgreSQL and ClickHouse simultaneously.
2. **Event Broadcasting**: As batches of trades are saved, the Indexer publishes `historical_batch` or `live_trade` events to Redis Pub/Sub, which the API Engine broadcasts over WebSockets to update the UI instantly.
3. **Enrichment**: The TaskIQ `Worker` picks up new wallets and queries the Polygon blockchain to determine wallet creation times (first deposit timestamps).
4. **Detection Pipeline**:
   - The ML worker queries ClickHouse to build behavioral profiles (trade counts, diversification, max exposure) and trains an Isolation Forest model to assign an `anomaly_score`.
   - The rule-based engine assigns an `insider_score` based on deterministic risk vectors (e.g., trading huge volume hours before market resolution).
5. **Actionable Intelligence**: A unified `global_score` is computed and saved to PostgreSQL. The dashboard queries the API to display a ranked list of the highest-risk wallets with detailed breakdowns of *why* they were flagged.

## Getting Started

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

## Tech Stack
- **Backend**: Python, FastAPI, TaskIQ, Web3.py, asyncpg
- **Frontend**: HTML5, Vanilla JavaScript, CSS3, Chart.js
- **Databases**: PostgreSQL 16, ClickHouse, Redis
- **Infrastructure**: Docker Compose, RabbitMQ
