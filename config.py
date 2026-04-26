"""
config.py — Loads settings from the .env file.

python-dotenv reads the .env file and puts the values into
environment variables. We then read those with os.getenv().
"""
import os
from dotenv import load_dotenv

# This reads the .env file in the current directory
load_dotenv()

ALCHEMY_RPC_URL   = os.getenv("ALCHEMY_RPC_URL")
SUBGRAPH_URL      = os.getenv("SUBGRAPH_URL")
DATABASE_URL      = os.getenv("DATABASE_URL")
REDIS_URL         = os.getenv("REDIS_URL", "redis://localhost:6379/0")
RABBITMQ_URL      = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")
CLICKHOUSE_HOST   = os.getenv("CLICKHOUSE_HOST", "localhost")
INSIDER_THRESHOLD = float(os.getenv("INSIDER_THRESHOLD", "0.50"))
POLL_INTERVAL_SEC = int(os.getenv("POLL_INTERVAL_SECONDS", "15"))

# ── Derived config for the async engine ──
# asyncpg expects a plain postgresql:// DSN without the "+asyncpg" driver suffix.
DATABASE_DSN = DATABASE_URL.replace("+asyncpg", "") if DATABASE_URL else None

# Concurrency tuning (Safe for Alchemy free-tier)
GRAPH_CONCURRENCY   = 5
ALCHEMY_CONCURRENCY = 30
DB_POOL_MIN         = 5
DB_POOL_MAX         = 20
