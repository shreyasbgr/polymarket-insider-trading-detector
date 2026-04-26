"""
db/pool.py — Async database connection pool (production engine).

WHY A POOL?
  Opening a new database connection takes ~5-20ms. If you do that 100,000
  times (once per trade insert), you waste 15-30 minutes just on connections.

  A connection pool opens 5-20 connections ONCE at startup, then hands them
  out to any function that needs one. When the function is done, the
  connection goes back into the pool — not closed, just recycled.

  Result: near-zero connection overhead for every database operation.

USAGE:
  pool = await get_pool()
  async with pool.acquire() as conn:
      await conn.execute("INSERT INTO ...")
"""
import asyncpg
import config

_pool: asyncpg.Pool | None = None


async def get_pool() -> asyncpg.Pool:
    """
    Return the global connection pool, creating it on first use.

    This is a singleton — no matter how many times you call get_pool(),
    you always get the same pool object. The pool manages 5-20 live
    connections to PostgreSQL internally.
    """
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(
            dsn=config.DATABASE_DSN,
            min_size=config.DB_POOL_MIN,
            max_size=config.DB_POOL_MAX,
        )
    return _pool


async def close_pool():
    """Gracefully close all connections when the app shuts down."""
    global _pool
    if _pool is not None:
        await _pool.close()
        _pool = None


async def create_tables():
    """
    Create all tables if they do not exist.
    Uses the async pool — call this once at startup.
    """
    print("  -> get_pool()...")
    pool = await get_pool()
    print("  -> pool.acquire()...")
    async with pool.acquire() as conn:
        print("  -> executing create wallets...")
        # --- wallets table ---
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS wallets (
                address           TEXT PRIMARY KEY,
                first_deposit_tx  TEXT,
                first_deposit_at  TIMESTAMP,
                insider_score     FLOAT,
                anomaly_score     FLOAT,
                global_score      FLOAT,
                flagged           BOOLEAN DEFAULT FALSE,
                scored_at         TIMESTAMP
            )
        """)

        # --- markets table ---
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS markets (
                condition_id  TEXT PRIMARY KEY,
                question      TEXT,
                end_time      TIMESTAMP
            )
        """)

        # --- trades table ---
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS trades (
                tx_hash      TEXT,
                log_index    TEXT,
                maker        TEXT,
                taker        TEXT,
                condition_id TEXT,
                asset_id     TEXT,
                usdc_amount  FLOAT,
                price        FLOAT,
                block_number INTEGER,
                traded_at    TIMESTAMP,
                PRIMARY KEY (tx_hash, log_index)
            )
        """)

        # --- Indexes ---
        indexes = {
            "idx_trades_maker": "CREATE INDEX idx_trades_maker ON trades (maker)",
            "idx_trades_traded_at": "CREATE INDEX idx_trades_traded_at ON trades (traded_at DESC)",
            "idx_trades_block_number": "CREATE INDEX idx_trades_block_number ON trades (block_number)"
        }
        
        for idx_name, idx_sql in indexes.items():
            exists = await conn.fetchval("SELECT 1 FROM pg_indexes WHERE indexname = $1", idx_name)
            if not exists:
                await conn.execute(idx_sql)

        # --- indexer_state table ---
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS indexer_state (
                key   TEXT PRIMARY KEY,
                value TEXT
            )
        """)

        # --- Migration: add anomaly_score column if missing ---
        await conn.execute("""
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_name = 'wallets' AND column_name = 'anomaly_score'
                ) THEN
                    ALTER TABLE wallets ADD COLUMN anomaly_score FLOAT;
                END IF;
            END $$;
        """)

        # --- Migration: add global_score column if missing ---
        await conn.execute("""
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_name = 'wallets' AND column_name = 'global_score'
                ) THEN
                    ALTER TABLE wallets ADD COLUMN global_score FLOAT;
                END IF;
            END $$;
        """)

    print("Tables created (or already exist) -- async pool ready.")
