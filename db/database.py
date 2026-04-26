"""
db/database.py — Connects to PostgreSQL and creates the tables.

We use psycopg2, a simple library that lets us run plain SQL queries
in Python. No complex ORM — just write SQL, run it, done.

DATABASE TABLES:
  wallets — one row per unique trader address
  trades  — one row per trade (OrderFilled event)
  markets — one row per Polymarket market (question)
"""
import psycopg2
import psycopg2.extras
from config import DATABASE_URL


def get_connection():
    """Open and return a connection to the PostgreSQL database."""
    return psycopg2.connect(DATABASE_URL.replace("+asyncpg", ""))


def create_tables():
    """
    Create all tables if they do not already exist.
    Call this once at the start of your program.
    """
    conn = get_connection()
    cursor = conn.cursor()

    # --- wallets table ---
    # Stores every unique wallet address we have seen trading on Polymarket.
    # first_deposit_at = when this wallet first received USDC.e (its "birthday")
    # insider_score    = our algorithm's risk score (0.0 to 1.0)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS wallets (
            address           TEXT PRIMARY KEY,
            first_deposit_tx  TEXT,
            first_deposit_at  TIMESTAMP,
            insider_score     FLOAT,
            flagged           BOOLEAN DEFAULT FALSE,
            scored_at         TIMESTAMP
        )
    """)

    # --- markets table ---
    # Stores Polymarket markets (e.g. "Will X win the election?")
    # end_time is critical: we compare trade times against it to detect
    # suspiciously-timed entries close to resolution.
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS markets (
            condition_id  TEXT PRIMARY KEY,
            question      TEXT,
            end_time      TIMESTAMP
        )
    """)

    # --- trades table ---
    # One row per OrderFilled event.
    # tx_hash + log_index together are unique (like a composite primary key).
    # usdc_amount is already divided by 1e6 (USDC has 6 decimal places).
    cursor.execute("""
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

    # --- Migration: Change log_index to TEXT if it's currently INTEGER ---
    # This handles the case where the user already created the table with the old schema.
    cursor.execute("""
        DO $$ 
        BEGIN 
            IF (SELECT data_type FROM information_schema.columns 
                WHERE table_name = 'trades' AND column_name = 'log_index') = 'integer' THEN
                ALTER TABLE trades ALTER COLUMN log_index TYPE TEXT;
            END IF;
        END $$;
    """)

    # --- indexer_state table ---
    # Stores a single value: the last block number we checked for trades.
    # Next time the poller runs, it picks up from this block onwards.
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS indexer_state (
            key   TEXT PRIMARY KEY,
            value TEXT
        )
    """)

    conn.commit()
    cursor.close()
    conn.close()
    print("Tables created (or already exist).")
