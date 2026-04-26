"""
db/clickhouse.py — ClickHouse connection and schema management for trade data.
"""
import clickhouse_connect
import config

_client = None

def get_clickhouse(database: str = 'polymarket'):
    """Get ClickHouse client with retry logic."""
    global _client
    import time
    
    for i in range(10):
        try:
            return clickhouse_connect.get_client(
                host=config.CLICKHOUSE_HOST,
                username='admin',
                password='poly123',
                database=database,
                connect_timeout=10
            )
        except Exception as e:
            if i == 9:
                raise
            print(f"ClickHouse connection failed (attempt {i+1}/10): {e}. Retrying in 2s...")
            time.sleep(2)

def init_clickhouse():
    """Initialize ClickHouse schema."""
    # 1. Connect to default to create the custom database
    sys_client = get_clickhouse(database='default')
    sys_client.command("CREATE DATABASE IF NOT EXISTS polymarket")
    
    # 2. Connect to the new database to create tables
    client = get_clickhouse(database='polymarket')
    
    # Create trades table (OLAP optimized)
    client.command("""
        CREATE TABLE IF NOT EXISTS trades (
            tx_hash String,
            maker String,
            taker String,
            condition_id String,
            usdc_amount Float64,
            price Float64,
            block_number UInt64,
            traded_at DateTime,
            inserted_at DateTime DEFAULT now()
        ) ENGINE = ReplacingMergeTree()
        ORDER BY (traded_at, tx_hash, maker)
    """)
    print("ClickHouse: 'trades' table initialized.")

def insert_trades_ch(rows: list):
    """Bulk insert trades into ClickHouse."""
    client = get_clickhouse()
    if not rows:
        return
    client.insert('trades', rows, column_names=[
        'tx_hash', 'maker', 'taker', 'condition_id', 
        'usdc_amount', 'price', 'block_number', 'traded_at'
    ])
