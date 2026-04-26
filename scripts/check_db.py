import asyncio
from db.pool import get_pool

async def check():
    pool = await get_pool()
    
    # Check what's in the DB
    r = await pool.fetchval("SELECT COUNT(*) FROM wallets WHERE insider_score >= 0.65")
    print(f"Wallets with insider_score >= 0.65: {r}")
    
    r2 = await pool.fetchval("SELECT COUNT(*) FROM wallets WHERE insider_score IS NOT NULL")
    print(f"Wallets with insider_score NOT NULL: {r2}")
    
    r3 = await pool.fetchval("SELECT COUNT(*) FROM wallets WHERE global_score IS NOT NULL")
    print(f"Wallets with global_score NOT NULL: {r3}")
    
    # Check the exact query used by the API
    from config import INSIDER_THRESHOLD
    print(f"INSIDER_THRESHOLD: {INSIDER_THRESHOLD}")
    
    r4 = await pool.fetchval(
        "SELECT COUNT(*) FROM wallets WHERE (COALESCE(global_score, insider_score, 0) >= $1 OR anomaly_score >= 0.85 OR insider_score >= $1)",
        INSIDER_THRESHOLD
    )
    print(f"API query match count: {r4}")
    
    # Sample some flagged wallets
    rows = await pool.fetch(
        "SELECT address, insider_score, anomaly_score, global_score FROM wallets WHERE insider_score >= $1 LIMIT 3",
        INSIDER_THRESHOLD
    )
    for row in rows:
        print(f"  {row['address'][:16]}... insider={row['insider_score']} anomaly={row['anomaly_score']} global={row['global_score']}")
    
    await pool.close()

asyncio.run(check())
