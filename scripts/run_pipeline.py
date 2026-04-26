"""
run_pipeline.py — Execute the full data pipeline in one command.

Usage:
  python run_pipeline.py                  # Full pipeline (backfill + enrich + score)
  python run_pipeline.py --backfill-only  # Just download trades
  python run_pipeline.py --score-only     # Just re-score existing data
  python run_pipeline.py --fresh          # Wipe DB and start from scratch
"""
import asyncio
import argparse
import time
import os
import sys
# Ensure the parent directory is in sys.path so we can import from 'detection', 'db', etc.
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import config

from db.pool import get_pool, create_tables, close_pool
from indexers.trades import backfill
from indexers.deposits import enrich_all_wallets
from detection.scorer import score_all_wallets
from detection.anomaly import run_anomaly_detection
from db.clickhouse import init_clickhouse


async def fresh_start(pool):
    """Wipe all tables and start clean."""
    async with pool.acquire() as conn:
        await conn.execute("TRUNCATE trades, wallets, indexer_state")
    print("Database wiped.")


async def main():
    parser = argparse.ArgumentParser(description="Run the insider detection pipeline")
    parser.add_argument("--fresh", action="store_true", help="Wipe DB before backfill")
    parser.add_argument("--backfill-only", action="store_true", help="Only download trades")
    parser.add_argument("--score-only", action="store_true", help="Only re-score wallets")
    parser.add_argument("--max-trades", type=int, default=100_000, help="Max trades to download (0 for unlimited)")
    parser.add_argument("--days", type=int, default=30, help="How many days of history")
    parser.add_argument("--live", action="store_true", help="Keep running live after initial pipeline")
    args = parser.parse_args()

    await create_tables()
    init_clickhouse()
    pool = await get_pool()

    t0 = time.time()

    if args.fresh:
        await fresh_start(pool)

    if args.score_only:
        await run_anomaly_detection()
        flagged = await score_all_wallets()
    elif args.backfill_only:
        from_ts = int(time.time()) - (86400 * args.days)
        await backfill(from_ts, args.max_trades)
    else:
        # Full pipeline (initial)
        from_ts = int(time.time()) - (86400 * args.days)

        print("=" * 60)
        print("STEP 1/4: Downloading trades...")
        print("=" * 60)
        await backfill(from_ts, args.max_trades)

        print()
        print("=" * 60)
        print("STEP 2/4: Enriching wallets...")
        print("=" * 60)
        await enrich_all_wallets()

        print()
        print("=" * 60)
        print("STEP 3/4: Anomaly detection (ML)...")
        print("=" * 60)
        await run_anomaly_detection()

        print()
        print("=" * 60)
        print("STEP 4/4: Scoring wallets (deterministic)...")
        print("=" * 60)
        flagged = await score_all_wallets()

    if args.live:
        print()
        print("=" * 60)
        print("LIVE MODE ENABLED: Polling for new trades...")
        print("=" * 60)
        
        from indexers.trades import poll_live
        
        from db.cache import is_paused
        
        while True:
            try:
                if await is_paused():
                    await asyncio.sleep(5)
                    continue
                    
                # 1. Poll for new trades via Alchemy RPC
                new_count = await poll_live()
                
                if new_count > 0:
                    print(f"[{time.strftime('%H:%M:%S')}] Found {new_count} new trades. Processing...")
                    
                    # 2. Enrich any new wallets found
                    await enrich_all_wallets()
                    
                    # 3. Re-run anomaly detection
                    await run_anomaly_detection()
                    
                    # 4. Update scores
                    await score_all_wallets()
                    print(f"[{time.strftime('%H:%M:%S')}] Update complete.")
                else:
                    # Just heartbeat
                    pass
                    
            except Exception as e:
                print(f"Error in live loop: {e}")
            
            await asyncio.sleep(config.POLL_INTERVAL_SEC)

    elapsed = time.time() - t0
    print()
    print(f"Pipeline finished in {elapsed:.1f}s")

    await close_pool()


if __name__ == "__main__":
    asyncio.run(main())
