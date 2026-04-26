"""
core/tasks.py — Definition of background tasks for the worker.
"""
import time
import asyncio
from core.broker import broker
from indexers.trades import backfill, poll_live
from indexers.deposits import enrich_all_wallets
from detection.scorer import score_all_wallets
from detection.anomaly import run_anomaly_detection
from db.cache import publish_alert, is_paused

@broker.task
async def run_full_pipeline(days: int = 7, max_trades: int = 50_000):
    """
    Orchestrates the entire detection pipeline.
    This can be called from the API or triggered on a schedule.
    """
    await publish_alert("system", f"Global pipeline triggered (lookback: {days} days, limit: {max_trades:,} trades)")
    from_ts = int(time.time()) - (86400 * days)
    
    # Step 1: Backfill
    await backfill(from_ts, max_trades=max_trades)
    
    # Step 2: Enrich
    await enrich_all_wallets()
    
    # Step 3: Anomaly (ML)
    await run_anomaly_detection()
    
    # Step 4: Final Unified Scoring
    await score_all_wallets()
    
    return "Pipeline Complete"

@broker.task
async def poll_and_rescore():
    """Live polling task with full reactive pipeline."""
    count = await poll_live()
    if count > 0:
        await publish_alert("system", f"Live activity detected ({count} trades). Running reactive analysis...")
        # Rapid reactive pipeline
        await enrich_all_wallets()
        await run_anomaly_detection()
        await score_all_wallets()
        await publish_alert("system", "✓ Reactive analysis complete. UI updated.")
    return count
