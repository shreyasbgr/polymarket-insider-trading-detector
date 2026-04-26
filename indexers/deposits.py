"""
indexers/deposits.py — Finds the first USDC.e deposit for each wallet.

Uses aiohttp with a 20-worker semaphore pool for concurrent Alchemy API calls.
Batch-updates wallet records via asyncpg executemany.
"""
import asyncio
import time
from datetime import datetime

import aiohttp

import config
from db.pool import get_pool
from db.cache import publish_alert

# The smart contract address for USDC.e on Polygon
USDC_E_ADDRESS = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"


async def _fetch_first_deposit(session: aiohttp.ClientSession,
                                wallet_address: str,
                                semaphore: asyncio.Semaphore) -> tuple[str, dict | None]:
    """
    Ask Alchemy for the first USDC.e transfer into this wallet.
    Returns (wallet_address, transfer_record_or_None).
    """
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "alchemy_getAssetTransfers",
        "params": [{
            "toAddress": wallet_address,
            "contractAddresses": [USDC_E_ADDRESS],
            "category": ["erc20"],
            "order": "asc",
            "maxCount": "0x1",
            "withMetadata": True,
        }]
    }

    async with semaphore:
        try:
            async with session.post(
                config.ALCHEMY_RPC_URL,
                json=payload,
                timeout=aiohttp.ClientTimeout(total=30)
            ) as resp:
                data = await resp.json()
                result = data.get("result", {})
                transfers = result.get("transfers", [])
                return (wallet_address, transfers[0] if transfers else None)
        except Exception as e:
            # On error, skip this wallet (will be retried on next run)
            return (wallet_address, None)


# Global lock to prevent multiple enrichment tasks from running concurrently
# and producing confusing progress logs.
_enrich_lock = asyncio.Lock()


async def enrich_all_wallets(progress_callback=None):
    """
    Find all wallets missing deposit info and enrich them concurrently.

    Strategy:
      1. Load all un-enriched wallet addresses from the DB.
      2. Fire up to ALCHEMY_CONCURRENCY (20) requests at a time.
      3. Collect results in batches of 100, then write to DB.
    """
    if _enrich_lock.locked():
        print("Enrichment already in progress. Skipping redundant trigger.")
        return

    async with _enrich_lock:
        pool = await get_pool()
        await publish_alert("info", "Starting wallet age enrichment...")
        if progress_callback:
            await progress_callback("Starting wallet age enrichment...")
        concurrency = config.ALCHEMY_CONCURRENCY
        semaphore = asyncio.Semaphore(concurrency)

        # Load wallets that need enrichment
        rows = await pool.fetch(
            "SELECT address FROM wallets WHERE first_deposit_at IS NULL"
        )
        addresses = [r["address"] for r in rows]

        if not addresses:
            print("All wallets already enriched.")
            return

        print(f"Enriching {len(addresses):,} wallets ({concurrency} concurrent workers)...")
        await publish_alert("info", f"Enriching {len(addresses):,} wallets for age analysis...")
        t0 = time.time()
        enriched = 0
        batch_size = 300  # how many results to collect before writing to DB

        async with aiohttp.ClientSession() as session:
            from db.cache import is_paused
            # Process in batches of batch_size
            for batch_start in range(0, len(addresses), batch_size):
                if await is_paused():
                    print("Enrichment waiting: System is paused...")
                    await publish_alert("info", "Enrichment waiting: System is paused.")
                    await asyncio.sleep(5)
                    continue
                
                batch_addrs = addresses[batch_start : batch_start + batch_size]

                # Fire all requests in this batch concurrently
                tasks = [
                    _fetch_first_deposit(session, addr, semaphore)
                    for addr in batch_addrs
                ]
                results = await asyncio.gather(*tasks)

                # Collect successful results for bulk DB update
                update_rows = []
                for wallet_address, transfer in results:
                    if transfer is None:
                        continue

                    ts_str = transfer.get("metadata", {}).get("blockTimestamp")
                    if ts_str:
                        deposit_time = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
                        # Strip timezone for naive TIMESTAMP column
                        deposit_time = deposit_time.replace(tzinfo=None)
                    else:
                        continue

                    tx_hash = transfer.get("hash")
                    update_rows.append((tx_hash, deposit_time, wallet_address))

                # Batch update the database
                if update_rows:
                    async with pool.acquire() as conn:
                        await conn.executemany("""
                            UPDATE wallets
                            SET first_deposit_tx = $1, first_deposit_at = $2
                            WHERE address = $3
                        """, update_rows)

                enriched += len(batch_addrs)
                elapsed = time.time() - t0
                rate = enriched / elapsed if elapsed > 0 else 0
                print(f"  {enriched:>7,} / {len(addresses):,} wallets  |  "
                      f"{rate:,.0f} wallets/sec  |  {elapsed:.1f}s elapsed")
                if progress_callback:
                    await progress_callback(f"Enrichment: {enriched:,} / {len(addresses):,} wallets processed...")
                await publish_alert("info", f"Enrichment progress: {enriched:,}/{len(addresses):,} wallets...")

        elapsed = time.time() - t0
        print(f"Enrichment complete: {enriched:,} wallets in {elapsed:.1f}s")
        await publish_alert("info", f"Wallet enrichment complete ({enriched:,} processed).")
