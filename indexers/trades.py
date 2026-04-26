"""
indexers/trades.py — Downloads trade data from Polymarket.
"""
import asyncio
import time
from datetime import datetime, timezone

import aiohttp
from web3 import Web3
from hexbytes import HexBytes
try:
    from web3.middleware import ExtraDataToPOAMiddleware as geth_poa_middleware
except ImportError:
    from web3.middleware import geth_poa_middleware

import config
from db.pool import get_pool
from db.clickhouse import insert_trades_ch
from db.cache import publish_alert, is_paused

# The address of Polymarket's trading smart contract on Polygon
CTF_EXCHANGE_ADDRESS = "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E"
USDC_DECIMALS = 6

# Standard OrderFilled event signature
ORDER_FILLED_SIG = "OrderFilled(bytes32,address,address,uint256,uint256,uint256,uint256,uint256)"

# The ABI for manual decoding
ORDER_FILLED_ABI = {
    "name": "OrderFilled",
    "type": "event",
    "anonymous": False,
    "inputs": [
        {"name": "orderHash",           "type": "bytes32", "indexed": True},
        {"name": "maker",               "type": "address", "indexed": True},
        {"name": "taker",               "type": "address", "indexed": True},
        {"name": "makerAssetId",        "type": "uint256", "indexed": False},
        {"name": "takerAssetId",        "type": "uint256", "indexed": False},
        {"name": "makerAmountFilled",   "type": "uint256", "indexed": False},
        {"name": "takerAmountFilled",   "type": "uint256", "indexed": False},
        {"name": "fee",                 "type": "uint256", "indexed": False},
    ],
}

def _parse_amounts(maker_asset_id, maker_amount, taker_amount, taker_asset_id):
    if maker_asset_id == 0:
        usdc_raw  = maker_amount
        share_raw = taker_amount
        asset_id  = taker_asset_id
    else:
        usdc_raw  = taker_amount
        share_raw = maker_amount
        asset_id  = maker_asset_id

    usdc_amount = usdc_raw / (10 ** USDC_DECIMALS)
    price = usdc_raw / share_raw if share_raw > 0 else 0
    return usdc_amount, price, str(asset_id)

async def _batch_insert(pool, rows, wallets):
    if not rows: return
    async with pool.acquire() as conn:
        wallet_rows = [(w,) for w in wallets]
        await conn.executemany("INSERT INTO wallets (address) VALUES ($1) ON CONFLICT (address) DO NOTHING", wallet_rows)
        await conn.executemany("""
            INSERT INTO trades (tx_hash, log_index, maker, taker, condition_id, asset_id, 
                               usdc_amount, price, block_number, traded_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            ON CONFLICT (tx_hash, log_index) DO NOTHING
        """, rows)
    try:
        ch_rows = [(r[0], r[2], r[3], r[4], r[6], r[7], r[8], r[9]) for r in rows]
        insert_trades_ch(ch_rows)
    except Exception as e:
        print(f"ClickHouse insert error: {e}")

async def _get_last_block(pool, w3) -> int:
    row = await pool.fetchval("SELECT value FROM indexer_state WHERE key = 'last_trade_block'")
    if row: return int(row)
    return w3.eth.block_number - 100

async def _save_last_block(pool, block_number: int):
    await pool.execute("INSERT INTO indexer_state (key, value) VALUES ('last_trade_block', $1) ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value", str(block_number))

async def backfill(from_ts: int, max_trades: int = 5000):
    """
    Historical download using The Graph Activity Subgraph.
    """
    pool = await get_pool()
    last_ts = await pool.fetchval("SELECT MAX(traded_at) FROM trades WHERE block_number = 0")
    current_min_ts = from_ts
    if last_ts:
        current_min_ts = int(last_ts.replace(tzinfo=timezone.utc).timestamp())
        print(f"Resuming backfill from {last_ts}...")

    total_collected = 0
    CHUNK_SIZE = 1000
    
    async with aiohttp.ClientSession() as session:
        while True:
            query = """
            {
              enrichedOrderFilleds(
                first: %d,
                orderBy: timestamp,
                orderDirection: asc,
                where: { timestamp_gt: "%d" }
              ) {
                id
                maker { id }
                taker { id }
                market { id }
                side
                size
                price
                timestamp
              }
            }
            """ % (CHUNK_SIZE, current_min_ts)

            async with session.post(config.SUBGRAPH_URL, json={'query': query}) as resp:
                if resp.status != 200: break
                try:
                    data = await resp.json()
                except: break
                    
                if not data or not isinstance(data, dict) or "data" not in data or data["data"] is None:
                    break
                    
                trades = data["data"].get("enrichedOrderFilleds", [])
                if not trades: break
                
                rows = []
                wallets = set()
                for t in trades:
                    try:
                        price = float(t["price"])
                        # size has 6 decimals just like USDC usually
                        size_raw = int(t["size"])
                        usdc_amount = (size_raw / (10 ** USDC_DECIMALS)) * price
                        
                        if usdc_amount < 50: continue
                        
                        asset_id = "0" # We don't have the exact asset ID easily accessible here without more querying, but 'market.id' could serve as a proxy if needed. Actually we'll just put '0' for subgraph trades to keep schema happy
                        
                        maker_address = t["maker"]["id"].lower()
                        taker_address = t["taker"]["id"].lower()
                        
                        traded_at = datetime.fromtimestamp(int(t["timestamp"]), tz=timezone.utc).replace(tzinfo=None)
                        rows.append((
                            t["id"].split('_')[0], t["id"].split('_')[1] if '_' in t["id"] else "0",
                            maker_address, taker_address, asset_id, asset_id,
                            usdc_amount, price, 0, traded_at
                        ))
                        wallets.add(maker_address)
                    except Exception as e:
                        print(f"Error parsing trade {t.get('id')}: {e}")
                        continue
                
                if rows:
                    await _batch_insert(pool, rows, wallets)
                    total_collected += len(rows)
                    current_min_ts = int(trades[-1]["timestamp"])
                    print(f"  {total_collected:,} historical trades collected...")
                    await publish_alert("historical_batch", f"Batch: {len(rows)} historical trades synced.")

                if max_trades > 0 and total_collected >= max_trades: break
                await asyncio.sleep(0.1)

    print(f"Backfill complete: {total_collected:,} trades total.")
    return total_collected

async def poll_live():
    """
    Fetch new OrderFilled events via Web3 with PoA middleware and manual log processing.
    """
    if await is_paused(): return 0
    pool = await get_pool()
    w3 = Web3(Web3.HTTPProvider(config.ALCHEMY_RPC_URL))
    w3.middleware_onion.inject(geth_poa_middleware, layer=0)
    
    try:
        latest_block = w3.eth.block_number
        from_block = await _get_last_block(pool, w3)
    except: return 0

    if latest_block <= from_block: return 0
    if latest_block - from_block > 2000: from_block = latest_block - 1000

    print(f"Polling blocks {from_block + 1} to {latest_block}...")
    
    CHUNK_SIZE = 10 
    all_rows = []
    all_wallets = set()
    block_cache = {}

    contract = w3.eth.contract(address=Web3.to_checksum_address(CTF_EXCHANGE_ADDRESS), abi=[ORDER_FILLED_ABI])
    # Ensure topic0 has 0x prefix
    t0_hex = w3.keccak(text=ORDER_FILLED_SIG).hex()
    topic0 = t0_hex if t0_hex.startswith("0x") else "0x" + t0_hex

    for start in range(from_block + 1, latest_block + 1, CHUNK_SIZE):
        if await is_paused(): break
        end = min(start + CHUNK_SIZE - 1, latest_block)
        try:
            logs = w3.eth.get_logs({
                "address": Web3.to_checksum_address(CTF_EXCHANGE_ADDRESS),
                "fromBlock": start,
                "toBlock": end,
                "topics": [topic0]
            })
            
            if logs:
                print(f"  [{start}-{end}] Found {len(logs)} logs.")
                
            for log in logs:
                try:
                    event_data = contract.events.OrderFilled().process_log(log)
                    args = event_data["args"]
                    bn = log["blockNumber"]
                    
                    if bn not in block_cache:
                        block_cache[bn] = w3.eth.get_block(bn)["timestamp"]
                    
                    traded_at = datetime.fromtimestamp(block_cache[bn], tz=timezone.utc).replace(tzinfo=None)
                    usdc_amount, price, asset_id = _parse_amounts(
                        args["makerAssetId"], args["makerAmountFilled"],
                        args["takerAmountFilled"], args["takerAssetId"]
                    )
                    
                    maker = args["maker"].lower()
                    all_rows.append((
                        log["transactionHash"].hex(), str(log["logIndex"]),
                        maker, args["taker"].lower(), "0", asset_id,
                        usdc_amount, price, bn, traded_at
                    ))
                    all_wallets.add(maker)
                except Exception as e:
                    print(f"  Error processing log {log['transactionHash'].hex() if isinstance(log.get('transactionHash'), bytes) else log.get('transactionHash', 'unknown')}: {e}")
                    continue
        except Exception as e:
            print(f"  RPC Error in range [{start}-{end}]: {e}")
            continue

    if all_rows:
        await _batch_insert(pool, all_rows, all_wallets)
        await _save_last_block(pool, latest_block)
        print(f"Live poll: {len(all_rows)} trades saved.")
        await publish_alert("live_trade", f"✓ Live sync: {len(all_rows)} new trades indexed.")
    else:
        await _save_last_block(pool, latest_block)
    
    return len(all_rows)
