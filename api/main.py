"""
api/main.py — Self-starting API server + dashboard for the Insider Detector.

On startup:
  1. Creates database tables
  2. Runs initial backfill + enrichment + scoring in background
  3. Starts the live polling scheduler

Serves:
  - REST API at /api/*
  - WebSocket alerts at /ws/alerts
  - Dashboard UI at /

Run with: uvicorn api.main:app --host 0.0.0.0 --port 8000
"""
import asyncio
import traceback
import json
import time
import aiohttp
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query, WebSocket, WebSocketDisconnect
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse

import config
from db.pool import get_pool, create_tables, close_pool
from db.cache import get_redis, close_redis, cache_get, cache_set, publish_alert
from db.clickhouse import init_clickhouse
from core.broker import broker
from core.tasks import run_full_pipeline, poll_and_rescore

# ── WebSocket broadcast registry ─────────────────────────────────────────────

_ws_clients: set[WebSocket] = set()


async def broadcast_alert(alert: dict):
    """Push an alert to all connected WebSocket clients."""
    global _ws_clients
    message = json.dumps(alert)
    disconnected = set()
    for ws in _ws_clients:
        try:
            await ws.send_text(message)
        except Exception:
            disconnected.add(ws)
    _ws_clients -= disconnected


async def redis_alert_listener():
    """Listen for alerts in Redis and broadcast them to WebSockets."""
    from db.cache import get_redis
    r = await get_redis()
    pubsub = r.pubsub()
    await pubsub.subscribe("alerts")
    
    try:
        async for message in pubsub.listen():
            if message["type"] == "message":
                try:
                    alert = json.loads(message["data"])
                    await broadcast_alert(alert)
                except Exception as e:
                    print(f"Error broadcasting alert: {e}")
    except Exception as e:
        print(f"Redis alert listener error: {e}")
    finally:
        await pubsub.unsubscribe("alerts")


# ── Background pipeline ──────────────────────────────────────────────────────





# ── Application lifecycle ────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    """On startup: create tables, launch background pipeline."""
    try:
        print("Creating tables...")
        await create_tables()
        print("Init ClickHouse...")
        init_clickhouse()
    except Exception as e:
        print("\n" + "="*60)
        print("CRITICAL: Could not connect to PostgreSQL.")
        print(f"Error: {e}")
        print("Please ensure Postgres is running (e.g., docker-compose up -d)")
        print("="*60 + "\n")
        raise e

    print("Init Redis...")
    await get_redis()
    
    print("Startup broker...")
    await broker.startup()
    
    print("Start Redis listener...")
    asyncio.create_task(redis_alert_listener())
    
    print("Publish system alert...")
    await publish_alert("system", "API Engine started. Initializing surveillance pipeline...")
    
    print("Trigger run_full_pipeline.kiq...")
    await run_full_pipeline.kiq(days=7, max_trades=500)
    
    print("Start _live_poll_scheduler...")
    asyncio.create_task(_live_poll_scheduler())
    
    print("Yielding...")
    yield
    await close_pool()
    await close_redis()
    await broker.shutdown()


app = FastAPI(
    title="Polymarket Insider Trading Detector",
    description="Hybrid detection engine: deterministic rules + ML anomaly detection.",
    version="3.0.0",
    lifespan=lifespan,
    docs_url="/api/docs",
    redoc_url=None,
)


# ── Static files (dashboard) ─────────────────────────────────────────────────

STATIC_DIR = Path(__file__).parent.parent / "static"
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


@app.get("/", include_in_schema=False)
async def serve_dashboard():
    return FileResponse(str(STATIC_DIR / "index.html"))


# ── WebSocket ─────────────────────────────────────────────────────────────────

@app.websocket("/ws/alerts")
async def websocket_alerts(ws: WebSocket):
    await ws.accept()
    _ws_clients.add(ws)
    try:
        while True:
            # Keep connection alive; client can also send messages
            await ws.receive_text()
    except WebSocketDisconnect:
        _ws_clients.discard(ws)


# ── API: Alerts History ──────────────────────────────────────────────────────

@app.get("/api/alerts/history")
async def get_alerts_history(limit: int = Query(default=100, le=500)):
    from db.cache import get_redis
    r = await get_redis()
    items = await r.lrange("alert_history", 0, limit - 1)
    return [json.loads(i) for i in items]


# ── Live Polling Scheduler ────────────────────────────────────────────────────

async def _live_poll_scheduler():
    """Periodically poll for new on-chain trades after backfill is done."""
    # Wait a bit for the initial pipeline to finish
    await asyncio.sleep(10)
    
    from db.cache import is_paused
    
    while True:
        try:
            if not await is_paused():
                try:
                    result = await poll_and_rescore.kiq()
                except Exception as e:
                    print(f"Live poll task dispatch error: {e}")
        except Exception as e:
            print(f"Live poll scheduler error: {e}")
            traceback.print_exc()
        
        await asyncio.sleep(config.POLL_INTERVAL_SEC)


# ── API: System Stats ────────────────────────────────────────────────────────

@app.get("/api/stats")
async def get_stats():
    # Try cache first
    cached = await cache_get("system_stats")
    if cached:
        return cached

    pool = await get_pool()
    stats = {}
    stats["total_wallets"] = await pool.fetchval("SELECT COUNT(*) FROM wallets")
    stats["total_trades"] = await pool.fetchval("SELECT COUNT(*) FROM trades")
    stats["flagged_wallets"] = await pool.fetchval(
        "SELECT COUNT(*) FROM wallets WHERE COALESCE(global_score, 0) >= $1", config.INSIDER_THRESHOLD
    )
    stats["enriched_wallets"] = await pool.fetchval(
        "SELECT COUNT(*) FROM wallets WHERE first_deposit_at IS NOT NULL"
    )
    stats["scored_wallets"] = await pool.fetchval(
        "SELECT COUNT(*) FROM wallets WHERE insider_score IS NOT NULL"
    )
    stats["historical_trades"] = await pool.fetchval(
        "SELECT COUNT(*) FROM trades WHERE block_number = 0"
    )
    stats["live_trades"] = await pool.fetchval(
        "SELECT COUNT(*) FROM trades WHERE block_number > 0"
    )

    # Cache for 5 seconds
    await cache_set("system_stats", stats, ttl_seconds=5)
    return stats


# ── API: Flagged Wallets ──────────────────────────────────────────────────────

@app.get("/api/flagged")
async def get_flagged_wallets(
    page: int = Query(default=1, ge=1),
    per_page: int = Query(default=25, ge=1, le=100),
    sort_by: str = Query(default="global_score", regex="^(global_score|insider_score|anomaly_score)$"),
    sort_dir: str = Query(
        default="desc",
        regex="^(asc|desc)$"
    ),
    search: str = Query(default=None, description="Filter by wallet address prefix"),
):
    pool = await get_pool()
    offset = (page - 1) * per_page

    # Base conditions - Show if Global Score is high OR if either component is very high
    # Use COALESCE to handle NULL global_score (wallets scored before column existed)
    conditions = ["(COALESCE(global_score, 0) >= $1)"]
    params = [config.INSIDER_THRESHOLD]

    if search:
        conditions.append(f"address ILIKE ${len(params) + 1}")
        params.append(f"%{search}%")

    where = " AND ".join(conditions)

    # Count total
    total = await pool.fetchval(
        f"SELECT COUNT(*) FROM wallets WHERE {where}", *params
    )

    # Fetch page
    rows = await pool.fetch(f"""
        SELECT address, insider_score, anomaly_score, global_score, flagged,
               first_deposit_at, scored_at
        FROM wallets
        WHERE {where}
        ORDER BY COALESCE({sort_by}, 0) {sort_dir.upper()}
        LIMIT ${len(params) + 1} OFFSET ${len(params) + 2}
    """, *params, per_page, offset)

    return {
        "total": total,
        "page": page,
        "per_page": per_page,
        "pages": (total + per_page - 1) // per_page if total else 0,
        "wallets": [
            {
                "address": r["address"],
                "insider_score": r["insider_score"],
                "anomaly_score": r["anomaly_score"],
                "global_score": r["global_score"],
                "flagged": r["flagged"],
                "first_deposit_at": r["first_deposit_at"].isoformat() if r["first_deposit_at"] else None,
                "scored_at": r["scored_at"].isoformat() if r["scored_at"] else None,
            }
            for r in rows
        ],
    }


# ── API: Wallet Detail ───────────────────────────────────────────────────────

@app.get("/api/wallets/{address}")
async def get_wallet_detail(address: str):
    pool = await get_pool()
    addr = address.lower()

    # Fetch wallet
    wallet = await pool.fetchrow(
        "SELECT * FROM wallets WHERE address = $1", addr
    )
    if not wallet:
        raise HTTPException(status_code=404, detail="Wallet not found.")

    # Fetch trades
    trades = await pool.fetch("""
        SELECT tx_hash, condition_id, usdc_amount, price, traded_at
        FROM trades WHERE maker = $1
        ORDER BY traded_at DESC
        LIMIT 200
    """, addr)

    # Compute score breakdown on-the-fly
    from detection.scorer import compute_score
    trade_dicts = [
        {"condition_id": t["condition_id"], "usdc_amount": t["usdc_amount"], "traded_at": t["traded_at"]}
        for t in trades
    ]

    # Aggregate stats manually for single wallet
    total_usdc = sum(t["usdc_amount"] for t in trade_dicts)
    max_trade = max(t["usdc_amount"] for t in trade_dicts) if trade_dicts else 0
    unique_markets = len(set(t["condition_id"] for t in trade_dicts))
    first_trade_at = min(t["traded_at"] for t in trade_dicts) if trade_dicts else None
    
    stats = {
        "trade_count": len(trade_dicts),
        "unique_markets": unique_markets,
        "max_trade_usdc": max_trade,
        "first_trade_at": first_trade_at,
        "concentration": max_trade / total_usdc if total_usdc > 0 else 0,
        "max_timing_score": 0.05
    }

    wallet_dict = {"address": wallet["address"], "first_deposit_at": wallet["first_deposit_at"]}
    score_result = compute_score(wallet_dict, stats)

    return {
        "address": wallet["address"],
        "insider_score": wallet["insider_score"],
        "anomaly_score": wallet["anomaly_score"],
        "global_score": wallet["global_score"],
        "flagged": wallet["flagged"],
        "first_deposit_at": wallet["first_deposit_at"].isoformat() if wallet["first_deposit_at"] else None,
        "scored_at": wallet["scored_at"].isoformat() if wallet["scored_at"] else None,
        "verdict": score_result.get("verdict", ""),
        "descriptions": score_result.get("descriptions", {}),
        "breakdown": score_result.get("breakdown", {}),
        "trade_count": len(trades),
        "unique_markets": score_result.get("unique_markets", 0),
        "max_trade_usdc": score_result.get("max_trade_usdc", 0),
        "trades": [
            {
                "tx_hash": t["tx_hash"],
                "condition_id": t["condition_id"],
                "usdc_amount": t["usdc_amount"],
                "price": round(t["price"], 4) if t["price"] else None,
                "traded_at": t["traded_at"].isoformat() if t["traded_at"] else None,
            }
            for t in trades
        ],
    }


# ── API: Recent Trades (Historical vs Live) ──────────────────────────────────

def _format_trade(r):
    return {
        "tx_hash": r["tx_hash"],
        "maker": r["maker"],
        "taker": r["taker"],
        "usdc_amount": r["usdc_amount"],
        "price": round(r["price"], 4) if r["price"] else None,
        "traded_at": r["traded_at"].isoformat() if r["traded_at"] else None,
    }

@app.get("/api/trades/recent")
async def get_recent_trades(limit: int = Query(default=50, le=200)):
    pool = await get_pool()
    rows = await pool.fetch("""
        SELECT tx_hash, maker, taker, usdc_amount, price, traded_at
        FROM trades
        ORDER BY traded_at DESC
        LIMIT $1
    """, limit)
    return [_format_trade(r) for r in rows]


@app.get("/api/trades/historical")
async def get_historical_trades(limit: int = Query(default=50, le=200)):
    """Historical trades (fetched from The Graph subgraph, block_number=0)."""
    pool = await get_pool()
    rows = await pool.fetch("""
        SELECT tx_hash, maker, taker, usdc_amount, price, traded_at
        FROM trades
        WHERE block_number = 0
        ORDER BY traded_at DESC
        LIMIT $1
    """, limit)
    return [_format_trade(r) for r in rows]


@app.get("/api/trades/live")
async def get_live_trades(limit: int = Query(default=50, le=200)):
    """Live trades (fetched from on-chain RPC, block_number > 0)."""
    pool = await get_pool()
    rows = await pool.fetch("""
        SELECT tx_hash, maker, taker, usdc_amount, price, traded_at
        FROM trades
        WHERE block_number > 0
        ORDER BY traded_at DESC
        LIMIT $1
    """, limit)
    return [_format_trade(r) for r in rows]


# ── API: Transaction Detail ──────────────────────────────────────────────────

@app.get("/api/trades/{tx_hash}")
async def get_trade_detail(tx_hash: str):
    pool = await get_pool()
    row = await pool.fetchrow("""
        SELECT t.*, m.end_time as market_resolution
        FROM trades t
        LEFT JOIN markets m ON t.condition_id = m.condition_id
        WHERE t.tx_hash = $1
        LIMIT 1
    """, tx_hash)
    
    if not row:
        raise HTTPException(status_code=404, detail="Transaction not found.")
        
    return {
        "tx_hash": row["tx_hash"],
        "maker": row["maker"],
        "taker": row["taker"],
        "usdc_amount": row["usdc_amount"],
        "price": row["price"],
        "condition_id": row["condition_id"],
        "block_number": row["block_number"],
        "traded_at": row["traded_at"].isoformat() if row["traded_at"] else None,
        "market_resolution": row["market_resolution"].isoformat() if row.get("market_resolution") else None,
    }


# ── API: Health & Systems Check ───────────────────────────────────────────────

@app.get("/api/health")
async def health():
    pool = await get_pool()
    return {
        "status": "running",
        "pool_size": pool.get_size(),
        "ws_clients": len(_ws_clients),
    }

@app.get("/api/admin/health-check")
async def systems_check():
    results = {}
    
    # 1. Postgres
    try:
        pool = await get_pool()
        await pool.execute("SELECT 1")
        results["postgres"] = {"status": "ok", "message": "Connected"}
    except Exception as e:
        results["postgres"] = {"status": "error", "message": str(e)}

    # 2. Redis
    try:
        r = await get_redis()
        await r.ping()
        results["redis"] = {"status": "ok", "message": "Connected"}
    except Exception as e:
        results["redis"] = {"status": "error", "message": str(e)}

    # 3. ClickHouse
    try:
        from db.clickhouse import get_clickhouse
        ch = get_clickhouse()
        ch.command("SELECT 1")
        results["clickhouse"] = {"status": "ok", "message": "Connected"}
    except Exception as e:
        results["clickhouse"] = {"status": "error", "message": str(e)}

    # 4. RabbitMQ (TaskIQ Broker)
    try:
        if broker.is_worker_process: # Simplified check
             results["rabbitmq"] = {"status": "ok", "message": "Connected"}
        else:
             # Try a manual ping or just check if initialized
             results["rabbitmq"] = {"status": "ok", "message": "Broker Active"}
    except Exception as e:
        results["rabbitmq"] = {"status": "error", "message": str(e)}

    # 5. External: Alchemy RPC
    try:
        async with aiohttp.ClientSession() as session:
            payload = {"jsonrpc": "2.0", "method": "eth_blockNumber", "params": [], "id": 1}
            async with session.post(config.ALCHEMY_RPC_URL, json=payload) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    results["alchemy"] = {"status": "ok", "message": f"Block: {int(data['result'], 16)}"}
                else:
                    results["alchemy"] = {"status": "error", "message": f"HTTP {resp.status}"}
    except Exception as e:
        results["alchemy"] = {"status": "error", "message": str(e)}

    # 6. External: The Graph
    try:
        async with aiohttp.ClientSession() as session:
            query = {"query": "{ _meta { block { number } } }"}
            async with session.post(config.SUBGRAPH_URL, json=query) as resp:
                if resp.status == 200:
                    results["the_graph"] = {"status": "ok", "message": "Subgraph Responsive"}
                else:
                    results["the_graph"] = {"status": "error", "message": f"HTTP {resp.status}"}
    except Exception as e:
        results["the_graph"] = {"status": "error", "message": str(e)}

    return results


@app.get("/api/admin/status")
async def get_system_status():
    from db.cache import is_paused
    return {"paused": await is_paused()}

@app.post("/api/admin/toggle-pause")
async def toggle_pause():
    from db.cache import is_paused, set_paused
    current = await is_paused()
    await set_paused(not current)
    return {"paused": not current}


# ── API: Admin Actions ───────────────────────────────────────────────────────

@app.post("/api/admin/backfill")
async def trigger_backfill(days: int = 7, max_trades: int = 500):
    await run_full_pipeline.kiq(days=days, max_trades=max_trades)
    return {"status": "Backfill task queued."}


@app.post("/api/admin/reset")
async def reset_system():
    """Nuclear option: Wipes all databases and caches."""
    try:
        # 1. Clear Postgres
        pool = await get_pool()
        async with pool.acquire() as conn:
            await conn.execute("TRUNCATE TABLE trades, wallets, markets, indexer_state RESTART IDENTITY")
            # Re-seed last_trade_block to something safe if needed, 
            # but usually it's fine to start from 0 or .env value.
        
        # 2. Clear ClickHouse
        from db.clickhouse import get_clickhouse
        ch = get_clickhouse()
        ch.command("TRUNCATE TABLE trades")
        
        # 3. Flush Redis
        from db.cache import get_redis
        redis = await get_redis()
        await redis.flushall()

        await broadcast_alert({
            "type": "system_reset",
            "message": "SYSTEM RESET: All data wiped. Starting fresh indexing..."
        })
        
        return {"status": "success", "message": "System wiped successfully."}
    except Exception as e:
        print(f"Reset error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/admin/sync")
async def trigger_sync():
    """Manual sync trigger via UI."""
    from db.cache import get_redis
    redis = await get_redis()
    await redis.flushall() # Clear cache so fresh data shows immediately
    
    from core.tasks import run_full_pipeline
    await run_full_pipeline.kiq()
    return {"status": "queued"}


@app.post("/api/admin/score-all")
async def trigger_score_all():
    # We can create a specific task for this or reuse full pipeline
    await run_full_pipeline.kiq(days=0, max_trades=0) 
    return {"status": "Scoring task queued."}


@app.post("/api/admin/rescore-all")
async def rescore_all_existing():
    """
    Re-score all existing wallets in the database without fetching any new data.
    This runs the deterministic scorer + ML anomaly detection on whatever is
    already stored, then broadcasts pipeline_complete so the UI refreshes.
    """
    async def _do_rescore():
        try:
            await publish_alert("system", "Rescoring all existing wallets (rules + ML)...")
            from detection.anomaly import run_anomaly_detection
            from detection.scorer import score_all_wallets
            await run_anomaly_detection()
            await score_all_wallets()
            await publish_alert("pipeline_complete", "Rescore complete \u2014 flagged wallet table and chart updated.")
        except Exception as exc:
            print(f"rescore-all error: {exc}")
            await publish_alert("warning", f"Rescore failed: {exc}")

    asyncio.create_task(_do_rescore())
    return {"status": "rescore_queued", "message": "Rescoring all wallets in background."}


@app.post("/api/admin/enrich")
async def trigger_enrich():
    from indexers.deposits import enrich_all_wallets
    asyncio.create_task(enrich_all_wallets())
    return {"status": "Enrichment started."}


@app.post("/api/admin/anomaly")
async def trigger_anomaly():
    from detection.anomaly import run_anomaly_detection
    asyncio.create_task(run_anomaly_detection())
    return {"status": "Anomaly detection started."}
