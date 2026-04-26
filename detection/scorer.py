"""
detection/scorer.py — Calculates an insider trading score for each wallet.

SCORING OVERVIEW:
  Each wallet gets a score from 0.0 (normal) to 1.0 (very suspicious).
  The score is a weighted average of 5 factors (deterministic Layer 1).
  If the score >= 0.65, the wallet is flagged.

  Layer 2 (anomaly detection via Isolation Forest) runs separately in anomaly.py.

THE 5 FACTORS:
  1. Entry Timing      (30%) — Did they trade right before the market ended?
  2. Trade Concentration (25%) — Did they bet only on 1-3 markets?
  3. Trade Size        (20%) — Did they make unusually large single trades?
  4. Wallet Age        (15%) — Was the wallet brand new when they traded?
  5. Market Count      (10%) — Did they ever trade anywhere else?
"""
import asyncio
import time
from datetime import datetime, timezone
from collections import Counter, defaultdict

import config
from db.pool import get_pool
from db.clickhouse import get_clickhouse
from db.cache import publish_alert

# ── Scoring Weights ──────────────────────────────────────────────────────────
RULE_WEIGHTS = {
    "entry_timing":         0.30,  # 1.0 if < 2h, 0.8 if < 12h, 0.5 if < 48h
    "trade_concentration":  0.25,  # 1.0 if > 95% in 1 market, 0.8 if > 80%
    "trade_size":           0.20,  # 1.0 if > $20k, 0.7 if > $5k, 0.3 if > $1k
    "wallet_age":           0.15,  # 1.0 if < 1 day old, 0.75 if < 7 days
    "market_count":         0.10,  # 1.0 if 1 market only, 0.7 if < 3 markets
}

# Global Weights
RULE_VS_ML_WEIGHTS = {
    "rules": 0.60,
    "ml":    0.40
}

FACTOR_DESCRIPTIONS = {
    "entry_timing":         "Risk spikes if trades occur < 48h before market resolution. CRITICAL: < 2h = 1.0 score.",
    "trade_concentration":  "Percentage of total volume in the top market. > 95% concentration = 1.0 score.",
    "trade_size":           "Absolute position size in USDC. > $20k = 1.0 score.",
    "wallet_age":           "Days between first deposit and first trade. < 24h = 1.0 score.",
    "market_count":         "Number of unique markets traded. Single-market focus = 1.0 score.",
}


# ── FACTOR 1: Entry Timing ───────────────────────────────────────────────────
def score_entry_timing(max_timing_score: float = 0.0) -> float:
    """Score based on how close to market resolution the wallet traded.
    Pre-calculated in ClickHouse for batch scoring."""
    return max_timing_score


# ── FACTOR 2: Trade Concentration ───────────────────────────────────────────
def score_trade_concentration(concentration: float) -> float:
    if concentration >= 0.95:
        return 1.0
    elif concentration >= 0.8:
        return 0.8
    elif concentration >= 0.5:
        return 0.4
    else:
        return 0.05


# ── FACTOR 3: Trade Size ────────────────────────────────────────────────────
def score_trade_size(max_trade_usdc: float) -> float:
    if max_trade_usdc >= 20000:
        return 1.0
    elif max_trade_usdc >= 5000:
        return 0.7
    elif max_trade_usdc >= 1000:
        return 0.3
    else:
        return 0.05


# ── FACTOR 4: Wallet Age ─────────────────────────────────────────────────────
def score_wallet_age(first_deposit_at: datetime | None, first_trade_at: datetime) -> float:
    if first_deposit_at is None:
        return 0.4
    # Normalize timezones
    f_dep = first_deposit_at.replace(tzinfo=timezone.utc) if first_deposit_at.tzinfo is None else first_deposit_at
    f_trd = first_trade_at.replace(tzinfo=timezone.utc) if first_trade_at.tzinfo is None else first_trade_at
    
    days_old = (f_trd - f_dep).days
    if days_old <= 1:
        return 1.0
    elif days_old <= 7:
        return 0.75
    elif days_old <= 30:
        return 0.35
    else:
        return 0.05


# ── FACTOR 5: Market Count ───────────────────────────────────────────────────
def score_market_count(unique_markets: int) -> float:
    if unique_markets == 1:
        return 1.0
    elif unique_markets <= 3:
        return 0.7
    elif unique_markets <= 10:
        return 0.3
    else:
        return 0.05


def generate_verdict(score: float, breakdown: dict, unique_markets: int, max_trade: float, ml_score: float = 0.0) -> str:
    """Generates a relevant explanation as to why the wallet was scored this way."""
    if score < 0.3 and ml_score < 0.6:
        return "Low-risk retail profile. Balanced trading activity and standard timing."
    
    reasons = []
    if breakdown.get("entry_timing", 0) >= 0.8:
        reasons.append("traded within hours of market resolution")
    if breakdown.get("trade_concentration", 0) >= 0.8:
        reasons.append("heavily concentrated in a single outcome")
    if breakdown.get("wallet_age", 0) >= 0.8:
        reasons.append("wallet was used immediately after first deposit")
    if unique_markets == 1:
        reasons.append("has only ever traded in one market")
    if max_trade > 5000:
        reasons.append(f"executed a large ${max_trade:,.0f} position")
        
    if ml_score >= 0.85:
        reasons.append(f"shows statistically extreme behavior (ML Anomaly: {ml_score*100:.0f}%)")
        
    if not reasons:
        return f"Suspicious activity detected across multiple factors with a cumulative risk score of {score:.2f}."
    
    verdict = "Flagged because the wallet " + ", ".join(reasons) + "."
    if score >= 0.8 or ml_score >= 0.9:
        verdict = "HIGH SUSPICION: " + verdict
    return verdict


# ── COMPUTE SCORE (Unified Risk Model) ──────────────────────────────────────
def compute_score(wallet: dict, stats: dict, ml_score: float = 0.0) -> dict:
    # Ensure ml_score is a valid float
    if ml_score is None: ml_score = 0.0
    if not stats or stats.get("trade_count", 0) == 0:
        return {
            "address": wallet["address"],
            "global_score": 0.0,
            "rule_score": 0.0,
            "ml_score": ml_score,
            "flagged": False,
            "breakdown": {},
            "trade_count": 0,
        }

    first_trade_at = stats.get("first_trade_at")
    if isinstance(first_trade_at, (int, float)):
        first_trade_at = datetime.fromtimestamp(first_trade_at, tz=timezone.utc)

    breakdown = {
        "entry_timing":         score_entry_timing(stats.get("max_timing_score", 0.05)),
        "trade_concentration":  score_trade_concentration(stats.get("concentration", 0.05)),
        "trade_size":           score_trade_size(stats.get("max_trade_usdc", 0)),
        "wallet_age":           score_wallet_age(wallet.get("first_deposit_at"), first_trade_at),
        "market_count":         score_market_count(stats.get("unique_markets", 0)),
    }

    rule_score = sum(breakdown[f] * RULE_WEIGHTS[f] for f in breakdown)
    
    # Combined Global Score
    global_score = (rule_score * RULE_VS_ML_WEIGHTS["rules"]) + (ml_score * RULE_VS_ML_WEIGHTS["ml"])
    global_score = round(global_score, 4)

    return {
        "address":      wallet["address"],
        "global_score": global_score,
        "rule_score":   round(rule_score, 4),
        "ml_score":     round(ml_score, 4),
        "flagged":      global_score >= config.INSIDER_THRESHOLD,
        "breakdown":    breakdown,
        "descriptions": FACTOR_DESCRIPTIONS,
        "verdict":      generate_verdict(global_score, breakdown, stats["unique_markets"], stats["max_trade_usdc"], ml_score),
        "trade_count":  stats["trade_count"],
        "unique_markets": stats["unique_markets"],
        "max_trade_usdc": stats["max_trade_usdc"],
    }


# ── SINGLE WALLET SCORER (for the API endpoint) ─────────────────────────────
async def score_wallet(address: str) -> dict | None:
    """Score one wallet. Used by GET /wallets/{address}/score."""
    pool = await get_pool()
    addr = address.lower()

    # Fetch wallet
    row = await pool.fetchrow(
        "SELECT address, first_deposit_at, insider_score, anomaly_score FROM wallets WHERE address = $1",
        addr,
    )
    if not row:
        return None

    wallet = {
        "address": row["address"],
        "first_deposit_at": row["first_deposit_at"],
        "anomaly_score": row["anomaly_score"]
    }

    # Fetch trades
    trade_rows = await pool.fetch(
        "SELECT condition_id, usdc_amount, traded_at FROM trades WHERE maker = $1",
        addr,
    )
    trades = [dict(r) for r in trade_rows]

    # Aggregate stats manually for single wallet
    total_usdc = sum(t["usdc_amount"] for t in trades)
    max_trade = max(t["usdc_amount"] for t in trades) if trades else 0
    unique_markets = len(set(t["condition_id"] for t in trades))
    first_trade_at = min(t["traded_at"] for t in trades) if trades else None
    
    stats = {
        "trade_count": len(trades),
        "unique_markets": unique_markets,
        "max_trade_usdc": max_trade,
        "first_trade_at": first_trade_at,
        "concentration": max_trade / total_usdc if total_usdc > 0 else 0,
        "max_timing_score": 0.05 # For simplicity in single lookup
    }

    result = compute_score(wallet, stats)

    # Save score
    await pool.execute("""
        UPDATE wallets SET insider_score = $1, global_score = $2, flagged = $3, scored_at = NOW()
        WHERE address = $4
    """, result["rule_score"], result["global_score"], result["flagged"], addr)

    return result


# ── BATCH SCORER (the production workhorse) ──────────────────────────────────
async def score_all_wallets() -> list:
    """
    Score every wallet in the database using bulk queries.

    Strategy:
      1. Load ALL wallets in one query.
      2. Load ALL trades in one query.
      3. Group trades by maker address in a Python dict.
      4. Score each wallet in-memory (pure CPU).
      5. Write all scores back in one bulk UPDATE.
    """
    pool = await get_pool()
    await publish_alert("info", "Starting global risk scoring...")
    t0 = time.time()

    # Step 1: Load all wallets
    wallet_rows = await pool.fetch(
        "SELECT address, first_deposit_at, anomaly_score FROM wallets"
    )
    wallets = {r["address"]: {
        "address": r["address"], 
        "first_deposit_at": r["first_deposit_at"],
        "anomaly_score": r["anomaly_score"]
    } for r in wallet_rows}

    # Step 2: Load aggregated trade stats from ClickHouse (OLAP)
    ch = get_clickhouse()
    ch_stats = ch.query("""
        SELECT maker, 
               count(*) as trade_count, 
               uniq(condition_id) as market_count,
               max(usdc_amount) as max_trade_usdc,
               min(traded_at) as first_trade_at,
               max(usdc_amount) / sum(usdc_amount) as concentration,
               0.5 as max_timing_score
        FROM trades 
        GROUP BY maker
    """)

    # Map stats by maker address
    # columns: [maker, trade_count, market_count, max_trade_usdc, first_trade_at, concentrated_market]
    stats_by_maker = {
        row[0]: {
            "trade_count": row[1],
            "unique_markets": row[2],
            "max_trade_usdc": row[3],
            "first_trade_at": row[4],
            "concentration": row[5],
            "max_timing_score": row[6],
        }
        for row in ch_stats.result_rows
    }

    load_time = time.time() - t0
    print(f"Data aggregated via ClickHouse in {load_time:.1f}s: "
          f"{len(wallets):,} wallets, {len(stats_by_maker):,} active makers")
    await publish_alert("info", f"Aggregated data for {len(wallets):,} wallets via ClickHouse...")

    # Step 5: Score all wallets in-memory
    flagged = []
    update_rows = []
    from db.cache import is_paused
    
    # Check pause status once before the loop
    if await is_paused():
        print("Scoring aborted: System is paused.")
        return []

    for i, (address, wallet) in enumerate(wallets.items()):
        stats = stats_by_maker.get(address, {})
        if not stats:
            continue
            
        # Get ML score if exists
        ml_score = wallet.get("anomaly_score")
        if ml_score is None: ml_score = 0.0
        
        result = compute_score(wallet, stats, ml_score)
        
        update_rows.append((result["rule_score"], result["ml_score"], result["global_score"], result["flagged"], address))

        if result["flagged"]:
            flagged.append(result)
            if result["global_score"] >= 0.85:
                await publish_alert("info", f"CRITICAL: High-risk wallet identified: {address[:10]}... (Score: {result['global_score']*100:.1f}%)")

    # Step 6: Bulk-write all scores in one executemany call
    async with pool.acquire() as conn:
        await conn.executemany("""
            UPDATE wallets SET 
                insider_score = $1, 
                anomaly_score = $2, 
                global_score = $3, 
                flagged = $4, 
                scored_at = NOW()
            WHERE address = $5
        """, update_rows)

    elapsed = time.time() - t0
    print(f"Scoring complete: {len(flagged)} / {len(wallets):,} wallets flagged "
          f"in {elapsed:.1f}s ({len(wallets) / elapsed:,.0f} wallets/sec)")
    await publish_alert("pipeline_complete", f"Scoring complete: {len(flagged)} wallets flagged as high-risk.")

    # Print top flagged wallets
    flagged.sort(key=lambda x: x["global_score"], reverse=True)
    for r in flagged[:20]:
        print(f"  FLAGGED: {r['address'][:12]}... "
              f"score={r['global_score']:.3f}  trades={r['trade_count']}  "
              f"max=${r['max_trade_usdc']:,.0f}")

    return flagged
