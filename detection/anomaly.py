"""
detection/anomaly.py — Unsupervised anomaly detection (Layer 2).

HOW THIS WORKS:
  Isolation Forest asks: "How easy is it to isolate this wallet from the rest?"
  
  Normal wallets look similar to each other — they trade moderate amounts
  across several markets over weeks/months. It takes many "cuts" to separate
  a normal wallet from the crowd.
  
  Insider wallets are WEIRD — they might have 1 giant trade in 1 market from
  a brand-new wallet. It only takes a few "cuts" to isolate them.
  
  The fewer cuts needed → the higher the anomaly score.

WHY UNSUPERVISED?
  Unlike supervised ML (which needs labeled data like "this IS an insider"),
  Isolation Forest needs NO labels at all. It just finds wallets that are
  statistically unusual compared to the majority.

FEATURES USED:
  1. trade_count        — how many trades this wallet made
  2. unique_markets     — how many different markets they traded in
  3. max_trade_usdc     — their single largest trade
  4. total_volume       — total USDC traded across all trades
  5. avg_trade_usdc     — average trade size
  6. concentration      — % of trades in their most-traded market
  7. wallet_age_days    — days since first deposit (0 if unknown)
"""
import time
from collections import Counter, defaultdict

import numpy as np
from sklearn.ensemble import IsolationForest

from db.pool import get_pool
from db.clickhouse import get_clickhouse
from db.cache import publish_alert


async def run_anomaly_detection(contamination: float = 0.05):
    """
    Run Isolation Forest on all wallets and store anomaly scores.

    contamination: expected fraction of insiders (default 5%).
                   Lower = fewer flagged. Higher = more flagged.
    """
    pool = await get_pool()
    await publish_alert("info", "Starting ML behavioral anomaly detection...")
    t0 = time.time()

    # ── Step 1 & 2: Feature Engineering via ClickHouse (OLAP) ──────────────
    # We aggregate all stats (Layer 2 features) in a single ultra-fast query.
    ch = get_clickhouse()
    ch_result = ch.query("""
        SELECT 
            maker,
            count(*) as trade_count,
            uniq(condition_id) as market_count,
            max(usdc_amount) as max_trade,
            sum(usdc_amount) as total_volume,
            avg(usdc_amount) as avg_trade,
            -- Concentration (approximated for speed)
            max(usdc_amount) / sum(usdc_amount) as concentration
        FROM trades
        GROUP BY maker
        HAVING trade_count >= 1
    """)

    if not ch_result.result_rows:
        print("No active wallets found for anomaly detection.")
        return []

    addresses = []
    features = []
    
    # Map back to Postgres to get wallet age (relational data)
    wallet_data = await pool.fetch("SELECT address, first_deposit_at FROM wallets")
    age_map = {r["address"]: r["first_deposit_at"] for r in wallet_data}

    for row in ch_result.result_rows:
        addr = row[0]
        # trade_count, unique_markets, max_trade, total_volume, avg_trade, concentration
        f = list(row[1:]) 
        
        # Add wallet age (days) from Postgres map
        age = age_map.get(addr)
        age_days = 0
        if age:
            age_days = (time.time() - age.timestamp()) / 86400
        f.append(max(0, int(age_days)))

        addresses.append(addr)
        features.append(f)

    feature_matrix = np.array(features)

    load_time = time.time() - t0
    print(f"Features computed for {len(addresses):,} wallets in {load_time:.1f}s")
    print(f"  Feature shape: {feature_matrix.shape}")
    await publish_alert("info", f"Extracted behavioral features for {len(addresses):,} wallets...")

    # ── Step 3: Fit Isolation Forest ──────────────────────────────────────
    #
    # contamination = expected % of outliers in the data
    # random_state = fixed seed for reproducible results
    # n_estimators = number of trees (more = more stable, slower)
    model = IsolationForest(
        contamination=contamination,
        random_state=42,
        n_estimators=200,
        n_jobs=-1,  # use all CPU cores
    )

    model.fit(feature_matrix)

    # decision_function returns negative scores for outliers, positive for inliers.
    # We invert and normalize to 0.0-1.0 range where 1.0 = most anomalous.
    raw_scores = model.decision_function(feature_matrix)

    # Normalize: map the raw scores to [0, 1] where higher = more anomalous
    min_score = raw_scores.min()
    max_score = raw_scores.max()
    if max_score - min_score > 0:
        anomaly_scores = 1.0 - (raw_scores - min_score) / (max_score - min_score)
    else:
        anomaly_scores = np.zeros(len(raw_scores))

    fit_time = time.time() - t0 - load_time
    print(f"Isolation Forest fitted in {fit_time:.1f}s")
    await publish_alert("info", f"Isolation Forest model fitted in {fit_time:.1f}s. Calculating anomaly scores...")

    # ── Step 4: Save anomaly scores to database ──────────────────────────
    update_rows = [
        (float(anomaly_scores[i]), addresses[i])
        for i in range(len(addresses))
    ]

    async with pool.acquire() as conn:
        await conn.executemany("""
            UPDATE wallets SET anomaly_score = $1 WHERE address = $2
        """, update_rows)

    # ── Step 5: Report results ────────────────────────────────────────────
    #
    # Find wallets that Isolation Forest flagged but deterministic scorer missed.
    flagged_by_ml = []
    for i, addr in enumerate(addresses):
        if anomaly_scores[i] >= 0.85:  # top ~5% most anomalous
            flagged_by_ml.append({
                "address": addr,
                "anomaly_score": round(float(anomaly_scores[i]), 4),
                "trade_count": int(features[i][0]),
                "unique_markets": int(features[i][1]),
                "max_trade_usdc": round(features[i][2], 2),
                "total_volume": round(features[i][3], 2),
                "wallet_age_days": int(features[i][6]),
            })

    flagged_by_ml.sort(key=lambda x: x["anomaly_score"], reverse=True)

    elapsed = time.time() - t0
    print(f"\nAnomaly detection complete in {elapsed:.1f}s")
    print(f"Wallets flagged by ML: {len(flagged_by_ml)}")
    await publish_alert("info", f"ML Detection complete: {len(flagged_by_ml)} behavioral anomalies identified.")

    # Show wallets that ML flagged
    for r in flagged_by_ml[:15]:
        print(f"  ANOMALY: {r['address'][:12]}... "
              f"score={r['anomaly_score']:.3f}  "
              f"trades={r['trade_count']}  "
              f"max=${r['max_trade_usdc']:,.0f}  "
              f"volume=${r['total_volume']:,.0f}  "
              f"age={r['wallet_age_days']}d")

    return flagged_by_ml
