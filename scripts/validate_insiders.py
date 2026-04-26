"""
validate_known_insiders.py — Tests the scorer against known insider wallets.

Run with: python validate_known_insiders.py
"""
import asyncio
import os
import sys

# Ensure the parent directory is in sys.path so we can import from 'detection', 'db', etc.
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from detection.scorer import score_wallet
from db.pool import create_tables, close_pool

KNOWN_INSIDERS = [
    ("0xee50a31c3f5a7c77824b12a941a54388a2827ed6", "Google d4vd market"),
    ("0x6baf05d193692bb208d616709e27442c910a94c5", "Maduro out SBet365"),
    ("0x31a56e9e690c621ed21de08cb559e9524cdb8ed9", "Maduro out unnamed"),
    ("0x0afc7ce56285bde1fbe3a75efaffdfc86d6530b2", "Israel Iran ricosuave"),
    ("0x7f1329ade2ec162c6f8791dad99125e0dc49801c", "Trump pardon CZ gj1"),
    ("0x976685b6e867a0400085b1273309e84cd0fc627c", "MicroStrategy fromagi"),
    ("0x55ea982cebff271722419595e0659ef297b48d7c", "DraftKings flaccidwillie"),
]

THRESHOLD = 0.65


async def main():
    await create_tables()

    print(f"\n{'Wallet':<15} {'Name':<30} {'Score':>6}  {'Flagged'}")
    print("-" * 70)

    passed = 0
    for address, name in KNOWN_INSIDERS:
        result = await score_wallet(address)

        if result is None:
            print(f"{address[:12]}...  {name:<30}  NOT IN DB -- run backfill first")
            continue

        score   = result["score"]
        flagged = "YES" if result["flagged"] else "NO"
        print(f"{address[:12]}...  {name:<30}  {score:>6.3f}  {flagged}")

        if result["flagged"]:
            passed += 1

    total = len(KNOWN_INSIDERS)
    print(f"\n{'-' * 70}")
    status = "PASS" if passed == total else "NEEDS TUNING"
    print(f"Result: {passed}/{total} correctly flagged ({status})")
    print(f"Threshold used: {THRESHOLD}\n")

    await close_pool()


asyncio.run(main())
