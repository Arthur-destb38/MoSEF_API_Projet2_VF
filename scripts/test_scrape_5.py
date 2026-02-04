#!/usr/bin/env python3
"""
Test minimal : 10 posts × 5 plateformes (uniquement HTTP/API, pas de Selenium).
Enregistrement en base : Supabase si DATABASE_URL est joignable, sinon SQLite.
Usage : depuis la racine du projet : python scripts/test_scrape_5.py
"""

import os
import sys
from datetime import datetime

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from dotenv import load_dotenv
load_dotenv(os.path.join(PROJECT_ROOT, ".env"))

from app.storage import save_posts
from app.scrapers import (
    scrape_reddit,
    scrape_4chan_biz,
    scrape_bitcointalk,
    scrape_github_discussions,
    scrape_telegram_simple,
    TELEGRAM_CHANNELS,
)

LIMIT = 10


def _log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")


def main() -> None:
    _log("=== Test : 10 posts × 5 plateformes → base (Supabase ou SQLite) ===")
    total_inserted = 0

    # 1. Reddit (HTTP, rapide)
    try:
        _log("  → Reddit r/bitcoin (10 posts)...")
        posts = scrape_reddit("bitcoin", limit=LIMIT, method="http")
        posts = (posts or [])[:LIMIT]
        if posts:
            out = save_posts(posts, source="reddit", method="http")
            n = out.get("inserted", 0)
            total_inserted += n
            _log(f"  ← Reddit: {len(posts)} récupérés, {n} insérés (db: {out.get('db_type', '?')})")
        else:
            _log("  ← Reddit: 0 post")
    except Exception as e:
        _log(f"  ✗ Reddit: {e}")

    # 2. 4chan
    try:
        _log("  → 4chan /biz/ crypto (10 posts)...")
        posts = scrape_4chan_biz("crypto", limit=LIMIT)
        posts = (posts or [])[:LIMIT]
        if posts:
            out = save_posts(posts, source="4chan", method="http")
            n = out.get("inserted", 0)
            total_inserted += n
            _log(f"  ← 4chan: {len(posts)} récupérés, {n} insérés (db: {out.get('db_type', '?')})")
        else:
            _log("  ← 4chan: 0 post")
    except Exception as e:
        _log(f"  ✗ 4chan: {e}")

    # 3. Bitcointalk
    try:
        _log("  → Bitcointalk bitcoin (10 posts)...")
        posts = scrape_bitcointalk("bitcoin", limit=LIMIT)
        posts = (posts or [])[:LIMIT]
        if posts:
            out = save_posts(posts, source="bitcointalk", method="http")
            n = out.get("inserted", 0)
            total_inserted += n
            _log(f"  ← Bitcointalk: {len(posts)} récupérés, {n} insérés (db: {out.get('db_type', '?')})")
        else:
            _log("  ← Bitcointalk: 0 post")
    except Exception as e:
        _log(f"  ✗ Bitcointalk: {e}")

    # 4. GitHub discussions (API, peut nécessiter GITHUB_TOKEN pour plus de requêtes)
    try:
        _log("  → GitHub discussions bitcoin (10 posts)...")
        posts = scrape_github_discussions("bitcoin", limit=LIMIT)
        posts = (posts or [])[:LIMIT]
        if posts:
            out = save_posts(posts, source="github", method="api")
            n = out.get("inserted", 0)
            total_inserted += n
            _log(f"  ← GitHub: {len(posts)} récupérés, {n} insérés (db: {out.get('db_type', '?')})")
        else:
            _log("  ← GitHub: 0 post")
    except Exception as e:
        _log(f"  ✗ GitHub: {e}")

    # 5. Telegram (1 channel, simple)
    try:
        channel = list(TELEGRAM_CHANNELS.keys())[0] if TELEGRAM_CHANNELS else "bitcoinnews"
        _log(f"  → Telegram {channel} (10 posts)...")
        posts = scrape_telegram_simple(channel, limit=LIMIT)
        posts = (posts or [])[:LIMIT]
        if posts:
            out = save_posts(posts, source="telegram", method="simple")
            n = out.get("inserted", 0)
            total_inserted += n
            _log(f"  ← Telegram: {len(posts)} récupérés, {n} insérés (db: {out.get('db_type', '?')})")
        else:
            _log("  ← Telegram: 0 post")
    except Exception as e:
        _log(f"  ✗ Telegram: {e}")

    _log("=== Fin du test ===")
    _log(f"Total nouveaux posts enregistrés : {total_inserted}")
    print("")
    print("Si tu vois 'db: sqlite' → la base utilisée est locale (Supabase injoignable).")
    print("Si tu vois 'db: postgres' → les posts sont bien sur Supabase.")


if __name__ == "__main__":
    main()
