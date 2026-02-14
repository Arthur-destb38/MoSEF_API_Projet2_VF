#!/usr/bin/env python3
"""
Import Reddit Zenodo (r/cryptocurrency) posts into posts2 (no comments).

Two-step workflow:
  1. Transform: python scripts/transform_reddit_zenodo.py  (posts only)
  2. Upload:    python scripts/import_reddit_zenodo.py --from-transformed

One-step: python scripts/import_reddit_zenodo.py (reads reddit_posts.csv, uploads).
"""

import argparse
import os
import sys
from datetime import datetime

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, PROJECT_ROOT)

from dotenv import load_dotenv
load_dotenv(os.path.join(PROJECT_ROOT, ".env"))

DATA_DIR = os.path.join(PROJECT_ROOT, "data_externe", "rediit_cryptocurrency_et_prixBTC")


def _date_to_created_utc(value) -> str:
    if value is None or (isinstance(value, float) and (value != value)):
        return ""
    s = str(value).strip()
    if not s:
        return ""
    try:
        if "T" in s or "Z" in s:
            return s
        if " " in s[:19]:
            dt = datetime.strptime(s[:19], "%Y-%m-%d %H:%M:%S")
        else:
            dt = datetime.strptime(s[:10], "%Y-%m-%d")
        return dt.strftime("%Y-%m-%dT%H:%M:%S")
    except Exception:
        return s


def _row_to_post(row, is_cloud_format: bool) -> dict:
    import pandas as pd
    if is_cloud_format:
        v = row.get("sentiment_score")
        sentiment_score = None
        if v is not None and v != "" and not (isinstance(v, float) and (v != v)):
            try:
                sentiment_score = float(v)
            except (TypeError, ValueError):
                pass
        try:
            num_comments = int(row["num_comments"]) if row.get("num_comments") not in (None, "") else None
        except (TypeError, ValueError):
            num_comments = None
        return {
            "id": str(row.get("id", "")),
            "source": str(row.get("source", "reddit")),
            "method": str(row.get("method", "zenodo_posts")),
            "title": str(row.get("title", "") or ""),
            "text": str(row.get("text", "") or ""),
            "score": int(row.get("score", 0) or 0),
            "created_utc": str(row.get("created_utc", "") or ""),
            "human_label": row.get("human_label") or None,
            "author": row.get("author") or None,
            "subreddit": row.get("subreddit") or None,
            "url": row.get("url") or None,
            "num_comments": num_comments,
            "sentiment_score": sentiment_score,
        }
    return {}


def main():
    parser = argparse.ArgumentParser(description="Import Reddit Zenodo data into posts2")
    parser.add_argument("--from-transformed", action="store_true", help="Read cloud-format CSV and upload")
    parser.add_argument("--dry-run", action="store_true", help="Show sample, do not insert")
    parser.add_argument("--limit", type=int, default=None, help="Limit rows (when not using --from-transformed)")
    args = parser.parse_args()

    import pandas as pd
    from app.storage import save_posts

    if args.from_transformed:
        path = os.path.join(DATA_DIR, "reddit_zenodo_cloud_format.csv")
        if not os.path.isfile(path):
            print(f"Not found: {path}")
            print("Run first: python scripts/transform_reddit_zenodo.py")
            sys.exit(1)
        print(f"Reading {path}")
        df = pd.read_csv(path)
        posts = [_row_to_post(row, True) for _, row in df.iterrows() if str(row.get("text", "")).strip()]
    else:
        posts = []
        posts_path = os.path.join(DATA_DIR, "reddit_posts.csv")
        if os.path.isfile(posts_path):
            df = pd.read_csv(posts_path, nrows=args.limit)
            for _, row in df.iterrows():
                text = row.get("body", "")
                if pd.isna(text):
                    continue
                posts.append({
                    "id": str(int(row.get("X", 0))),
                    "source": "reddit",
                    "method": "zenodo_posts",
                    "title": str(row.get("title", "") or "")[:1000],
                    "text": str(text).strip()[:50000],
                    "score": int(row.get("score", 0) or 0),
                    "created_utc": _date_to_created_utc(row.get("date")),
                    "human_label": None,
                    "author": None,
                    "subreddit": "CryptoCurrency",
                    "url": str(row.get("url", "") or ""),
                    "num_comments": int(row.get("num_comments", 0)) if row.get("num_comments") not in (None, "") and not pd.isna(row.get("num_comments")) else None,
                    "sentiment_score": None,
                })
        else:
            print(f"Not found: {posts_path}")
            sys.exit(1)

    print(f"  {len(posts)} rows ready (source=reddit, method=zenodo_posts).")

    if args.dry_run:
        for j, p in enumerate(posts[:3]):
            print(f"  [{j+1}] {p.get('method')} created_utc={p.get('created_utc')} text={p.get('text', '')[:60]}...")
        print("  (dry-run: no insert)")
        return

    result = save_posts(posts, source=None, method=None)
    print(f"Result: {result['inserted']} inserted / {result['total']} processed (db_type={result.get('db_type', '?')}).")
    if result.get("error"):
        print("Error:", result["error"])


if __name__ == "__main__":
    main()
