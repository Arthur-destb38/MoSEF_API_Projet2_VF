#!/usr/bin/env python3
"""
Import BMC Twitter influencers data into posts2.

Two-step workflow:
  1. Transform: python scripts/transform_bmc_influencers.py
  2. Upload:    python scripts/import_bmc_influencers.py --from-transformed

One-step: python scripts/import_bmc_influencers.py (reads CSV, transforms in memory, uploads).
"""

import argparse
import os
import sys
from datetime import datetime

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, PROJECT_ROOT)

from dotenv import load_dotenv
load_dotenv(os.path.join(PROJECT_ROOT, ".env"))

DATA_DIR = os.path.join(PROJECT_ROOT, "data_externe", "BMC_Twitter_influenceur")


def _date_to_created_utc(value) -> str:
    if value is None or (isinstance(value, float) and (value != value)):
        return ""
    s = str(value).strip()
    if not s:
        return ""
    try:
        parts = s.split()
        if "/" in parts[0]:
            m, d, y = parts[0].split("/")
            dt = datetime(int(y), int(m), int(d))
            if len(parts) > 1 and ":" in parts[1]:
                h, mi = parts[1].split(":")[:2]
                dt = dt.replace(hour=int(h), minute=int(mi), second=0, microsecond=0)
            return dt.strftime("%Y-%m-%dT%H:%M:%S")
    except Exception:
        pass
    return s


def _row_to_post(row, is_cloud_format: bool) -> dict:
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
            "source": str(row.get("source", "bmc_influencers")),
            "method": str(row.get("method", "bmc_import")),
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
            "is_influencer": True,
        }
    return {}


def main():
    parser = argparse.ArgumentParser(description="Import BMC Twitter influencers into posts2")
    parser.add_argument("--from-transformed", action="store_true", help="Read cloud-format CSV and upload")
    parser.add_argument("--dry-run", action="store_true", help="Show sample, do not insert")
    parser.add_argument("--limit", type=int, default=None, help="Limit rows (when not using --from-transformed)")
    parser.add_argument("--csv", type=str, default=None)
    args = parser.parse_args()

    import pandas as pd
    import glob
    from app.storage import save_posts

    if args.from_transformed:
        path = args.csv or os.path.join(DATA_DIR, "bmc_influencers_cloud_format.csv")
        if not os.path.isfile(path):
            print(f"Not found: {path}")
            print("Run first: python scripts/transform_bmc_influencers.py")
            sys.exit(1)
        print(f"Reading {path}")
        df = pd.read_csv(path)
        posts = [_row_to_post(row, True) for _, row in df.iterrows() if str(row.get("text", "")).strip()]
    else:
        pattern = os.path.join(DATA_DIR, "*_with_sentiment.csv")
        candidates = [f for f in glob.glob(pattern) if "dataset_52" in f or "with_sentiment" in f]
        csv_path = args.csv or (candidates[0] if candidates else None)
        if not csv_path or not os.path.isfile(csv_path):
            print(f"No BMC CSV found in {DATA_DIR} or file not found: {args.csv}")
            sys.exit(1)
        print(f"Reading {csv_path}")
        df = pd.read_csv(csv_path, nrows=args.limit)
        text_col = "full_text" if "full_text" in df.columns else "clean_text"
        id_col = df.columns[0]
        posts = []
        for i, row in df.iterrows():
            text = row.get(text_col, "")
            if pd.isna(text) or not str(text).strip():
                continue
            try:
                sentiment_score = float(row.get("compound", 0))
            except (TypeError, ValueError):
                sentiment_score = None
            posts.append({
                "id": str(row.get(id_col, i)),
                "source": "bmc_influencers",
                "method": "bmc_import",
                "title": "",
                "text": str(text).strip()[:50000],
                "score": int(row.get("favorite_count", 0)) if row.get("favorite_count") not in (None, "") and not pd.isna(row.get("favorite_count")) else 0,
                "created_utc": _date_to_created_utc(row.get("created_at")),
                "human_label": str(row.get("sentiment_type", "")).strip() or None,
                "author": None,
                "subreddit": str(row.get("new_coins", "") or "")[:200] or None,
                "url": None,
                "num_comments": int(row.get("reply_count", 0)) if row.get("reply_count") not in (None, "") and not pd.isna(row.get("reply_count")) else None,
                "sentiment_score": sentiment_score,
                "is_influencer": True,
            })

    print(f"  {len(posts)} rows ready (source=bmc_influencers, method=bmc_import).")

    if args.dry_run:
        for j, p in enumerate(posts[:3]):
            print(f"  [{j+1}] created_utc={p.get('created_utc')} sentiment={p.get('human_label')} text={p.get('text', '')[:60]}...")
        print("  (dry-run: no insert)")
        return

    result = save_posts(posts, source=None, method=None)
    print(f"Result: {result['inserted']} inserted / {result['total']} processed (db_type={result.get('db_type', '?')}).")
    if result.get("error"):
        print("Error:", result["error"])


if __name__ == "__main__":
    main()
