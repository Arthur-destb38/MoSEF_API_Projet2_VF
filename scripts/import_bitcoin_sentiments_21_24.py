#!/usr/bin/env python3
"""
Import Bitcoin News / Sentiments 2021-2024 (Yahoo Finance) into posts2 (cloud or local).

Two-step workflow (recommended):
  1. Transform:  python scripts/transform_bitcoin_sentiments_21_24.py
     → Produces data_externe/bitcoinesentiment/bitcoin_sentiments_cloud_format.csv
     (same columns as cloud table: uid, source, text, created_utc, sentiment_score, ...)
  2. Upload:     python scripts/import_bitcoin_sentiments_21_24.py --from-transformed
     → Reads the transformed file and sends to cloud.

One-step (transform in memory then upload):
  python scripts/import_bitcoin_sentiments_21_24.py

Before first cloud upload: run migration 001_add_sentiment_score.sql on Supabase.
"""

import argparse
import os
import sys
from datetime import datetime

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, PROJECT_ROOT)

from dotenv import load_dotenv
load_dotenv(os.path.join(PROJECT_ROOT, ".env"))


def _date_to_created_utc(value) -> str:
    """Convertit la date du CSV en chaîne pour created_utc (ISO si possible)."""
    if value is None or (isinstance(value, float) and (value != value)):
        return ""
    s = str(value).strip()
    if not s:
        return ""
    try:
        # "2021-11-05 04:42:00" ou déjà ISO
        if "T" in s or "Z" in s:
            return s
        dt = datetime.strptime(s[:19], "%Y-%m-%d %H:%M:%S")
        return dt.strftime("%Y-%m-%dT%H:%M:%S")
    except Exception:
        return s


def _row_to_post(row, is_cloud_format: bool):
    """Build a post dict from a CSV row (original or cloud-format)."""
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
            "source": str(row.get("source", "yahoo_finance")),
            "method": str(row.get("method", "import_21_24")),
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
    # Original CSV
    return {
        "id": str(row.get("id", "")),
        "source": "yahoo_finance",
        "method": "import_21_24",
        "title": "",
        "text": str(row.get("text", "") or "").strip()[:50000],
        "score": 0,
        "created_utc": str(row.get("created_utc", "") or ""),
        "human_label": None,
        "author": None,
        "subreddit": None,
        "url": None,
        "num_comments": None,
        "sentiment_score": float(row["sentiment_score"]) if row.get("sentiment_score") not in (None, "", float("nan")) else None,
    }


def main():
    parser = argparse.ArgumentParser(description="Import Bitcoin Sentiments 2021-2024 into posts2")
    parser.add_argument("--limit", type=int, default=None, help="Limit number of rows (original CSV only)")
    parser.add_argument("--dry-run", action="store_true", help="Show sample, do not insert")
    parser.add_argument("--csv", type=str, default=None, help="Path to original CSV")
    parser.add_argument("--from-transformed", action="store_true", help="Read cloud-format CSV (from transform script) and upload")
    args = parser.parse_args()

    import pandas as pd
    from app.storage import save_posts

    if args.from_transformed:
        default_path = os.path.join(PROJECT_ROOT, "data_externe", "bitcoinesentiment", "bitcoin_sentiments_cloud_format.csv")
        csv_path = args.csv or default_path
        if not os.path.isfile(csv_path):
            print(f"File not found: {csv_path}")
            print("Run first: python scripts/transform_bitcoin_sentiments_21_24.py")
            sys.exit(1)
        print(f"Reading cloud-format file: {csv_path}")
        df = pd.read_csv(csv_path)
        posts = [_row_to_post(row, is_cloud_format=True) for _, row in df.iterrows() if str(row.get("text", "")).strip()]
    else:
        default_path = os.path.join(PROJECT_ROOT, "data_externe", "bitcoinesentiment", "bitcoin_sentiments_21_24.csv")
        csv_path = args.csv or default_path
        if not os.path.isfile(csv_path):
            print(f"File not found: {csv_path}")
            sys.exit(1)
        print(f"Reading original CSV: {csv_path}")
        df = pd.read_csv(csv_path, nrows=args.limit)
        for col in ["Date", "Short Description", "Accurate Sentiments"]:
            if col not in df.columns:
                print(f"Missing column: {col}. Columns: {list(df.columns)}")
                sys.exit(1)
        posts = []
        for i, row in df.iterrows():
            text = row.get("Short Description", "")
            if pd.isna(text) or not str(text).strip():
                continue
            created_utc = _date_to_created_utc(row.get("Date"))
            try:
                sentiment_score = float(row["Accurate Sentiments"])
            except (TypeError, ValueError):
                sentiment_score = None
            posts.append({
                "id": str(i),
                "source": "yahoo_finance",
                "method": "import_21_24",
                "title": "",
                "text": str(text).strip()[:50000],
                "score": 0,
                "created_utc": created_utc,
                "human_label": None,
                "author": None,
                "subreddit": None,
                "url": None,
                "num_comments": None,
                "sentiment_score": sentiment_score,
            })

    print(f"  {len(posts)} rows ready (source=yahoo_finance, method=import_21_24).")

    if args.dry_run:
        for j, p in enumerate(posts[:3]):
            print(f"  [{j+1}] created_utc={p['created_utc']} sentiment_score={p['sentiment_score']} text={p['text'][:70]}...")
        print("  (dry-run: aucun enregistrement inséré)")
        return

    result = save_posts(posts, source="yahoo_finance", method="import_21_24")
    print(f"Résultat: {result['inserted']} insérés / {result['total']} traités (db_type={result.get('db_type', '?')}).")
    if result.get("error"):
        print("Erreur:", result["error"])
    if result.get("inserted", 0) == 0 and result.get("total", 0) > 0 and result.get("db_type") == "supabase_rest":
        print("  Astuce: si la table existe déjà sans la colonne sentiment_score, exécutez la migration:")
        print("  → Supabase SQL Editor: ALTER TABLE posts2 ADD COLUMN sentiment_score REAL;")


if __name__ == "__main__":
    main()
