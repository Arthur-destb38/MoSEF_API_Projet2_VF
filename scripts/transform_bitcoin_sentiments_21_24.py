#!/usr/bin/env python3
"""
Step 1: Transform the Bitcoin Sentiments CSV into a file that matches the cloud table schema.
Output: data_externe/bitcoinesentiment/bitcoin_sentiments_cloud_format.csv
Columns = same as posts2: uid, id, source, method, title, text, score, created_utc,
          human_label, author, subreddit, url, num_comments, scraped_at, sentiment_score

Run this first, then run import_bitcoin_sentiments_21_24.py --from-transformed to upload.

Usage:
  python scripts/transform_bitcoin_sentiments_21_24.py
  python scripts/transform_bitcoin_sentiments_21_24.py --limit 100
"""

import argparse
import hashlib
import os
import sys
from datetime import datetime

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, PROJECT_ROOT)


def _post_uid(source: str, method: str, post_id: str) -> str:
    """Same as app.storage._post_uid so uids match the cloud."""
    base = f"{source}:{method}:{post_id}"
    return hashlib.sha1(base.encode("utf-8")).hexdigest()


def _date_to_created_utc(value) -> str:
    if value is None or (isinstance(value, float) and (value != value)):
        return ""
    s = str(value).strip()
    if not s:
        return ""
    try:
        if "T" in s or "Z" in s:
            return s
        dt = datetime.strptime(s[:19], "%Y-%m-%d %H:%M:%S")
        return dt.strftime("%Y-%m-%dT%H:%M:%S")
    except Exception:
        return s


# Column order matching posts2 (cloud table)
CLOUD_COLUMNS = [
    "uid", "id", "source", "method", "title", "text", "score", "created_utc",
    "human_label", "author", "subreddit", "url", "num_comments", "scraped_at", "sentiment_score",
]


def main():
    parser = argparse.ArgumentParser(description="Transform Bitcoin Sentiments CSV to cloud schema")
    parser.add_argument("--limit", type=int, default=None, help="Limit number of rows")
    parser.add_argument("--csv", type=str, default=None, help="Input CSV path")
    parser.add_argument("--output", type=str, default=None, help="Output CSV path (cloud format)")
    args = parser.parse_args()

    default_input = os.path.join(PROJECT_ROOT, "data_externe", "bitcoinesentiment", "bitcoin_sentiments_21_24.csv")
    default_output = os.path.join(PROJECT_ROOT, "data_externe", "bitcoinesentiment", "bitcoin_sentiments_cloud_format.csv")

    csv_path = args.csv or default_input
    out_path = args.output or default_output

    if not os.path.isfile(csv_path):
        print(f"File not found: {csv_path}")
        sys.exit(1)

    import pandas as pd

    print(f"Reading {csv_path}...")
    df = pd.read_csv(csv_path, nrows=args.limit)

    for col in ["Date", "Short Description", "Accurate Sentiments"]:
        if col not in df.columns:
            print(f"Missing column: {col}. Columns: {list(df.columns)}")
            sys.exit(1)

    scraped_at = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
    source = "yahoo_finance"
    method = "import_21_24"

    rows = []
    for i, row in df.iterrows():
        text = row.get("Short Description", "")
        if pd.isna(text) or not str(text).strip():
            continue
        created_utc = _date_to_created_utc(row.get("Date"))
        try:
            sentiment_score = float(row["Accurate Sentiments"])
        except (TypeError, ValueError):
            sentiment_score = ""

        post_id = str(i)
        uid = _post_uid(source, method, post_id)

        rows.append({
            "uid": uid,
            "id": post_id,
            "source": source,
            "method": method,
            "title": "",
            "text": str(text).strip()[:50000],
            "score": 0,
            "created_utc": created_utc,
            "human_label": "",
            "author": "",
            "subreddit": "",
            "url": "",
            "num_comments": "",
            "scraped_at": scraped_at,
            "sentiment_score": sentiment_score if sentiment_score != "" else "",
        })

    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    out_df = pd.DataFrame(rows, columns=CLOUD_COLUMNS)
    out_df.to_csv(out_path, index=False, encoding="utf-8")
    print(f"Written {len(rows)} rows to {out_path}")
    print(f"Columns (same as cloud table posts2): {CLOUD_COLUMNS}")
    print("Next step: python scripts/import_bitcoin_sentiments_21_24.py --from-transformed")


if __name__ == "__main__":
    main()
