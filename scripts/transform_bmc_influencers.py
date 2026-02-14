#!/usr/bin/env python3
"""
Transform BMC Twitter influencers CSV to cloud table schema (posts2).
Reads: data_externe/BMC_Twitter_influenceur/dataset_52-person-from-2021-02-05_2023-06-12_*_with_sentiment.csv
Columns: created_at, favorite_count, full_text, reply_count, retweet_count, compound, sentiment_type, ...
Output: bmc_influencers_cloud_format.csv (same columns as posts2)

Then run: python scripts/import_bmc_influencers.py --from-transformed
"""

import argparse
import hashlib
import os
import sys
from datetime import datetime

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, PROJECT_ROOT)

DATA_DIR = os.path.join(PROJECT_ROOT, "data_externe", "BMC_Twitter_influenceur")


def _post_uid(source: str, method: str, post_id: str) -> str:
    base = f"{source}:{method}:{post_id}"
    return hashlib.sha1(base.encode("utf-8")).hexdigest()


def _date_to_created_utc(value) -> str:
    """Parse M/D/YYYY or M/D/YYYY H:MM and return ISO."""
    if value is None or (isinstance(value, float) and (value != value)):
        return ""
    s = str(value).strip()
    if not s:
        return ""
    try:
        # "2/1/2021" or "12/25/2022 14:30"
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


CLOUD_COLUMNS = [
    "uid", "id", "source", "method", "title", "text", "score", "created_utc",
    "human_label", "author", "subreddit", "url", "num_comments", "scraped_at", "sentiment_score", "is_influencer",
]


def main():
    parser = argparse.ArgumentParser(description="Transform BMC influencers CSV to cloud schema")
    parser.add_argument("--limit", type=int, default=None, help="Limit number of rows")
    parser.add_argument("--csv", type=str, default=None, help="Input CSV path")
    parser.add_argument("--output", type=str, default=None)
    args = parser.parse_args()

    import pandas as pd
    import glob

    if args.csv:
        csv_path = args.csv
    else:
        pattern = os.path.join(DATA_DIR, "*_with_sentiment.csv")
        candidates = [f for f in glob.glob(pattern) if "dataset_52" in f or "with_sentiment" in f]
        if not candidates:
            print(f"No *_with_sentiment.csv found in {DATA_DIR}")
            sys.exit(1)
        csv_path = candidates[0]

    if not os.path.isfile(csv_path):
        print(f"File not found: {csv_path}")
        sys.exit(1)

    out_path = args.output or os.path.join(DATA_DIR, "bmc_influencers_cloud_format.csv")
    scraped_at = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
    source = "bmc_influencers"
    method = "bmc_import"

    print(f"Reading {csv_path}...")
    df = pd.read_csv(csv_path, nrows=args.limit)

    # First column may be unnamed (index)
    id_col = df.columns[0] if df.columns[0] != "created_at" else None
    if "full_text" not in df.columns and "clean_text" in df.columns:
        text_col = "clean_text"
    else:
        text_col = "full_text"

    rows = []
    for i, row in df.iterrows():
        text = row.get(text_col, "") or row.get("full_text", "")
        if pd.isna(text) or not str(text).strip():
            continue
        post_id = str(row.get(id_col, i)) if id_col else str(i)
        uid = _post_uid(source, method, post_id)
        created_utc = _date_to_created_utc(row.get("created_at"))
        try:
            sentiment_score = float(row.get("compound", 0))
        except (TypeError, ValueError):
            sentiment_score = ""
        human_label = str(row.get("sentiment_type", "")).strip() or ""
        score = int(row.get("favorite_count", 0)) if row.get("favorite_count") not in (None, "") and not pd.isna(row.get("favorite_count")) else 0
        num_comments = int(row.get("reply_count", 0)) if row.get("reply_count") not in (None, "") and not pd.isna(row.get("reply_count")) else 0

        rows.append({
            "uid": uid,
            "id": post_id,
            "source": source,
            "method": method,
            "title": "",
            "text": str(text).strip()[:50000],
            "score": score,
            "created_utc": created_utc,
            "human_label": human_label,
            "author": "",
            "subreddit": str(row.get("new_coins", "") or "")[:200],
            "url": "",
            "num_comments": num_comments,
            "scraped_at": scraped_at,
            "sentiment_score": sentiment_score,
            "is_influencer": True,
        })

    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    out_df = pd.DataFrame(rows, columns=CLOUD_COLUMNS)
    out_df.to_csv(out_path, index=False, encoding="utf-8")
    print(f"Written {len(rows)} rows to {out_path}")
    print("Next: python scripts/import_bmc_influencers.py --from-transformed")


if __name__ == "__main__":
    main()
