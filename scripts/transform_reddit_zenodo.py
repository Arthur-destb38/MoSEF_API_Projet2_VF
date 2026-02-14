#!/usr/bin/env python3
"""
Transform Reddit r/cryptocurrency Zenodo data to cloud table schema (posts2).
Reads: reddit_posts.csv only (no comments).
Output: reddit_zenodo_cloud_format.csv (same columns as posts2)

Then run: python scripts/import_reddit_zenodo.py --from-transformed
"""

import argparse
import hashlib
import os
import sys
from datetime import datetime

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, PROJECT_ROOT)

DATA_DIR = os.path.join(PROJECT_ROOT, "data_externe", "rediit_cryptocurrency_et_prixBTC")


def _post_uid(source: str, method: str, post_id: str) -> str:
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
        # "2021-05-08 15:28:35" or "2021-05-08"
        if " " in s[:19]:
            dt = datetime.strptime(s[:19], "%Y-%m-%d %H:%M:%S")
        else:
            dt = datetime.strptime(s[:10], "%Y-%m-%d")
        return dt.strftime("%Y-%m-%dT%H:%M:%S")
    except Exception:
        return s


CLOUD_COLUMNS = [
    "uid", "id", "source", "method", "title", "text", "score", "created_utc",
    "human_label", "author", "subreddit", "url", "num_comments", "scraped_at", "sentiment_score",
]


def main():
    parser = argparse.ArgumentParser(description="Transform Reddit Zenodo CSVs to cloud schema")
    parser.add_argument("--limit", type=int, default=None, help="Limit number of posts")
    parser.add_argument("--output", type=str, default=None)
    args = parser.parse_args()

    import pandas as pd

    out_path = args.output or os.path.join(DATA_DIR, "reddit_zenodo_cloud_format.csv")
    scraped_at = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
    source = "reddit"

    rows = []

    # Posts only (no comments): X, title, score, url, num_comments, body, date
    posts_path = os.path.join(DATA_DIR, "reddit_posts.csv")
    if os.path.isfile(posts_path):
        df_posts = pd.read_csv(posts_path, nrows=args.limit)
        for _, row in df_posts.iterrows():
            text = row.get("body", "")
            if pd.isna(text):
                text = ""
            text = str(text).strip()[:50000]
            post_id = str(int(row.get("X", 0)))
            uid = _post_uid(source, "zenodo_posts", post_id)
            rows.append({
                "uid": uid,
                "id": post_id,
                "source": source,
                "method": "zenodo_posts",
                "title": str(row.get("title", "") or "")[:1000],
                "text": text,
                "score": int(row.get("score", 0) or 0),
                "created_utc": _date_to_created_utc(row.get("date")),
                "human_label": "",
                "author": "",
                "subreddit": "CryptoCurrency",
                "url": str(row.get("url", "") or ""),
                "num_comments": int(row.get("num_comments", 0)) if row.get("num_comments") not in (None, "") and not pd.isna(row.get("num_comments")) else 0,
                "scraped_at": scraped_at,
                "sentiment_score": "",
            })
        print(f"Posts: {len(rows)} rows")
    else:
        print(f"Not found: {posts_path}")

    if not rows:
        print("No rows to write.")
        sys.exit(1)

    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    out_df = pd.DataFrame(rows, columns=CLOUD_COLUMNS)
    out_df.to_csv(out_path, index=False, encoding="utf-8")
    print(f"Written {len(rows)} rows to {out_path}")
    print("Next: python scripts/import_reddit_zenodo.py --from-transformed")


if __name__ == "__main__":
    main()
