#!/usr/bin/env python3
"""
Import BTC-USD.csv (daily OHLCV) into table btc_usd.
Requires direct DB connection (DATABASE_URL or SQLite). Does not work with Supabase REST only.

Before first run: execute scripts/migrations/002_create_btc_usd.sql on the cloud.
"""

import os
import sys
from datetime import datetime

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, PROJECT_ROOT)

from dotenv import load_dotenv
load_dotenv(os.path.join(PROJECT_ROOT, ".env"))

DATA_DIR = os.path.join(PROJECT_ROOT, "data_externe", "rediit_cryptocurrency_et_prixBTC")
CSV_PATH = os.path.join(DATA_DIR, "BTC-USD.csv")


def main():
    if not os.path.isfile(CSV_PATH):
        print(f"File not found: {CSV_PATH}")
        sys.exit(1)

    from app.storage import get_raw_connection
    import pandas as pd

    conn, db_type = get_raw_connection()
    if conn is None:
        print("BTC-USD import requires a direct DB connection (DATABASE_URL or SQLite).")
        print("Supabase REST-only mode does not support custom tables; use Supabase SQL Editor to run migration 002, then connect via DATABASE_URL.")
        sys.exit(1)

    df = pd.read_csv(CSV_PATH)
    for col in ["date", "Open", "High", "Low", "Close", "Adj.Close", "Volume"]:
        if col not in df.columns:
            print(f"Missing column: {col}. Columns: {list(df.columns)}")
            sys.exit(1)

    cur = conn.cursor()
    if db_type == "postgres":
        cur.execute("""
            CREATE TABLE IF NOT EXISTS btc_usd (
                date DATE PRIMARY KEY,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                adj_close REAL,
                volume REAL
            )
        """)
        conn.commit()
        inserted = 0
        for _, row in df.iterrows():
            try:
                d = str(row["date"])[:10]
                cur.execute("""
                    INSERT INTO btc_usd (date, open, high, low, close, adj_close, volume)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (date) DO UPDATE SET
                    open = EXCLUDED.open, high = EXCLUDED.high, low = EXCLUDED.low,
                    close = EXCLUDED.close, adj_close = EXCLUDED.adj_close, volume = EXCLUDED.volume
                """, (
                    d,
                    float(row["Open"]),
                    float(row["High"]),
                    float(row["Low"]),
                    float(row["Close"]),
                    float(row["Adj.Close"]),
                    float(row["Volume"]) if row.get("Volume") not in (None, "") else None,
                ))
                inserted += 1
            except Exception as e:
                print(f"Row {row.get('date')}: {e}")
        conn.commit()
    else:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS btc_usd (
                date TEXT PRIMARY KEY,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                adj_close REAL,
                volume REAL
            )
        """)
        conn.commit()
        inserted = 0
        for _, row in df.iterrows():
            try:
                d = str(row["date"])[:10]
                cur.execute("""
                    INSERT OR REPLACE INTO btc_usd (date, open, high, low, close, adj_close, volume)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    d,
                    float(row["Open"]),
                    float(row["High"]),
                    float(row["Low"]),
                    float(row["Close"]),
                    float(row["Adj.Close"]),
                    float(row["Volume"]) if row.get("Volume") not in (None, "") else None,
                ))
                inserted += 1
            except Exception as e:
                print(f"Row {row.get('date')}: {e}")
        conn.commit()

    conn.close()
    print(f"Inserted/updated {inserted} rows into btc_usd (db_type={db_type}).")
    if not df.empty:
        print(f"Plage dans le CSV : {str(df['date'].min())[:10]} → {str(df['date'].max())[:10]}")


if __name__ == "__main__":
    main()
