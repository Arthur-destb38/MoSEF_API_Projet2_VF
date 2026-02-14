#!/usr/bin/env python3
"""
Télécharge les données BTC-USD (Yahoo Finance) de 2021-01-01 à aujourd'hui
et enregistre un CSV au format attendu par import_btc_usd.py (date, Open, High, Low, Close, Adj.Close, Volume).

Usage:
  pip install yfinance   # si nécessaire
  python scripts/download_btc_usd_2021_now.py

Sortie : data_externe/rediit_cryptocurrency_et_prixBTC/BTC-USD.csv (écrasé avec la plage 2021 → aujourd'hui).
Ensuite : python scripts/import_btc_usd.py
"""

import os
import sys
from datetime import date, datetime

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, PROJECT_ROOT)

OUTPUT_DIR = os.path.join(PROJECT_ROOT, "data_externe", "rediit_cryptocurrency_et_prixBTC")
OUTPUT_CSV = os.path.join(OUTPUT_DIR, "BTC-USD.csv")


def main():
    try:
        import yfinance as yf
    except ImportError:
        print("Ce script nécessite yfinance. Installez avec : pip install yfinance")
        sys.exit(1)

    start = date(2021, 1, 1)
    end = date.today()
    ticker = yf.Ticker("BTC-USD")
    df = ticker.history(start=start, end=end, interval="1d", auto_adjust=False)
    if df is None or df.empty:
        print("Aucune donnée récupérée (Yahoo Finance). Réessayez plus tard.")
        sys.exit(1)

    df = df.reset_index()
    df["date"] = df["Date"].dt.strftime("%Y-%m-%d")
    if "Adj Close" in df.columns:
        df["Adj.Close"] = df["Adj Close"]
    elif "Adj.Close" not in df.columns:
        df["Adj.Close"] = df["Close"]
    for c in ["Open", "High", "Low", "Close", "Volume"]:
        if c not in df.columns:
            print(f"Colonne manquante: {c}. Colonnes: {list(df.columns)}")
            sys.exit(1)
    export = df[["date", "Open", "High", "Low", "Close", "Adj.Close", "Volume"]].copy()

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    export.to_csv(OUTPUT_CSV, index=False)
    print(f"Écrit {len(export)} lignes dans {OUTPUT_CSV}")
    print(f"Période : {export['date'].min()} → {export['date'].max()}")
    print("Ensuite : python scripts/import_btc_usd.py")


if __name__ == "__main__":
    main()
