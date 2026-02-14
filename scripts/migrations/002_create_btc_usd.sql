-- Migration 002 : table des prix BTC quotidiens (Zenodo / Yahoo style)
-- À exécuter sur la base cloud (Supabase SQL Editor) avant d'importer BTC-USD.csv

CREATE TABLE IF NOT EXISTS btc_usd (
    date DATE PRIMARY KEY,
    open REAL,
    high REAL,
    low REAL,
    close REAL,
    adj_close REAL,
    volume REAL
);
