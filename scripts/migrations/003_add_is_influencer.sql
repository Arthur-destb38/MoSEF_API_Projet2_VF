-- Migration 003 : marquer les posts d'influenceurs (ex. BMC Twitter)
-- À exécuter sur la base cloud (Supabase SQL Editor) avant d'importer les données BMC.

ALTER TABLE posts2 ADD COLUMN IF NOT EXISTS is_influencer BOOLEAN DEFAULT FALSE;

-- Les lignes importées avec source = 'bmc_influencers' auront is_influencer = true.
-- Les autres sources (yahoo_finance, reddit, etc.) restent à false.
