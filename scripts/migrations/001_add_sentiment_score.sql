-- Migration 001 : adapter la table posts2 pour le fichier Bitcoin News (Yahoo Finance)
-- Colonnes utilisées pour cet import : uid, source, text, created_utc (date), sentiment_score (score)
-- À exécuter sur la base cloud (Supabase → SQL Editor) avant le premier import.

-- Ajouter la colonne sentiment_score si elle n'existe pas encore
ALTER TABLE posts2 ADD COLUMN IF NOT EXISTS sentiment_score REAL;

-- Structure de référence de la table posts2 après migration :
--   uid            VARCHAR(255) PRIMARY KEY   -- clé unique (générée à l'import)
--   id             TEXT                       -- identifiant ligne (ex. index)
--   source         TEXT                       -- ex. 'yahoo_finance' pour Bitcoin News
--   method         TEXT                       -- ex. 'import_21_24'
--   title          TEXT                       -- optionnel (vide pour ce fichier)
--   text           TEXT                       -- contenu (Short Description)
--   score          INTEGER                    -- optionnel (0 pour ce fichier)
--   created_utc    TEXT                       -- date/heure (Date du CSV)
--   human_label    TEXT                       -- optionnel
--   author         TEXT                       -- optionnel
--   subreddit      TEXT                       -- optionnel
--   url            TEXT                       -- optionnel
--   num_comments   INTEGER                    -- optionnel
--   scraped_at     TIMESTAMP                  -- date d'import
--   sentiment_score REAL                      -- score de sentiment (Accurate Sentiments)
