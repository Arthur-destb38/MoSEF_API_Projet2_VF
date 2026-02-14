#!/usr/bin/env python3
"""
Analyse de sentiment en lot sur tous les posts de la base (posts2).
Charge les posts par paquets, applique FinBERT ou CryptoBERT, met à jour sentiment_score en base.

Usage:
  # Analyser uniquement les posts sans sentiment (recommandé pour 36k+ posts)
  python scripts/analyze_posts_batch.py --model finbert --only-missing

  # Limiter à 1000 posts (test)
  python scripts/analyze_posts_batch.py --model finbert --only-missing --limit 1000

  # Tout ré-analyser (écrase les scores existants)
  python scripts/analyze_posts_batch.py --model cryptobert --limit 5000

  # Dry-run : afficher ce qui serait fait sans écrire en base
  python scripts/analyze_posts_batch.py --model finbert --only-missing --dry-run --limit 100
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Charger .env pour que DATABASE_URL / SUPABASE_* soient disponibles en CLI
try:
    from dotenv import load_dotenv
    _env_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env")
    load_dotenv(_env_path)
except ImportError:
    pass

from app.storage import get_all_posts, update_sentiment_scores, get_stats
from app.nlp import SentimentAnalyzer
from app.utils import clean_text


def main():
    parser = argparse.ArgumentParser(
        description="Analyse de sentiment en lot sur les posts de la base (posts2)"
    )
    parser.add_argument(
        "--model",
        choices=["finbert", "cryptobert"],
        default="finbert",
        help="Modèle NLP à utiliser (défaut: finbert)",
    )
    parser.add_argument(
        "--only-missing",
        action="store_true",
        help="Ne traiter que les posts dont sentiment_score est NULL",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Nombre max de posts à traiter (défaut: illimité)",
    )
    parser.add_argument(
        "--offset",
        type=int,
        default=0,
        help="Décalage pour pagination (défaut: 0)",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=500,
        help="Nombre de posts récupérés et mis à jour par lot (défaut: 500)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Ne pas écrire en base, seulement afficher les stats",
    )
    args = parser.parse_args()

    stats = get_stats()
    total_in_db = stats.get("total_posts", 0)
    print(f"Base: {total_in_db} posts (type: {stats.get('db_type', '?')})")
    if total_in_db == 0:
        print("Aucun post en base. Importez des données d'abord.")
        return

    print(f"Chargement du modèle {args.model}...")
    analyzer = SentimentAnalyzer(args.model)

    total_processed = 0
    total_updated = 0
    offset = args.offset
    chunk_size = args.chunk_size

    while True:
        limit_this_chunk = chunk_size
        if args.limit is not None and total_processed + chunk_size > args.limit:
            limit_this_chunk = args.limit - total_processed
        if limit_this_chunk <= 0:
            break

        posts = get_all_posts(
            limit=limit_this_chunk,
            offset=offset,
            only_without_sentiment=args.only_missing,
        )
        if not posts:
            break

        updates = []
        for p in posts:
            uid = p.get("uid")
            if not uid:
                continue
            text = clean_text((p.get("title") or "") + " " + (p.get("text") or ""))
            if len(text) < 10:
                score = 0.0
            else:
                out = analyzer.analyze(text)
                score = out["score"]
            updates.append((uid, score))

        if not args.dry_run and updates:
            n = update_sentiment_scores(updates)
            total_updated += n
        else:
            total_updated += len(updates)

        total_processed += len(posts)
        print(f"  Traité: {total_processed} | Mis à jour: {total_updated}")

        if len(posts) < limit_this_chunk:
            break
        if args.limit is not None and total_processed >= args.limit:
            break
        offset += len(posts)

    print(f"Terminé. Total traité: {total_processed}, scores mis à jour: {total_updated}")
    if args.dry_run:
        print("(Dry-run: aucune modification en base)")


if __name__ == "__main__":
    main()
