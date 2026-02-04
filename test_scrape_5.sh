#!/usr/bin/env bash
# Test : 5 posts × 5 plateformes → base (Supabase ou SQLite). Rapide, pas de Selenium.
set -e
cd "$(dirname "$0")"
[ -d ".venv" ] && source .venv/bin/activate
[ -f ".env" ] && set -a && source .env && set +a
echo "Lancement du test (5 posts × 5 plateformes)..."
echo ""
python scripts/test_scrape_5.py
echo ""
echo "Terminé."
