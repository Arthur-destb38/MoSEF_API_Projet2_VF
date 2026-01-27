#!/bin/bash
# Script de lancement Streamlit avec Poetry

cd "$(dirname "$0")"

# Vérifier si Poetry est installé (essayer les deux méthodes)
if ! command -v poetry &> /dev/null && ! python3 -m poetry --version &> /dev/null; then
    echo "❌ Poetry n'est pas installé."
    echo ""
    echo "Installe Poetry avec :"
    echo "  pip3 install --user poetry"
    echo ""
    exit 1
fi

# Utiliser python3 -m poetry si poetry n'est pas dans le PATH
if command -v poetry &> /dev/null; then
    POETRY_CMD="poetry"
else
    POETRY_CMD="python3 -m poetry"
fi

# Vérifier si les dépendances sont installées
if [ ! -d ".venv" ] && [ ! -f "poetry.lock" ]; then
    echo "📦 Installation des dépendances..."
    $POETRY_CMD install
fi

# Lancer Streamlit
echo "🚀 Lancement de Streamlit..."
$POETRY_CMD run streamlit run streamlit_app.py
