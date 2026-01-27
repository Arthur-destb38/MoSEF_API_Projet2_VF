"""
Crypto Sentiment Dashboard
Projet MoSEF 2024-2025
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, date
import sys
import os
from dotenv import load_dotenv

# Charger les variables d'environnement
load_dotenv()

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from app.scrapers import scrape_reddit, scrape_stocktwits, scrape_twitter, get_reddit_limits, get_stocktwits_limits, get_twitter_limits
from app.scrapers import scrape_telegram_simple, scrape_telegram_paginated, TELEGRAM_CHANNELS, get_telegram_limits
from app.scrapers import scrape_4chan_biz, get_chan4_limits
from app.scrapers import scrape_bitcointalk, get_bitcointalk_limits
from app.scrapers import scrape_github_discussions, get_github_limits
from app.nlp import load_finbert, load_cryptobert, analyze_finbert, analyze_cryptobert
from app.utils import clean_text
from app.prices import get_historical_prices, CryptoPrices
from app.storage import save_posts, get_all_posts, export_to_csv, export_to_json, get_stats

try:
    from econometrics import run_full_analysis, run_demo_analysis
    ECONO_OK = True
except ImportError:
    ECONO_OK = False

# ============ PAGE CONFIG ============

st.set_page_config(
    page_title="Crypto Sentiment",
    page_icon="◈",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ============ PROTECTION PAR MOT DE PASSE ============
# Si APP_PASSWORD ou DASHBOARD_PASSWORD est défini (dans .env ou variables d'env cloud),
# l'utilisateur doit entrer le mot de passe pour accéder au dashboard.
if "authenticated" not in st.session_state:
    st.session_state.authenticated = False

_app_password = os.environ.get("APP_PASSWORD") or os.environ.get("DASHBOARD_PASSWORD")
if not _app_password:
    try:
        _app_password = st.secrets.get("APP_PASSWORD") or st.secrets.get("DASHBOARD_PASSWORD")
    except Exception:
        pass

if not st.session_state.authenticated:
    if not _app_password:
        # Pas de mot de passe configuré (dev local) → accès libre
        st.session_state.authenticated = True
    else:
        # Afficher la page de connexion
        st.markdown("""
        <style>
            .login-box { max-width: 380px; margin: 4rem auto; padding: 2rem;
                background: linear-gradient(135deg, rgba(30,30,46,0.95) 0%, rgba(26,26,46,0.9) 100%);
                border: 1px solid rgba(99,102,241,0.3); border-radius: 16px; }
        </style>
        """, unsafe_allow_html=True)
        col1, col2, col3 = st.columns([1, 2, 1])
        with col2:
            st.markdown("### ◈ Crypto Sentiment")
            st.caption("Entrez le mot de passe pour accéder au dashboard.")
            with st.form("login_form"):
                pwd = st.text_input("Mot de passe", type="password", placeholder="••••••••", key="login_pwd")
                submitted = st.form_submit_button("Accéder")
            if submitted:
                if pwd and pwd == _app_password:
                    st.session_state.authenticated = True
                    st.rerun()
                else:
                    st.error("Mot de passe incorrect.")
            st.stop()

# ============ CUSTOM CSS ============

st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;500;600;700&family=Plus+Jakarta+Sans:wght@400;500;600;700&display=swap');
    
    .stApp {
        background: linear-gradient(135deg, #0a0a0f 0%, #1a1a2e 50%, #0f0f1a 100%);
    }
    
    #MainMenu, footer, header {visibility: hidden;}
    .block-container {padding-top: 2rem;}
    
    html, body, [class*="css"] {
        font-family: 'Plus Jakarta Sans', sans-serif;
    }
    
    h1, h2, h3 {
        font-family: 'Plus Jakarta Sans', sans-serif !important;
        font-weight: 700 !important;
    }
    
    section[data-testid="stSidebar"] {
        background: linear-gradient(180deg, #12121a 0%, #1a1a2e 100%);
        border-right: 1px solid rgba(99, 102, 241, 0.2);
    }
    
    section[data-testid="stSidebar"] .stRadio label {
        color: #a5b4fc !important;
    }
    
    .metric-card {
        background: linear-gradient(135deg, rgba(99, 102, 241, 0.1) 0%, rgba(139, 92, 246, 0.05) 100%);
        border: 1px solid rgba(99, 102, 241, 0.3);
        border-radius: 16px;
        padding: 24px;
        margin: 8px 0;
        backdrop-filter: blur(10px);
    }
    
    .metric-value {
        font-family: 'JetBrains Mono', monospace;
        font-size: 2.5rem;
        font-weight: 700;
        background: linear-gradient(135deg, #818cf8 0%, #c084fc 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        margin: 0;
    }
    
    .metric-label {
        color: #94a3b8;
        font-size: 0.875rem;
        text-transform: uppercase;
        letter-spacing: 0.1em;
        margin-bottom: 8px;
    }
    
    .metric-delta {
        font-family: 'JetBrains Mono', monospace;
        font-size: 0.875rem;
        margin-top: 8px;
    }
    
    .delta-positive { color: #4ade80; }
    .delta-negative { color: #f87171; }
    
    .dashboard-title {
        font-size: 2.5rem;
        font-weight: 700;
        background: linear-gradient(135deg, #e0e7ff 0%, #c7d2fe 50%, #a5b4fc 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        margin-bottom: 0.5rem;
    }
    
    .dashboard-subtitle {
        color: #64748b;
        font-size: 1rem;
        margin-bottom: 2rem;
    }
    
    .stButton > button {
        background: linear-gradient(135deg, #6366f1 0%, #8b5cf6 100%);
        color: white;
        border: none;
        border-radius: 12px;
        padding: 12px 24px;
        font-weight: 600;
        font-size: 1rem;
        transition: all 0.3s ease;
        box-shadow: 0 4px 15px rgba(99, 102, 241, 0.3);
    }
    
    .stButton > button:hover {
        transform: translateY(-2px);
        box-shadow: 0 6px 20px rgba(99, 102, 241, 0.4);
    }
    
    .stSelectbox > div > div,
    .stMultiSelect > div > div {
        background: rgba(30, 30, 46, 0.8);
        border: 1px solid rgba(99, 102, 241, 0.3);
        border-radius: 12px;
    }
    
    .stRadio > div {
        background: rgba(30, 30, 46, 0.5);
        border-radius: 12px;
        padding: 12px;
    }
    
    .stProgress > div > div {
        background: linear-gradient(90deg, #6366f1, #8b5cf6, #a855f7);
        border-radius: 10px;
    }
    
    .info-box {
        background: rgba(99, 102, 241, 0.1);
        border-left: 4px solid #6366f1;
        padding: 16px 20px;
        border-radius: 0 12px 12px 0;
        margin: 16px 0;
    }
    
    .warning-box {
        background: rgba(251, 191, 36, 0.1);
        border-left: 4px solid #fbbf24;
        padding: 16px 20px;
        border-radius: 0 12px 12px 0;
        margin: 16px 0;
    }
    
    .success-box {
        background: rgba(74, 222, 128, 0.1);
        border-left: 4px solid #4ade80;
        padding: 16px 20px;
        border-radius: 0 12px 12px 0;
        margin: 16px 0;
    }
    
    hr {
        border: none;
        height: 1px;
        background: linear-gradient(90deg, transparent, rgba(99, 102, 241, 0.3), transparent);
        margin: 2rem 0;
    }
    
    .stTabs [data-baseweb="tab-list"] {
        background: rgba(30, 30, 46, 0.5);
        border-radius: 12px;
        padding: 4px;
        gap: 4px;
    }
    
    .stTabs [data-baseweb="tab"] {
        border-radius: 8px;
        color: #94a3b8;
        font-weight: 500;
    }
    
    .stTabs [aria-selected="true"] {
        background: linear-gradient(135deg, #6366f1 0%, #8b5cf6 100%);
        color: white;
    }
    
    .stSlider > div > div > div {
        background: #6366f1;
    }
    
    .viewerBadge_container__1QSob {display: none;}
</style>
""", unsafe_allow_html=True)

# ============ CONFIG ============

CRYPTO_LIST = {
    "Bitcoin": {"id": "bitcoin", "sub": "Bitcoin", "stocktwits": "BTC.X", "icon": "₿"},
    "Ethereum": {"id": "ethereum", "sub": "ethereum", "stocktwits": "ETH.X", "icon": "Ξ"},
    "Solana": {"id": "solana", "sub": "solana", "stocktwits": "SOL.X", "icon": "◎"},
    "Cardano": {"id": "cardano", "sub": "cardano", "stocktwits": "ADA.X", "icon": "₳"},
    "Dogecoin": {"id": "dogecoin", "sub": "dogecoin", "stocktwits": "DOGE.X", "icon": "Ð"},
    "XRP": {"id": "ripple", "sub": "xrp", "stocktwits": "XRP.X", "icon": "✕"},
}

LIMITS = {
    "Reddit": {"HTTP": get_reddit_limits()["http"], "Selenium": get_reddit_limits()["selenium"]},
    "StockTwits": {"Selenium": get_stocktwits_limits()["selenium"]},  # 1000 posts max avec scroll amélioré
    "Twitter": {"Selenium": 100, "Login": 2000},  # Login = avec cookies (methode Jose)
    "Telegram": {"Simple": get_telegram_limits()["simple"], "Paginé": get_telegram_limits()["paginated"]},
    "4chan": {"HTTP": get_chan4_limits()["http"]},
    "Bitcointalk": {"HTTP": get_bitcointalk_limits()["http"]},
    "GitHub": {"API": get_github_limits()["api"]}
}

# ============ CACHE ============

@st.cache_resource
def get_finbert():
    return load_finbert()

@st.cache_resource
def get_cryptobert():
    return load_cryptobert()

@st.cache_data(ttl=300)
def get_prices():
    client = CryptoPrices()
    return client.get_multiple_prices(["bitcoin", "ethereum", "solana", "cardano", "dogecoin"])

def get_model(name):
    if name == "FinBERT":
        tok, mod = get_finbert()
        return tok, mod, analyze_finbert
    else:
        tok, mod = get_cryptobert()
        return tok, mod, analyze_cryptobert

def scrape_data(source, config, limit, method, telegram_channel=None, crypto_name=None,
                twitter_min_likes=None, twitter_start_date=None, twitter_end_date=None, twitter_sort="top"):
    if source == "Reddit":
        posts = scrape_reddit(config['sub'], limit, method=method.lower())
        save_posts(posts, source="reddit", method=method.lower())
        return posts
    elif source == "Twitter":
        query = crypto_name or config.get('sub', 'Bitcoin')
        try:
            posts = scrape_twitter(
                query, limit,
                min_likes=twitter_min_likes,
                start_date=twitter_start_date,
                end_date=twitter_end_date,
                sort_mode=twitter_sort
            )
            method_used = "selenium_login" if posts else "selenium"
            save_posts(posts, source="twitter", method=method_used)
            return posts
        except Exception as e:
            import traceback
            print(f"Erreur Twitter scraping: {e}")
            print(f"Traceback: {traceback.format_exc()}")
            return []
    elif source == "Telegram":
        if limit > 30:
            posts = scrape_telegram_paginated(telegram_channel, limit)
        else:
            posts = scrape_telegram_simple(telegram_channel, limit)
        for p in posts:
            p['title'] = p.get('text', '')
        save_posts(posts, source="telegram", method="http")
        return posts
    elif source == "4chan":
        query = crypto_name or config.get('sub', 'crypto').lower()
        posts = scrape_4chan_biz(query, limit)
        save_posts(posts, source="4chan", method="http")
        return posts
    elif source == "Bitcointalk":
        query = crypto_name or config.get('sub', 'crypto').lower()
        posts = scrape_bitcointalk(query, limit)
        save_posts(posts, source="bitcointalk", method="http")
        return posts
    elif source == "GitHub":
        query = crypto_name or config.get('sub', 'crypto').lower()
        posts = scrape_github_discussions(query, limit)
        save_posts(posts, source="github", method="api")
        return posts
    else:
        posts = scrape_stocktwits(config['stocktwits'], limit)
        save_posts(posts, source="stocktwits", method="selenium")
        return posts

# ============ COMPONENTS ============

def render_metric_card(label, value, delta=None, delta_type="neutral"):
    delta_html = ""
    if delta:
        delta_class = "delta-positive" if delta_type == "positive" else "delta-negative" if delta_type == "negative" else ""
        delta_html = f'<div class="metric-delta {delta_class}">{delta}</div>'
    
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-label">{label}</div>
        <div class="metric-value">{value}</div>
        {delta_html}
    </div>
    """, unsafe_allow_html=True)

def render_header():
    st.markdown("""
    <div style="text-align: center; padding: 1rem 0 2rem 0;">
        <h1 class="dashboard-title">Crypto Sentiment Dashboard</h1>
        <p class="dashboard-subtitle">Analyse en temps réel du sentiment crypto • Reddit & StockTwits • FinBERT & CryptoBERT</p>
    </div>
    """, unsafe_allow_html=True)

# ============ PAGES ============

def page_dashboard():
    render_header()
    
    try:
        prices = get_prices()
        if prices:
            cols = st.columns(len(prices))
            for i, (name, data) in enumerate(prices.items()):
                with cols[i]:
                    change = data.get('change_24h', 0)
                    delta_type = "positive" if change > 0 else "negative"
                    # Format prix selon la valeur
                    price = data['price']
                    if price >= 1000:
                        price_str = f"${price:,.0f}"
                    elif price >= 1:
                        price_str = f"${price:,.2f}"
                    else:
                        price_str = f"${price:.4f}"
                    render_metric_card(name.upper(), price_str, f"{change:+.2f}%", delta_type)
    except:
        st.info("Prix non disponibles")
    
    st.markdown("---")
    
    col1, col2 = st.columns([1, 2])
    
    with col1:
        st.markdown("### Configuration")
        
        crypto = st.selectbox("Crypto", list(CRYPTO_LIST.keys()), key="dash_crypto")
        config = CRYPTO_LIST[crypto]
        
        source = st.radio("Source", ["Reddit", "StockTwits", "Twitter", "Telegram", "4chan", "Bitcointalk", "GitHub"], horizontal=True, key="dash_source")
        
        if source == "Reddit":
            method = st.radio("Méthode", ["HTTP", "Selenium"], horizontal=True, key="dash_method")
            max_limit = LIMITS["Reddit"][method]
            telegram_channel = None
        elif source == "Twitter":
            method = st.radio("Mode", ["Login", "Selenium"], horizontal=True, key="dash_tw_method",
                             help="Login: recherche avancee (2000 tweets) | Selenium: profils publics (100 tweets)")
            max_limit = LIMITS["Twitter"][method]
            telegram_channel = None
            
            # Options avancees Twitter (methode Jose)
            with st.expander("Options Twitter avancees"):
                tw_sort = st.radio("Tri", ["top", "live"], horizontal=True, key="dash_tw_sort",
                                  help="top: populaires | live: recents")
                tw_min_likes = st.number_input("Min likes", min_value=0, value=0, key="dash_tw_likes",
                                              help="0 = pas de filtre")
                col_d1, col_d2 = st.columns(2)
                with col_d1:
                    tw_start = st.date_input("Date debut", value=None, key="dash_tw_start")
                with col_d2:
                    tw_end = st.date_input("Date fin", value=None, key="dash_tw_end")
            
            st.markdown("""
            <div class="info-box">
                <strong>Twitter/X</strong> — instable depuis 2023<br>
                <small>X exige le login, détecte Selenium et change son API toutes les 2–4 sem.
                Si login échoue ou sans identifiants: <b>Nitter</b> (fallback) puis profils publics.
                Mettez <code>TWITTER_USERNAME</code> et <code>TWITTER_PASSWORD</code> dans <code>.env</code> pour tenter le login.</small>
            </div>
            """, unsafe_allow_html=True)
        elif source == "Telegram":
            method = st.radio("Méthode", ["Simple", "Paginé"], horizontal=True, key="dash_method_tg")
            max_limit = LIMITS["Telegram"][method]
            telegram_channel = st.selectbox("Channel Telegram", list(TELEGRAM_CHANNELS.keys()),
                                           format_func=lambda x: f"{x} - {TELEGRAM_CHANNELS[x]}", key="dash_tg_channel")
            st.markdown(f"""
            <div class="info-box">
                <strong>Channel:</strong> @{telegram_channel}<br>
                <small>Scraping public sans API</small>
            </div>
            """, unsafe_allow_html=True)
        elif source == "4chan":
            method = "HTTP"
            max_limit = LIMITS["4chan"]["HTTP"]
            telegram_channel = None
            st.markdown("""
            <div class="success-box">
                <strong>4chan /biz/</strong> — Très actif pour crypto<br>
                <small>Scraping rapide via API, pas de login requis. Discussions anonymes sur /biz/.</small>
            </div>
            """, unsafe_allow_html=True)
        elif source == "Bitcointalk":
            method = "HTTP"
            max_limit = LIMITS["Bitcointalk"]["HTTP"]
            telegram_channel = None
            st.markdown("""
            <div class="success-box">
                <strong>Bitcointalk</strong> — Forum crypto historique<br>
                <small>Scraping via HTTP, pas de login requis. Discussions longues et détaillées sur crypto.</small>
            </div>
            """, unsafe_allow_html=True)
        elif source == "GitHub":
            method = "API"
            max_limit = LIMITS["GitHub"]["API"]
            telegram_channel = None
            st.markdown("""
            <div class="success-box">
                <strong>GitHub</strong> — Issues/Discussions projets crypto<br>
                <small>API officielle GitHub (gratuite). Discussions techniques sur projets Bitcoin, Ethereum, etc.</small>
            </div>
            """, unsafe_allow_html=True)
        else:
            method = "Selenium"
            max_limit = LIMITS["StockTwits"]["Selenium"]
            telegram_channel = None
            st.markdown("""
            <div class="success-box">
                <strong>Labels humains disponibles</strong><br>
                <small>StockTwits fournit des labels Bullish/Bearish</small>
            </div>
            """, unsafe_allow_html=True)
        
        model = st.radio("Modèle NLP", ["FinBERT", "CryptoBERT"], horizontal=True, key="dash_model")
        limit = st.slider("Nombre de posts", 20, max_limit, min(50, max_limit), key="dash_limit")
        
        st.markdown(f"""
        <div class="info-box">
            <strong>Limite max:</strong> {max_limit} posts<br>
            <small>Pour éviter les bans</small>
        </div>
        """, unsafe_allow_html=True)
        
        analyze = st.button("Analyser", use_container_width=True, key="dash_analyze")
    
    # Build Twitter options if applicable
    twitter_opts = None
    if source == "Twitter":
        twitter_opts = {
            'sort': tw_sort if 'tw_sort' in dir() else 'top',
            'min_likes': tw_min_likes if tw_min_likes > 0 else None,
            'start_date': tw_start.strftime('%Y-%m-%d') if tw_start else None,
            'end_date': tw_end.strftime('%Y-%m-%d') if tw_end else None
        }
    
    with col2:
        if analyze:
            run_analysis(crypto, config, source, method, model, limit, telegram_channel, crypto, twitter_opts)
        else:
            st.markdown("""
            <div style="
                display: flex;
                flex-direction: column;
                align-items: center;
                justify-content: center;
                height: 400px;
                background: rgba(30, 30, 46, 0.3);
                border-radius: 16px;
                border: 1px dashed rgba(99, 102, 241, 0.3);
            ">
                <div style="color: #64748b; font-size: 1.1rem;">Configure et lance une analyse</div>
                <div style="color: #475569; font-size: 0.9rem; margin-top: 0.5rem;">Les résultats apparaîtront ici</div>
            </div>
            """, unsafe_allow_html=True)


def run_analysis(crypto, config, source, method, model, limit, telegram_channel=None, crypto_name=None,
                 twitter_opts=None):
    with st.spinner(f"Scraping {source}..."):
        tw_opts = twitter_opts or {}
        posts = scrape_data(source, config, limit, method, telegram_channel, crypto_name,
                           twitter_min_likes=tw_opts.get('min_likes'),
                           twitter_start_date=tw_opts.get('start_date'),
                           twitter_end_date=tw_opts.get('end_date'),
                           twitter_sort=tw_opts.get('sort', 'top'))
    
    if not posts:
        st.error("Aucun post récupéré")
        return
    
    # Afficher confirmation de sauvegarde
    st.success(f"{len(posts)} posts sauvegardés dans la base de données")
    
    with st.spinner(f"Analyse avec {model}..."):
        tokenizer, mod, analyze_fn = get_model(model)
        
        results = []
        progress = st.progress(0)
        
        for i, post in enumerate(posts):
            text = clean_text(post["title"] + " " + post.get("text", ""))
            if text and len(text) > 5:
                sent = analyze_fn(text, tokenizer, mod)
            else:
                sent = {"score": 0, "label": "Neutral"}
            
            results.append({
                **post,
                "sentiment_score": sent["score"],
                "sentiment_label": sent["label"]
            })
            progress.progress((i + 1) / len(posts))
    
    st.session_state['results'] = results
    st.session_state['crypto'] = crypto
    st.session_state['config'] = config
    
    display_results(results, source, model)


def display_results(results, source, model):
    scores = [r["sentiment_score"] for r in results]
    labels = {"Bullish": 0, "Bearish": 0, "Neutral": 0}
    for r in results:
        labels[r["sentiment_label"]] += 1
    
    avg_score = np.mean(scores)
    
    st.markdown("### Résultats")
    
    cols = st.columns(4)
    with cols[0]:
        render_metric_card("Posts analysés", len(results))
    with cols[1]:
        delta_type = "positive" if avg_score > 0 else "negative"
        render_metric_card("Sentiment moyen", f"{avg_score:+.3f}", delta_type=delta_type)
    with cols[2]:
        render_metric_card("Bullish", labels['Bullish'], f"{labels['Bullish']/len(results)*100:.0f}%", "positive")
    with cols[3]:
        render_metric_card("Bearish", labels['Bearish'], f"{labels['Bearish']/len(results)*100:.0f}%", "negative")
    
    labeled = [r for r in results if r.get("human_label")]
    if labeled:
        correct = sum(1 for r in labeled if r["sentiment_label"] == r["human_label"])
        acc = correct / len(labeled) * 100
        st.markdown(f"""
        <div class="success-box">
            <strong>Accuracy vs labels humains: {acc:.1f}%</strong><br>
            <small>{correct}/{len(labeled)} prédictions correctes</small>
        </div>
        """, unsafe_allow_html=True)
    
    col1, col2 = st.columns(2)
    
    with col1:
        fig = go.Figure(data=[go.Pie(
            labels=list(labels.keys()),
            values=list(labels.values()),
            hole=0.6,
            marker=dict(colors=['#4ade80', '#f87171', '#64748b']),
            textinfo='label+percent',
            textfont=dict(size=14, color='white')
        )])
        fig.update_layout(
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)',
            font=dict(color='white'),
            showlegend=False,
            margin=dict(t=20, b=20, l=20, r=20),
            height=300
        )
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        fig = go.Figure(data=[go.Histogram(
            x=scores,
            nbinsx=30,
            marker=dict(color='rgba(99, 102, 241, 0.7)', line=dict(color='#818cf8', width=1))
        )])
        fig.add_vline(x=0, line_dash="dash", line_color="#64748b")
        fig.add_vline(x=avg_score, line_dash="solid", line_color="#a855f7", line_width=2)
        fig.update_layout(
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)',
            font=dict(color='white'),
            xaxis=dict(gridcolor='rgba(255,255,255,0.1)', title="Score"),
            yaxis=dict(gridcolor='rgba(255,255,255,0.1)', title="Count"),
            margin=dict(t=20, b=40, l=40, r=20),
            height=300
        )
        st.plotly_chart(fig, use_container_width=True)
    
    st.markdown("### Détail des posts")
    
    df = pd.DataFrame([{
        "Texte": r["title"][:60] + "..." if len(r["title"]) > 60 else r["title"],
        "Score": round(r["sentiment_score"], 3),
        "Prédiction": r["sentiment_label"],
        "Label": r.get("human_label", "-")
    } for r in results])
    
    st.dataframe(df, use_container_width=True, height=300)
    
    col1, col2 = st.columns(2)
    with col1:
        st.download_button("Télécharger CSV", df.to_csv(index=False), "sentiment.csv", use_container_width=True)


def page_compare():
    render_header()
    st.markdown("### Comparaison FinBERT vs CryptoBERT")
    
    col1, col2 = st.columns([1, 3])
    
    with col1:
        crypto = st.selectbox("Crypto", list(CRYPTO_LIST.keys()), key="cmp_crypto")
        config = CRYPTO_LIST[crypto]
        
        source = st.radio("Source", ["Reddit", "StockTwits", "Twitter", "Telegram", "4chan", "Bitcointalk", "GitHub"], key="cmp_source")
        
        telegram_channel = None
        if source == "Reddit":
            method = st.radio("Méthode", ["HTTP", "Selenium"], key="cmp_method")
            max_limit = LIMITS["Reddit"][method]
        elif source == "Twitter":
            method = "Selenium"
            max_limit = LIMITS["Twitter"]["Selenium"]
        elif source == "Telegram":
            method = st.radio("Méthode", ["Simple", "Paginé"], key="cmp_method_tg")
            max_limit = LIMITS["Telegram"][method]
            telegram_channel = st.selectbox("Channel", list(TELEGRAM_CHANNELS.keys()),
                                           format_func=lambda x: f"{x}", key="cmp_tg_channel")
        else:
            method = "Selenium"
            max_limit = LIMITS["StockTwits"]["Selenium"]
        
        limit = st.slider("Posts", 20, max_limit, min(50, max_limit), key="cmp_limit")
        run = st.button("Comparer", use_container_width=True, key="cmp_run")
    
    with col2:
        if run:
            with st.spinner("Scraping..."):
                posts = scrape_data(source, config, limit, method, telegram_channel, crypto)
            
            if not posts:
                st.error("Aucun post")
                return
            
            with st.spinner("Analyse..."):
                fin_tok, fin_mod, _ = get_model("FinBERT")
                cry_tok, cry_mod, _ = get_model("CryptoBERT")
                
                results = []
                progress = st.progress(0)
                
                for i, post in enumerate(posts):
                    text = clean_text(post["title"])
                    if not text:
                        continue
                    
                    fin = analyze_finbert(text, fin_tok, fin_mod)
                    cry = analyze_cryptobert(text, cry_tok, cry_mod)
                    
                    results.append({
                        "text": text[:50],
                        "human_label": post.get("human_label"),
                        "finbert_score": fin["score"],
                        "finbert_label": fin["label"],
                        "cryptobert_score": cry["score"],
                        "cryptobert_label": cry["label"]
                    })
                    progress.progress((i + 1) / len(posts))
            
            df = pd.DataFrame(results)
            
            cols = st.columns(2)
            with cols[0]:
                render_metric_card("FinBERT", f"{df['finbert_score'].mean():+.3f}")
            with cols[1]:
                render_metric_card("CryptoBERT", f"{df['cryptobert_score'].mean():+.3f}")
            
            labeled = df[df['human_label'].notna()]
            if len(labeled) > 0:
                fin_acc = (labeled['finbert_label'] == labeled['human_label']).mean() * 100
                cry_acc = (labeled['cryptobert_label'] == labeled['human_label']).mean() * 100
                
                st.markdown("### Accuracy vs labels humains")
                cols = st.columns(2)
                with cols[0]:
                    render_metric_card("FinBERT", f"{fin_acc:.1f}%")
                with cols[1]:
                    render_metric_card("CryptoBERT", f"{cry_acc:.1f}%")
                
                winner = "CryptoBERT" if cry_acc > fin_acc else "FinBERT"
                diff = abs(cry_acc - fin_acc)
                st.markdown(f"""
                <div class="success-box">
                    <strong>{winner} gagne!</strong> (+{diff:.1f}%)
                </div>
                """, unsafe_allow_html=True)
            
            fig = px.scatter(df, x='finbert_score', y='cryptobert_score', color_discrete_sequence=['#8b5cf6'])
            fig.add_hline(y=0, line_dash="dash", line_color="#64748b")
            fig.add_vline(x=0, line_dash="dash", line_color="#64748b")
            fig.update_layout(
                paper_bgcolor='rgba(0,0,0,0)',
                plot_bgcolor='rgba(0,0,0,0)',
                font=dict(color='white'),
                xaxis=dict(gridcolor='rgba(255,255,255,0.1)', title="FinBERT"),
                yaxis=dict(gridcolor='rgba(255,255,255,0.1)', title="CryptoBERT"),
                height=400
            )
            st.plotly_chart(fig, use_container_width=True)


def page_multi():
    render_header()
    st.markdown("### Analyse Multi-Crypto Comparative")
    
    col1, col2 = st.columns([1, 3])
    
    with col1:
        selected = st.multiselect("Cryptos", list(CRYPTO_LIST.keys()),
                                  default=["Bitcoin", "Ethereum", "Solana"], key="multi_crypto")
        
        source = st.radio("Source", ["Reddit", "StockTwits", "Twitter", "Telegram", "4chan", "Bitcointalk", "GitHub"], key="multi_source")
        
        telegram_channel = None
        if source == "Reddit":
            method = st.radio("Méthode", ["HTTP", "Selenium"], key="multi_method")
            max_limit = LIMITS["Reddit"][method]
        elif source == "Twitter":
            method = "Selenium"
            max_limit = LIMITS["Twitter"]["Selenium"]
        elif source == "Telegram":
            method = st.radio("Méthode", ["Simple", "Paginé"], key="multi_method_tg")
            max_limit = LIMITS["Telegram"][method]
            telegram_channel = st.selectbox("Channel", list(TELEGRAM_CHANNELS.keys()),
                                           format_func=lambda x: f"{x}", key="multi_tg_channel")
        elif source == "4chan":
            method = "HTTP"
            max_limit = LIMITS["4chan"]["HTTP"]
            telegram_channel = None
        elif source == "Bitcointalk":
            method = "HTTP"
            max_limit = LIMITS["Bitcointalk"]["HTTP"]
            telegram_channel = None
        elif source == "GitHub":
            method = "API"
            max_limit = LIMITS["GitHub"]["API"]
            telegram_channel = None
        else:
            method = "Selenium"
            max_limit = LIMITS["StockTwits"]["Selenium"]
        
        model = st.radio("Modèle", ["FinBERT", "CryptoBERT"], key="multi_model")
        
        # Limite adaptée au nombre de cryptos (éviter les bans)
        nb_cryptos = len(selected) if selected else 1
        safe_limit = min(max_limit, max(20, 200 // nb_cryptos))  # Répartir pour éviter ban
        
        limit = st.slider("Posts/crypto", 20, max_limit, safe_limit, key="multi_limit")
        
        # Warning si risque de ban
        total_posts = limit * nb_cryptos
        st.markdown(f"""
        <div class="info-box">
            <strong>Total estimé:</strong> {total_posts} posts<br>
            <small>Limite {source}: {max_limit}/crypto</small>
        </div>
        """, unsafe_allow_html=True)
        
        if total_posts > 500:
            st.markdown("""
            <div class="warning-box">
                <strong>Attention</strong><br>
                <small>Beaucoup de posts = risque de ban. Réduire si erreur.</small>
            </div>
            """, unsafe_allow_html=True)
        
        run = st.button("Analyser", use_container_width=True, key="multi_run")
    
    with col2:
        if run and selected:
            tokenizer, mod, analyze_fn = get_model(model)
            
            all_results = []
            all_posts_data = {}  # Stocker tous les posts pour détails
            progress = st.progress(0)
            status = st.empty()
            
            for i, name in enumerate(selected):
                status.text(f"Scraping {name}...")
                config = CRYPTO_LIST[name]
                posts = scrape_data(source, config, limit, method, telegram_channel, name)
                
                if posts:
                    scores = []
                    labels = {"Bullish": 0, "Bearish": 0, "Neutral": 0}
                    post_details = []
                    correct = 0
                    labeled_count = 0
                    
                    for post in posts:
                        text = clean_text(post["title"])
                        if text:
                            s = analyze_fn(text, tokenizer, mod)
                            scores.append(s["score"])
                            labels[s["label"]] += 1
                            
                            # Accuracy si StockTwits
                            if post.get("human_label"):
                                labeled_count += 1
                                if s["label"] == post["human_label"]:
                                    correct += 1
                            
                            post_details.append({
                                "text": text[:50],
                                "score": s["score"],
                                "label": s["label"],
                                "human_label": post.get("human_label")
                            })
                    
                    accuracy = round(correct / labeled_count * 100, 1) if labeled_count > 0 else None
                    
                    all_results.append({
                        "Crypto": name,
                        "Posts": len(scores),
                        "Sentiment": round(np.mean(scores), 4) if scores else 0,
                        "Std": round(np.std(scores), 4) if scores else 0,
                        "Bullish": labels["Bullish"],
                        "Bearish": labels["Bearish"],
                        "Neutral": labels["Neutral"],
                        "Bullish%": round(labels["Bullish"] / len(scores) * 100, 1) if scores else 0,
                        "Accuracy": accuracy
                    })
                    all_posts_data[name] = post_details
                
                progress.progress((i + 1) / len(selected))
            
            status.empty()
            
            if not all_results:
                st.error("Aucun résultat")
                return
            
            df = pd.DataFrame(all_results)
            
            # === METRIQUES GLOBALES ===
            st.markdown("### Vue d'ensemble")
            
            best_crypto = df.loc[df["Sentiment"].idxmax(), "Crypto"]
            worst_crypto = df.loc[df["Sentiment"].idxmin(), "Crypto"]
            avg_sentiment = df["Sentiment"].mean()
            
            cols = st.columns(4)
            with cols[0]:
                render_metric_card("Cryptos analysées", len(df))
            with cols[1]:
                render_metric_card("Sentiment moyen", f"{avg_sentiment:+.3f}")
            with cols[2]:
                render_metric_card("Plus haussier", best_crypto, f"{df.loc[df['Crypto']==best_crypto, 'Sentiment'].values[0]:+.3f}", "positive")
            with cols[3]:
                render_metric_card("Plus baissier", worst_crypto, f"{df.loc[df['Crypto']==worst_crypto, 'Sentiment'].values[0]:+.3f}", "negative")
            
            # === GRAPHIQUE COMPARATIF ===
            st.markdown("### Comparaison des sentiments")
            
            fig = go.Figure(data=[go.Bar(
                x=df["Crypto"],
                y=df["Sentiment"],
                marker=dict(
                    color=df["Sentiment"],
                    colorscale=[[0, '#f87171'], [0.5, '#64748b'], [1, '#4ade80']],
                    cmin=-0.5,
                    cmax=0.5
                ),
                text=[f"{s:+.3f}" for s in df["Sentiment"]],
                textposition='outside',
                textfont=dict(color='white')
            )])
            fig.add_hline(y=0, line_dash="dash", line_color="#64748b")
            fig.update_layout(
                paper_bgcolor='rgba(0,0,0,0)',
                plot_bgcolor='rgba(0,0,0,0)',
                font=dict(color='white'),
                xaxis=dict(gridcolor='rgba(255,255,255,0.1)'),
                yaxis=dict(gridcolor='rgba(255,255,255,0.1)', title="Sentiment Score"),
                height=350,
                margin=dict(t=30, b=30)
            )
            st.plotly_chart(fig, use_container_width=True)
            
            # === DISTRIBUTION PAR CRYPTO ===
            st.markdown("### Distribution Bullish/Bearish/Neutral")
            
            cols = st.columns(min(len(df), 3))
            for i, row in df.iterrows():
                with cols[i % 3]:
                    fig = go.Figure(data=[go.Pie(
                        labels=["Bullish", "Bearish", "Neutral"],
                        values=[row["Bullish"], row["Bearish"], row["Neutral"]],
                        hole=0.5,
                        marker=dict(colors=['#4ade80', '#f87171', '#64748b']),
                        textinfo='percent',
                        textfont=dict(size=11, color='white')
                    )])
                    fig.update_layout(
                        paper_bgcolor='rgba(0,0,0,0)',
                        plot_bgcolor='rgba(0,0,0,0)',
                        font=dict(color='white'),
                        showlegend=False,
                        title=dict(text=row["Crypto"], font=dict(size=14)),
                        margin=dict(t=40, b=20, l=20, r=20),
                        height=200
                    )
                    st.plotly_chart(fig, use_container_width=True)
            
            # === TABLEAU DETAILLE ===
            st.markdown("### Détails par crypto")
            
            display_df = df[["Crypto", "Posts", "Sentiment", "Std", "Bullish%", "Accuracy"]].copy()
            display_df.columns = ["Crypto", "Posts", "Sentiment", "Écart-type", "% Bullish", "Accuracy"]
            display_df["Accuracy"] = display_df["Accuracy"].apply(lambda x: f"{x}%" if x else "-")
            
            st.dataframe(display_df, use_container_width=True)
            
            # === DETAILS PAR CRYPTO (expandable) ===
            st.markdown("### Analyse détaillée par crypto")
            
            for name in selected:
                if name in all_posts_data:
                    with st.expander(f"{name} - {len(all_posts_data[name])} posts"):
                        crypto_df = pd.DataFrame(all_posts_data[name])
                        
                        # Stats
                        row = df[df["Crypto"] == name].iloc[0]
                        c1, c2, c3, c4 = st.columns(4)
                        c1.metric("Sentiment", f"{row['Sentiment']:+.3f}")
                        c2.metric("Bullish", f"{row['Bullish%']}%")
                        c3.metric("Bearish", f"{100 - row['Bullish%'] - (row['Neutral']/row['Posts']*100):.1f}%")
                        if row["Accuracy"]:
                            c4.metric("Accuracy", f"{row['Accuracy']}%")
                        
                        # Histogramme
                        fig = go.Figure(data=[go.Histogram(
                            x=crypto_df["score"],
                            nbinsx=20,
                            marker=dict(color='rgba(99, 102, 241, 0.7)')
                        )])
                        fig.add_vline(x=0, line_dash="dash", line_color="#64748b")
                        fig.update_layout(
                            paper_bgcolor='rgba(0,0,0,0)',
                            plot_bgcolor='rgba(0,0,0,0)',
                            font=dict(color='white'),
                            xaxis=dict(gridcolor='rgba(255,255,255,0.1)', title="Score"),
                            yaxis=dict(gridcolor='rgba(255,255,255,0.1)'),
                            height=200,
                            margin=dict(t=10, b=30)
                        )
                        st.plotly_chart(fig, use_container_width=True)
                        
                        # Table posts
                        st.dataframe(crypto_df, use_container_width=True, height=200)
            
            # === EXPORT ===
            st.markdown("---")
            col1, col2 = st.columns(2)
            with col1:
                st.download_button("Télécharger résumé CSV", df.to_csv(index=False), "multi_crypto_summary.csv", use_container_width=True)


def page_econometrie():
    render_header()
    st.markdown("### Analyse Économétrique")
    
    if not ECONO_OK:
        st.error("Module econometrics.py non disponible")
        return
    
    col1, col2 = st.columns([1, 3])
    
    with col1:
        # Mode selection
        mode = st.radio("Mode", ["Demo", "Données réelles"], key="eco_mode")
        
        if mode == "Demo":
            st.markdown("""
            <div class="info-box">
                <strong>Mode Demo</strong><br>
                <small>Données sentiment simulées sur 60 jours pour illustrer l'analyse</small>
            </div>
            """, unsafe_allow_html=True)
            crypto = st.selectbox("Crypto", list(CRYPTO_LIST.keys()), key="eco_crypto")
            config = CRYPTO_LIST[crypto]
        else:
            if 'results' not in st.session_state:
                st.markdown("""
                <div class="warning-box">
                    <strong>Aucune donnée</strong><br>
                    Lance d'abord une analyse sur le Dashboard
                </div>
                """, unsafe_allow_html=True)
                crypto = None
                config = None
            else:
                results = st.session_state['results']
                crypto = st.session_state.get('crypto', 'Bitcoin')
                config = st.session_state.get('config', CRYPTO_LIST['Bitcoin'])
                st.info(f"{len(results)} posts ({crypto})")
        
        days = st.slider("Jours historiques", 30, 90, 60)
        max_lag = st.slider("Lag max", 3, 10, 5)
        run = st.button("Analyser", use_container_width=True)
    
    with col2:
        if run:
            if mode == "Demo":
                with st.spinner("Analyse économétrique (demo)..."):
                    output = run_demo_analysis(config['id'], days, max_lag)
            else:
                if 'results' not in st.session_state:
                    st.error("Pas de données. Lance une analyse sur le Dashboard d'abord.")
                    return
                
                results = st.session_state['results']
                posts = [{"title": r.get("title", ""), "created_utc": r.get("created_utc")} for r in results]
                sent = [{"score": r.get("sentiment_score", 0), "label": r.get("sentiment_label", "Neutral")} for r in results]
                
                with st.spinner("Analyse économétrique..."):
                    output = run_full_analysis(posts, sent, config['id'], days, max_lag)
            
            if output["status"] == "error":
                st.error(output.get("error"))
                return
            
            # Badge mode demo
            if output.get("mode") == "demo":
                st.markdown("""
                <div style="background: rgba(139, 92, 246, 0.2); border: 1px solid #8b5cf6; padding: 10px; border-radius: 8px; margin-bottom: 16px; text-align: center;">
                    <strong style="color: #c4b5fd;">MODE DEMO</strong> - Données sentiment simulées
                </div>
                """, unsafe_allow_html=True)
            
            # Info données
            info = output.get("data_info", {})
            st.markdown(f"**Période:** {info.get('date_debut', 'N/A')} → {info.get('date_fin', 'N/A')} ({info.get('jours_merged', 0)} jours)")
            
            st.markdown("#### Tests de Stationnarité (ADF)")
            adf = output["adf_tests"]
            cols = st.columns(2)
            with cols[0]:
                s = adf.get("sentiment", {})
                status = "Stationnaire" if s.get("stationary") else "Non stationnaire"
                render_metric_card("Sentiment", status, f"p={s.get('pvalue', 'N/A')}")
            with cols[1]:
                r = adf.get("returns", {})
                status = "Stationnaire" if r.get("stationary") else "Non stationnaire"
                render_metric_card("Returns", status, f"p={r.get('pvalue', 'N/A')}")
            
            st.markdown("#### Causalité de Granger")
            granger = output.get("granger", {})
            if "error" not in granger:
                cols = st.columns(2)
                with cols[0]:
                    s2r = granger.get("sentiment_to_returns", {})
                    status = "Significatif" if s2r.get("significant") else "Non significatif"
                    render_metric_card("Sentiment → Prix", status, f"lag={s2r.get('best_lag', 'N/A')}")
                with cols[1]:
                    r2s = granger.get("returns_to_sentiment", {})
                    status = "Significatif" if r2s.get("significant") else "Non significatif"
                    render_metric_card("Prix → Sentiment", status, f"lag={r2s.get('best_lag', 'N/A')}")
            else:
                st.warning(f"Granger: {granger.get('error')}")
            
            # Cross-correlation
            cross = output.get("cross_corr", {})
            if cross.get("best_lag") is not None:
                st.markdown("#### Corrélation croisée")
                best_lag = cross.get("best_lag")
                best_corr = cross.get("best_correlation")
                if best_lag > 0:
                    interp = f"Sentiment précède les prix de {best_lag} jour(s)"
                elif best_lag < 0:
                    interp = f"Prix précèdent le sentiment de {-best_lag} jour(s)"
                else:
                    interp = "Relation contemporaine"
                render_metric_card("Meilleure corrélation", f"r = {best_corr}", interp)
            
            # Graphique sentiment vs returns
            if "merged_data" in output:
                merged = output["merged_data"]
                st.markdown("#### Évolution Sentiment vs Returns")
                
                fig = go.Figure()
                fig.add_trace(go.Scatter(
                    x=merged['date'], y=merged['sentiment_mean'],
                    name='Sentiment', line=dict(color='#8b5cf6', width=2)
                ))
                fig.add_trace(go.Scatter(
                    x=merged['date'], y=merged['log_return'] * 10,
                    name='Returns (x10)', line=dict(color='#4ade80', width=2)
                ))
                fig.update_layout(
                    paper_bgcolor='rgba(0,0,0,0)',
                    plot_bgcolor='rgba(0,0,0,0)',
                    font=dict(color='white'),
                    xaxis=dict(gridcolor='rgba(255,255,255,0.1)'),
                    yaxis=dict(gridcolor='rgba(255,255,255,0.1)'),
                    height=300,
                    margin=dict(t=20, b=40, l=40, r=20),
                    legend=dict(orientation="h", yanchor="bottom", y=1.02)
                )
                st.plotly_chart(fig, use_container_width=True)
            
            st.markdown("#### Conclusion")
            conclusion_text = output.get("conclusion", "Analyse terminée").replace("\n", "<br>")
            st.markdown(f"""
            <div class="info-box">
                {conclusion_text}
            </div>
            """, unsafe_allow_html=True)


def page_methodo():
    render_header()
    st.markdown("### Méthodologie")
    
    tabs = st.tabs(["Sources", "Modèles", "Limites", "Références"])
    
    with tabs[0]:
        st.markdown("""
        | Source | Méthode | Max posts | Vitesse | Labels |
        |--------|---------|-----------|---------|--------|
        | Reddit | HTTP | 1000 | ~1-5s | Non |
        | Reddit | Selenium | 200 | ~10-30s | Non |
        | StockTwits | Selenium | 1000 | ~30-60s | Oui (Bullish/Bearish) |
        
        **Note:** StockTwits utilise Cloudflare, seul Selenium fonctionne.
        """)
    
    with tabs[1]:
        st.markdown("""
        | Modèle | Entraîné sur | Labels |
        |--------|--------------|--------|
        | **FinBERT** | News financières | Positive/Negative/Neutral |
        | **CryptoBERT** | 3.2M posts crypto | Bullish/Bearish/Neutral |
        
        CryptoBERT: StockTwits (1.8M) + Telegram (664K) + Reddit (172K) + Twitter (496K)
        """)
    
    with tabs[2]:
        st.markdown("""
        **Pour éviter les bans:**
        - Reddit HTTP: max 1000 posts, 1 req/s
        - Reddit Selenium: max 200 posts
        - StockTwits: max 1000 posts (avec scroll amélioré)
        """)
    
    with tabs[3]:
        st.markdown("""
        - **FinBERT:** ProsusAI/finbert
        - **CryptoBERT:** ElKulako/cryptobert (IEEE Intelligent Systems 38(4), 2023)
        - Kraaijeveld & De Smedt (2020) - Predictive power of Twitter sentiment
        """)


# ============ PAGE DONNÉES STOCKÉES ============

def page_stored_data():
    render_header()
    st.markdown("### Données Stockées")
    
    # Récupérer les statistiques
    stats = get_stats()
    
    # Affichage des métriques
    col1, col2, col3 = st.columns(3)
    
    with col1:
        render_metric_card("Total Posts", f"{stats['total_posts']:,}")
    
    with col2:
        render_metric_card("Premier Scrape", stats['first_scrape'][:10] if stats['first_scrape'] else "N/A")
    
    with col3:
        render_metric_card("Dernier Scrape", stats['last_scrape'][:10] if stats['last_scrape'] else "N/A")
    
    st.markdown("---")
    
    # Répartition par source/méthode
    if stats['by_source_method']:
        st.markdown("#### Répartition par Source et Méthode")
        df_stats = pd.DataFrame(stats['by_source_method'])
        
        fig = px.bar(
            df_stats,
            x='source',
            y='count',
            color='method',
            barmode='group',
            title='Nombre de posts par source et méthode',
            color_discrete_sequence=['#818cf8', '#22d3ee']
        )
        fig.update_layout(
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)',
            font_color='#e0e7ff'
        )
        st.plotly_chart(fig, use_container_width=True)
    
    st.markdown("---")
    
    # Filtres
    st.markdown("#### Consulter les Données")
    
    col1, col2, col3 = st.columns(3)
    with col1:
        source_filter = st.selectbox("Source", ["Toutes", "reddit", "stocktwits", "telegram"])
    with col2:
        method_filter = st.selectbox("Méthode", ["Toutes", "http", "selenium"])
    with col3:
        limit = st.number_input("Limite", min_value=10, max_value=1000, value=100)
    
    # Récupérer les données
    source = source_filter if source_filter != "Toutes" else None
    method = method_filter if method_filter != "Toutes" else None
    
    posts = get_all_posts(source=source, method=method, limit=limit)
    
    if posts:
        st.success(f"{len(posts)} posts trouvés")
        
        # Afficher en DataFrame
        df = pd.DataFrame(posts)
        st.dataframe(df, use_container_width=True)
        
        # Boutons d'export
        st.markdown("#### Exporter les Données")
        col1, col2 = st.columns(2)
        
        with col1:
            if st.button("Exporter en CSV"):
                csv_path = export_to_csv(source=source, method=method)
                st.success(f"Exporté vers: {csv_path}")
        
        with col2:
            if st.button("Exporter en JSON"):
                json_path = export_to_json(source=source, method=method)
                st.success(f"Exporté vers: {json_path}")
    else:
        st.warning("Aucune donnée trouvée avec ces filtres.")
    
    # Informations sur les fichiers
    st.markdown("---")
    st.markdown("#### Localisation des Fichiers")
    st.code(f"""
Base de données SQLite: {stats['db_path']}
Fichier JSONL: {stats['jsonl_path']}
Exports CSV/JSON: data/exports/
    """)


# ============ PAGE SCRAPING ============

def page_scraping():
    """Page dédiée au scraping de données"""
    st.markdown("""
    <div style="margin-bottom: 1.5rem;">
        <h2 style="font-size: 1.8rem; font-weight: 600; color: #e0e7ff; margin-bottom: 0.3rem;">Data Scraper</h2>
        <p style="color: #64748b; font-size: 0.9rem;">Collecte de données multi-sources</p>
    </div>
    """, unsafe_allow_html=True)
    
    # Sources avec icônes
    sources = {
        "Reddit": {"icon": "🔴", "max": 1000, "desc": "Subreddits crypto"},
        "Twitter": {"icon": "🐦", "max": 2000, "desc": "Recherche avancée"},
        "YouTube": {"icon": "▶️", "max": 5000, "desc": "Commentaires vidéos"},
        "Telegram": {"icon": "✈️", "max": 500, "desc": "Channels publics"},
        "StockTwits": {"icon": "📈", "max": 1000, "desc": "Labels inclus (scroll amélioré)"},
        "4chan": {"icon": "💬", "max": 200, "desc": "/biz/ discussions"},
        "Bitcointalk": {"icon": "💭", "max": 200, "desc": "Forum historique"},
        "GitHub": {"icon": "💻", "max": 200, "desc": "Issues/Discussions"},
    }
    
    # Sélection de la source - 3 plateformes par ligne
    if 'scrape_source' not in st.session_state:
        st.session_state.scrape_source = "Reddit"
    if 'show_more_platforms' not in st.session_state:
        st.session_state.show_more_platforms = False
    
    sources_list = list(sources.items())
    num_rows = (len(sources_list) + 2) // 3  # Arrondir vers le haut (3 par ligne)
    
    # Afficher UNIQUEMENT les 2 premières lignes (6 plateformes) dans la section principale
    # Les autres lignes seront affichées uniquement quand "Voir plus" est cliqué
    for row in range(2):  # Toujours afficher seulement les lignes 0 et 1
        cols = st.columns(3)
        for col_idx in range(3):
            source_idx = row * 3 + col_idx
            if source_idx < len(sources_list):
                name, info = sources_list[source_idx]
                with cols[col_idx]:
                    selected = st.session_state.scrape_source == name
                    border_color = "#6366f1" if selected else "rgba(100,100,140,0.3)"
                    bg = "rgba(99, 102, 241, 0.1)" if selected else "rgba(30, 30, 50, 0.5)"
                    
                    st.markdown(f"""
                    <div style="
                        background: {bg};
                        border: 2px solid {border_color};
                        border-radius: 12px;
                        padding: 16px 8px;
                        text-align: center;
                    ">
                        <div style="font-size: 1.5rem;">{info['icon']}</div>
                        <div style="font-weight: 600; color: {'#fff' if selected else '#a5b4fc'}; margin-top: 4px;">{name}</div>
                        <div style="font-size: 0.7rem; color: #64748b; margin-top: 2px;">{info['desc']}</div>
                        <div style="font-size: 0.65rem; color: #475569; margin-top: 2px;">{info['max']} max</div>
                    </div>
                    """, unsafe_allow_html=True)
                    
                    btn_label = "Actif" if selected else "Sélectionner"
                    if st.button(btn_label, key=f"src_{name}", use_container_width=True, disabled=selected):
                        st.session_state.scrape_source = name
                        st.session_state.pop('scrape_results', None)
                        st.rerun()
    
    # Bouton "Voir plus" / "Voir moins" si plus de 2 lignes
    if num_rows > 2:
        col1, col2, col3 = st.columns([1, 1, 1])
        with col2:
            if st.session_state.show_more_platforms:
                if st.button("▲ Voir moins", use_container_width=True, key="toggle_platforms"):
                    st.session_state.show_more_platforms = False
                    st.rerun()
            else:
                if st.button("▼ Voir plus", use_container_width=True, key="toggle_platforms"):
                    st.session_state.show_more_platforms = True
                    st.rerun()
        
        # Afficher UNIQUEMENT les lignes supplémentaires (à partir de la ligne 2) quand "Voir plus" est cliqué
        if st.session_state.show_more_platforms:
            for row in range(2, num_rows):
                cols = st.columns(3)
                for col_idx in range(3):
                    source_idx = row * 3 + col_idx
                    if source_idx < len(sources_list):
                        name, info = sources_list[source_idx]
                        with cols[col_idx]:
                            selected = st.session_state.scrape_source == name
                            border_color = "#6366f1" if selected else "rgba(100,100,140,0.3)"
                            bg = "rgba(99, 102, 241, 0.1)" if selected else "rgba(30, 30, 50, 0.5)"
                            
                            st.markdown(f"""
                            <div style="
                                background: {bg};
                                border: 2px solid {border_color};
                                border-radius: 12px;
                                padding: 16px 8px;
                                text-align: center;
                            ">
                                <div style="font-size: 1.5rem;">{info['icon']}</div>
                                <div style="font-weight: 600; color: {'#fff' if selected else '#a5b4fc'}; margin-top: 4px;">{name}</div>
                                <div style="font-size: 0.7rem; color: #64748b; margin-top: 2px;">{info['desc']}</div>
                                <div style="font-size: 0.65rem; color: #475569; margin-top: 2px;">{info['max']} max</div>
                            </div>
                            """, unsafe_allow_html=True)
                            
                            btn_label = "Actif" if selected else "Sélectionner"
                            if st.button(btn_label, key=f"src_{name}_more", use_container_width=True, disabled=selected):
                                st.session_state.scrape_source = name
                                st.session_state.pop('scrape_results', None)
                                st.rerun()
    
    st.markdown("---")
    
    # Configuration selon la source
    source = st.session_state.scrape_source
    
    st.markdown(f"### Configuration {source}")
    
    if source == "Reddit":
        c1, c2 = st.columns(2)
        with c1:
            crypto = st.selectbox("Cryptomonnaie", list(CRYPTO_LIST.keys()), key="scr_crypto")
        with c2:
            limit = st.slider("Nombre de posts", 10, 1000, 100, key="scr_limit")
        
        # Sélecteurs de date
        st.markdown("**Filtres de date (optionnel)**")
        c3, c4 = st.columns(2)
        with c3:
            start_date = st.date_input("Date de début", value=None, key="scr_reddit_start")
        with c4:
            end_date = st.date_input("Date de fin", value=None, key="scr_reddit_end")
        
        if st.button("Lancer le scraping", type="primary", use_container_width=True, key="scr_btn"):
            config = CRYPTO_LIST[crypto]
            
            # Validation des dates
            today = date.today()
            if start_date and start_date > today:
                st.error("⚠️ La date de début ne peut pas être dans le futur")
                st.stop()
            if end_date and end_date > today:
                st.warning("⚠️ La date de fin est dans le futur. Les posts récents seront récupérés jusqu'à aujourd'hui.")
                end_date = today
            
            with st.spinner("Scraping Reddit en cours..."):
                posts = scrape_reddit(
                    config['sub'], limit, method='http',
                    start_date=start_date.strftime('%Y-%m-%d') if start_date else None,
                    end_date=end_date.strftime('%Y-%m-%d') if end_date else None
                )
            
            # Message d'aide si aucun post
            if not posts:
                if end_date and end_date < today:
                    st.warning(f"ℹ️ Aucun post récupéré. Les posts récents sont datés de {today.strftime('%Y-%m-%d')} ou après. La date de fin ({end_date.strftime('%Y-%m-%d')}) est dans le passé. Essayez de mettre la date de fin à aujourd'hui ou laissez-la vide pour récupérer les posts récents.")
                elif start_date:
                    st.warning(f"ℹ️ Aucun post récupéré dans la plage {start_date.strftime('%Y-%m-%d')} - {end_date.strftime('%Y-%m-%d') if end_date else 'aujourd\'hui'}. Les scrapers récupèrent d'abord les posts les plus récents.")
                else:
                    st.error("❌ Aucun post récupéré. Vérifiez le nom du subreddit et votre connexion.")
            
            st.session_state.scrape_results = {"posts": posts, "source": "reddit", "crypto": crypto}
    
    elif source == "Twitter":
        c1, c2 = st.columns(2)
        with c1:
            crypto = st.selectbox("Cryptomonnaie", list(CRYPTO_LIST.keys()), key="scr_crypto")
            limit = st.slider("Nombre de tweets", 10, 2000, 100, key="scr_limit")
        with c2:
            sort_mode = st.selectbox("Tri", ["top", "live"], format_func=lambda x: "Populaires" if x == "top" else "Récents", key="scr_sort")
            min_likes = st.number_input("Minimum de likes", 0, 10000, 0, key="scr_likes")
        
        c1, c2 = st.columns(2)
        with c1:
            start_date = st.date_input("Date de début (optionnel)", value=None, key="scr_start")
        with c2:
            end_date = st.date_input("Date de fin (optionnel)", value=None, key="scr_end")
        
        if st.button("Lancer le scraping", type="primary", use_container_width=True, key="scr_btn"):
            config = CRYPTO_LIST[crypto]
            with st.spinner("Scraping Twitter en cours..."):
                try:
                    posts = scrape_twitter(
                        config.get('sub', crypto), limit,
                        min_likes=min_likes if min_likes > 0 else None,
                        start_date=start_date.strftime('%Y-%m-%d') if start_date else None,
                        end_date=end_date.strftime('%Y-%m-%d') if end_date else None,
                        sort_mode=sort_mode
                    )
                    if not posts:
                        st.warning("⚠️ Aucun tweet récupéré. Twitter peut bloquer le scraping. Vérifiez les logs dans le terminal.")
                    else:
                        st.success(f"✅ {len(posts)} tweets récupérés!")
                except Exception as e:
                    st.error(f"❌ Erreur lors du scraping Twitter: {e}")
                    st.info("💡 Conseils: Vérifiez que Chrome/ChromeDriver est installé, ou utilisez le mode Nitter (fallback automatique)")
                    posts = []
            st.session_state.scrape_results = {"posts": posts, "source": "twitter", "crypto": crypto}
    
    elif source == "YouTube":
        try:
            from app.scrapers.youtube_scraper import scrape_youtube
            api_key = os.environ.get('YOUTUBE_API_KEY', '')
            
            url = st.text_input("URL de la vidéo YouTube", placeholder="https://youtube.com/watch?v=...", key="scr_url")
            
            c1, c2 = st.columns(2)
            with c1:
                limit = st.slider("Nombre de commentaires", 10, 5000, 100, key="scr_limit")
            with c2:
                order = st.selectbox("Tri", ["relevance", "time"], format_func=lambda x: "Populaires" if x == "relevance" else "Récents", key="scr_order")
            
            if api_key:
                st.success("Clé API YouTube configurée")
            else:
                st.warning("Clé API manquante - ajoutez YOUTUBE_API_KEY dans .env")
            
            if st.button("Lancer le scraping", type="primary", use_container_width=True, key="scr_btn"):
                if not url:
                    st.error("Veuillez entrer une URL YouTube")
                else:
                    with st.spinner("Scraping YouTube en cours..."):
                        posts = scrape_youtube("", limit, method="api", video_url=url, order=order)
                    st.session_state.scrape_results = {"posts": posts, "source": "youtube", "crypto": "YouTube"}
        except ImportError:
            st.error("Module YouTube non disponible")
    
    elif source == "Telegram":
        c1, c2 = st.columns(2)
        with c1:
            channel = st.selectbox("Channel", list(TELEGRAM_CHANNELS.keys()), format_func=lambda x: f"@{x}", key="scr_channel")
        with c2:
            limit = st.slider("Nombre de messages", 10, 500, 100, key="scr_limit")
        
        st.caption(f"Description: {TELEGRAM_CHANNELS[channel]}")
        
        if st.button("Lancer le scraping", type="primary", use_container_width=True, key="scr_btn"):
            with st.spinner("Scraping Telegram en cours..."):
                try:
                    if limit > 30:
                        posts = scrape_telegram_paginated(channel, limit)
                    else:
                        posts = scrape_telegram_simple(channel, limit)
                    
                    if not posts:
                        st.warning(f"⚠️ Aucun message récupéré pour @{channel}")
                        st.info("**Note :** Seuls les canaux publics fonctionnels sont disponibles dans la liste.")
                    else:
                        for p in posts:
                            p['title'] = p.get('text', '')
                        st.session_state.scrape_results = {"posts": posts, "source": "telegram", "crypto": channel}
                except Exception as e:
                    st.error(f"❌ Erreur lors du scraping: {e}")
                    st.exception(e)
    
    elif source == "StockTwits":
        c1, c2 = st.columns(2)
        with c1:
            crypto = st.selectbox("Cryptomonnaie", list(CRYPTO_LIST.keys()), key="scr_crypto")
        with c2:
            max_limit = LIMITS["StockTwits"]["Selenium"]  # 1000 posts max
            limit = st.slider("Nombre de posts", 10, max_limit, min(100, max_limit), key="scr_limit")
        
        # Sélecteurs de date
        st.markdown("**Filtres de date (optionnel)**")
        c3, c4 = st.columns(2)
        with c3:
            start_date = st.date_input("Date de début", value=None, key="scr_stocktwits_start")
        with c4:
            end_date = st.date_input("Date de fin", value=None, key="scr_stocktwits_end")
        
        st.info("Les labels Bullish/Bearish sont inclus automatiquement")
        
        if st.button("Lancer le scraping", type="primary", use_container_width=True, key="scr_btn"):
            config = CRYPTO_LIST[crypto]
            
            # Validation des dates
            today = date.today()
            if start_date and start_date > today:
                st.error("⚠️ La date de début ne peut pas être dans le futur")
                st.stop()
            if end_date and end_date > today:
                st.warning("⚠️ La date de fin est dans le futur. Les posts récents seront récupérés jusqu'à aujourd'hui.")
                end_date = today
            
            with st.spinner("Scraping StockTwits en cours..."):
                posts = scrape_stocktwits(
                    config['stocktwits'], limit,
                    start_date=start_date.strftime('%Y-%m-%d') if start_date else None,
                    end_date=end_date.strftime('%Y-%m-%d') if end_date else None
                )
            
            # Message d'aide si aucun post
            if not posts:
                if end_date and end_date < today:
                    st.warning(f"ℹ️ Aucun post récupéré. Les posts récents sont datés de {today.strftime('%Y-%m-%d')} ou après. La date de fin ({end_date.strftime('%Y-%m-%d')}) est dans le passé. Essayez de mettre la date de fin à aujourd'hui ou laissez-la vide pour récupérer les posts récents.")
                elif start_date:
                    st.warning(f"ℹ️ Aucun post récupéré dans la plage {start_date.strftime('%Y-%m-%d')} - {end_date.strftime('%Y-%m-%d') if end_date else 'aujourd\'hui'}. Les scrapers récupèrent d'abord les posts les plus récents.")
                else:
                    st.error("❌ Aucun post récupéré. Vérifiez votre connexion et que Selenium est installé.")
            
            st.session_state.scrape_results = {"posts": posts, "source": "stocktwits", "crypto": crypto}
    
    elif source == "4chan":
        c1, c2 = st.columns(2)
        with c1:
            crypto = st.selectbox("Cryptomonnaie", list(CRYPTO_LIST.keys()), key="scr_crypto")
        with c2:
            limit = st.slider("Nombre de posts", 10, 200, 50, key="scr_limit")
        
        st.markdown("""
        <div class="info-box">
            <strong>4chan /biz/</strong> — Discussions crypto anonymes<br>
            <small>Scraping rapide via API, pas de login requis. Discussions très actives sur crypto.</small>
        </div>
        """, unsafe_allow_html=True)
        
        if st.button("Lancer le scraping", type="primary", use_container_width=True, key="scr_btn"):
            config = CRYPTO_LIST[crypto]
            with st.spinner("Scraping 4chan /biz/ en cours..."):
                query = config.get('sub', 'crypto').lower()
                posts = scrape_4chan_biz(query, limit)
            if posts:
                st.success(f"✅ {len(posts)} posts récupérés depuis 4chan /biz/")
            else:
                st.warning("⚠️ Aucun post récupéré")
            st.session_state.scrape_results = {"posts": posts, "source": "4chan", "crypto": crypto}
    
    elif source == "Bitcointalk":
        c1, c2 = st.columns(2)
        with c1:
            crypto = st.selectbox("Cryptomonnaie", list(CRYPTO_LIST.keys()), key="scr_crypto")
        with c2:
            limit = st.slider("Nombre de posts", 10, 200, 50, key="scr_limit")
        
        st.markdown("""
        <div class="info-box">
            <strong>Bitcointalk</strong> — Forum crypto historique<br>
            <small>Scraping via HTTP, pas de login requis. Discussions longues et détaillées sur crypto.</small>
        </div>
        """, unsafe_allow_html=True)
        
        if st.button("Lancer le scraping", type="primary", use_container_width=True, key="scr_btn"):
            config = CRYPTO_LIST[crypto]
            with st.spinner("Scraping Bitcointalk en cours..."):
                query = config.get('sub', 'crypto').lower()
                posts = scrape_bitcointalk(query, limit)
            if posts:
                st.success(f"✅ {len(posts)} posts récupérés depuis Bitcointalk")
            else:
                st.warning("⚠️ Aucun post récupéré")
            st.session_state.scrape_results = {"posts": posts, "source": "bitcointalk", "crypto": crypto}
    
    elif source == "GitHub":
        c1, c2 = st.columns(2)
        with c1:
            crypto = st.selectbox("Cryptomonnaie", list(CRYPTO_LIST.keys()), key="scr_crypto")
        with c2:
            limit = st.slider("Nombre de posts", 10, 200, 50, key="scr_limit")
        
        st.markdown("""
        <div class="info-box">
            <strong>GitHub</strong> — Issues/Discussions projets crypto<br>
            <small>API officielle GitHub (gratuite). Discussions techniques sur projets Bitcoin, Ethereum, Solana, etc.</small>
        </div>
        """, unsafe_allow_html=True)
        
        if st.button("Lancer le scraping", type="primary", use_container_width=True, key="scr_btn"):
            config = CRYPTO_LIST[crypto]
            with st.spinner("Scraping GitHub Issues en cours..."):
                query = config.get('sub', 'crypto').lower()
                posts = scrape_github_discussions(query, limit)
            if posts:
                st.success(f"✅ {len(posts)} issues/discussions récupérées depuis GitHub")
            else:
                st.warning("⚠️ Aucun post récupéré")
            st.session_state.scrape_results = {"posts": posts, "source": "github", "crypto": crypto}
    
    # Affichage des résultats
    st.markdown("---")
    
    if 'scrape_results' in st.session_state and st.session_state.scrape_results:
        data = st.session_state.scrape_results
        posts = data['posts']
        
        if not posts:
            st.error("Aucun post récupéré")
        else:
            # Stats
            labeled = sum(1 for p in posts if p.get('human_label'))
            with_score = sum(1 for p in posts if p.get('score', 0) > 0)
            
            st.markdown(f"""
            <div style="display: flex; gap: 16px; margin-bottom: 16px;">
                <div style="background: linear-gradient(135deg, rgba(99, 102, 241, 0.15), rgba(139, 92, 246, 0.1)); padding: 14px 24px; border-radius: 12px; border: 1px solid rgba(99, 102, 241, 0.3);">
                    <span style="font-size: 1.8rem; font-weight: 700; color: #a5b4fc;">{len(posts)}</span>
                    <span style="color: #94a3b8; font-size: 0.9rem; margin-left: 8px;">posts récupérés</span>
                </div>
                <div style="background: rgba(74, 222, 128, 0.1); padding: 14px 20px; border-radius: 12px; border: 1px solid rgba(74, 222, 128, 0.2);">
                    <span style="color: #4ade80; font-weight: 600;">{labeled}</span>
                    <span style="color: #64748b; font-size: 0.85rem;"> avec label</span>
                </div>
                <div style="background: rgba(251, 191, 36, 0.1); padding: 14px 20px; border-radius: 12px; border: 1px solid rgba(251, 191, 36, 0.2);">
                    <span style="color: #fbbf24; font-weight: 600;">{with_score}</span>
                    <span style="color: #64748b; font-size: 0.85rem;"> avec score</span>
                </div>
            </div>
            """, unsafe_allow_html=True)
            
            # Actions
            c1, c2, c3 = st.columns(3)
            with c1:
                if st.button("Sauvegarder en base", use_container_width=True, type="primary"):
                    result = save_posts(posts, source=data['source'], method="scraper")
                    st.success(f"{result['inserted']} posts sauvegardés")
            with c2:
                if st.button("Envoyer vers Analyse", use_container_width=True):
                    st.session_state['analyze_data'] = posts
                    st.info("Données prêtes pour l'analyse")
            with c3:
                csv_data = pd.DataFrame(posts).to_csv(index=False)
                st.download_button("Exporter CSV", csv_data, f"{data['source']}_data.csv", use_container_width=True)
            
            # Tableau
            st.markdown("<br>", unsafe_allow_html=True)
            
            def safe_date(val):
                if not val:
                    return '-'
                if isinstance(val, (int, float)):
                    try:
                        return datetime.fromtimestamp(val).strftime('%Y-%m-%d')
                    except:
                        return '-'
                return str(val)[:10] if len(str(val)) > 10 else str(val)
            
            df = pd.DataFrame([{
                "Texte": (p.get('title') or p.get('text', ''))[:100] + "..." if len(p.get('title') or p.get('text', '')) > 100 else (p.get('title') or p.get('text', '')),
                "Score": p.get('score', 0),
                "Label": p.get('human_label') or '-',
                "Auteur": (p.get('author') or '-')[:15],
                "Date": safe_date(p.get('created_utc'))
            } for p in posts[:50]])
            
            st.dataframe(df, use_container_width=True, height=400)
            
            if len(posts) > 50:
                st.caption(f"Affichage de 50 posts sur {len(posts)}")
    else:
        st.markdown("""
        <div style="
            display: flex; flex-direction: column; align-items: center; justify-content: center;
            padding: 60px 20px; background: rgba(30, 30, 50, 0.3); border-radius: 16px;
            border: 1px dashed rgba(99, 102, 241, 0.3);
        ">
            <div style="color: #64748b; font-size: 1rem;">Les résultats apparaîtront ici</div>
            <div style="color: #475569; font-size: 0.85rem; margin-top: 8px;">Sélectionnez une source et lancez le scraping</div>
        </div>
        """, unsafe_allow_html=True)


# ============ MAIN ============

def main():
    with st.sidebar:
        st.markdown("""
        <div style="text-align: center; padding: 1rem 0;">
            <div style="font-size: 2rem; color: #818cf8;">◈</div>
            <div style="font-weight: 700; color: #e0e7ff;">Crypto Sentiment</div>
            <div style="font-size: 0.75rem; color: #64748b;">MoSEF 2025-2026</div>
        </div>
        """, unsafe_allow_html=True)
        
        st.markdown("---")
        
        page = st.radio(
            "Navigation",
            ["Scraping", "Dashboard", "Comparaison", "Multi-crypto", "Économétrie", "Données", "Méthodologie"],
            label_visibility="collapsed"
        )
    
    if "Scraping" in page:
        page_scraping()
    elif "Dashboard" in page:
        page_dashboard()
    elif "Comparaison" in page:
        page_compare()
    elif "Multi" in page:
        page_multi()
    elif "Économétrie" in page:
        page_econometrie()
    elif "Données" in page:
        page_stored_data()
    elif "Méthodologie" in page:
        page_methodo()


if __name__ == "__main__":
    main()
