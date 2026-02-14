# Données externes — Compléter la base et étudier le comportement grégaire

Ce dossier regroupe les **sources de données externes** (Kaggle, Zenodo, Hugging Face, etc.) que l’on peut importer dans la base du projet (`posts2` ou SQLite) pour enrichir les analyses **sentiment–prix** et étudier, en particulier, les **comportements de type « mouton »** (grégaire) sur le marché du Bitcoin.

---

## 1. Objectif et plage visée

- **Objectif** : disposer de **texte (posts, tweets, news) + dates + éventuellement prix** sur une **longue période**, pour faire des analyses économétriques (stationnarité, causalité de Granger, VAR) et étudier le lien entre sentiment en ligne et évolution des prix.
- **Plage visée** : **2020 (ou avant) → 2026**.  
  Les bases publiques couvrent surtout 2020–2023 ; pour **2023–2026**, le plus réaliste est d’utiliser **votre propre scraping** (Streamlit + scripts du projet), déjà en place.

En combinant **données externes** (historique) et **votre collecte** (période récente), on peut viser une couverture 2020→2026.

---

## 2. Bases de données disponibles

### 2.1 Longue plage (2020–2022)

| Source | Plage | Type | Lien / accès |
|--------|--------|------|----------------|
| **Zenodo – Reddit r/cryptocurrency** | Jan 2021 – Déc 2022 | Reddit (770 posts, 14 886 commentaires) + **prix BTC quotidiens** (OHLCV) | [Zenodo DOI 10.5281/zenodo.12593440](https://zenodo.org/doi/10.5281/zenodo.12593439) · [reddit_posts.csv](https://zenodo.org/records/12593440/files/reddit_posts.csv?download=1) · [reddit_comments.csv](https://zenodo.org/records/12593440/files/reddit_comments.csv?download=1) · [BTC-USD.csv](https://zenodo.org/records/12593440/files/BTC-USD.csv?download=1) |
| **SoBigData – Tweets crypto** | Oct 2020 – Mar 2021 | ~30 M tweets + prix BTC 15 min | Inscription sur [SoBigData.eu](https://ckan-sobigdata.d4science.org/), recherche « Crypto related tweets » |
| **BMC – Twitter influenceurs** | Fév 2021 – Juin 2023 | 52+ influenceurs, 8 cryptos, sentiment + prix | **Données** : [Mendeley Data (5 fichiers Excel + codes Python)](https://data.mendeley.com/datasets/8fbdhh72gs/5) · **Article** : [BMC Research Notes (2021–2023)](https://bmcresnotes.biomedcentral.com/articles/10.1186/s13104-023-06548-z) |

### 2.2 Bases plus récentes (2023–2025)

| Source | Plage | Type | Lien |
|--------|--------|------|------|
| **vmintam/reddit_dataset_66** (Hugging Face) | **20 fév. 2025 – 16 avr. 2025** | ~280 k posts/commentaires, 13 subreddits crypto | [Hugging Face – vmintam/reddit_dataset_66](https://huggingface.co/datasets/vmintam/reddit_dataset_66) |
| **PulseReddit / RedditDataset** (GitHub) | ~2023–2024 | Reddit (JSONL) + **prix Binance** 5 min / 15 min / 1 h / 4 h (BTC, ETH, DOGE, SOL) | [GitHub – 7huahua/RedditDataset](https://github.com/7huahua/RedditDataset) · [PulseReddit arXiv](https://arxiv.org/html/2506.03861) |
| **Kaggle – Sentiment Analysis of Bitcoin News** | 2021–2024 | Titres d’actualités + prix (open, high, low, close, volume) | [Kaggle – imadallal/sentiment-analysis-of-bitcoin-news-2021-2024](https://www.kaggle.com/datasets/imadallal/sentiment-analysis-of-bitcoin-news-2021-2024) |
| **Mendeley – Influenceurs crypto** | 2021–2023 | Même jeu que BMC ci‑dessus : 5 Excel + codes Python, téléchargeable directement | [Mendeley Data V5](https://data.mendeley.com/datasets/8fbdhh72gs/5) |
| **Kaggle – Cryptocurrency Tweets** | À vérifier | Tweets crypto | [Kaggle – infsceps/cryptocurrency-tweets](https://www.kaggle.com/datasets/infsceps/cryptocurrency-tweets) |

### 2.3 À éviter pour une longue période

| Source | Plage | Raison |
|--------|--------|--------|
| **Kaggle – BTC Tweets Sentiment** | **Un seul jour** (23 mars 2018) | Trop court pour séries temporelles ; utile seulement pour tests d’import. Script : `python scripts/import_kaggle_btc_tweets.py` (voir fin de ce README). |

### 2.4 Votre propre scraping (2023 → 2026)

Les scrapers du projet (Reddit, Twitter, Telegram, 4chan, Bitcointalk, GitHub, Bluesky, YouTube, etc.) et la base `posts2` permettent de **couvrir 2023–2026** au fil du temps. C’est la seule façon d’avoir des données vraiment récentes et alignées avec votre schéma.

---

## 3. Comportement « de mouton » (grégaire) sur le marché Bitcoin

### 3.1 De quoi parle-t-on ?

En finance, le **comportement de mouton** (ou **grégaire**, *herd behavior*) désigne le fait que les acteurs **suivent le groupe** plutôt que leur propre analyse :

- **Unanimité** : presque tout le monde est très bullish ou très bearish en même temps.
- **Réaction en retard** : le prix bouge après que le sentiment soit déjà extrême (les « moutons » réagissent après le mouvement).
- **Convergence des croyances** : peu de désaccord, tout le monde dit la même chose.

L’idée avec les bases listées ci-dessus : utiliser le **sentiment sur les réseaux** (Reddit, Twitter, news) comme **proxy de ce que « le troupeau » exprime**, puis regarder comment ce sentiment se relie au **prix** (retards, excès d’optimisme/pessimisme, retournements).

### 3.2 Est-ce que ces bases permettent de l’étudier ?

**Oui.** Plusieurs bases sont adaptées, à condition de les **combiner avec des prix** (fournis dans le dataset ou via CoinGecko/Binance) :

- **Zenodo Reddit (2021–2022)** : texte + **prix BTC quotidiens** → sentiment agrégé par jour vs rendement ; repérer les périodes où tout le monde est très positif/négatif.
- **PulseReddit** : Reddit + **prix haute fréquence** → voir si le sentiment « suit » le prix avec un décalage (les moutons réagissent après le mouvement).
- **vmintam/reddit_dataset_66 (2025)** : même logique sur une fenêtre très récente.
- **Kaggle Bitcoin News (2021–2024)** : médias comme reflet du récit dominant (effet de troupeau via l’information).
- **BMC / Mendeley (influenceurs)** : tester si la foule (Reddit) **suit** les influenceurs avec un décalage (mouton = retard par rapport aux leaders).

### 3.3 Comment mesurer le « mouton » avec ces données

Quelques **pistes méthodologiques** possibles :

1. **Unanimité du sentiment**  
   Agréger le sentiment (FinBERT / CryptoBERT ou labels) **par jour** (ou par fenêtre). Repérer les périodes où une très forte majorité est bullish ou bearish (ex. > 80 %). Comparer ces épisodes aux **mouvements de prix** (forte hausse/baisse qui suit ? qui précède ?).

2. **Retard sentiment → prix (effet suiveur)**  
   Tester si le **sentiment d’hier** (ou 2–3 jours avant) explique le **rendement aujourd’hui** (régression ou **causalité de Granger**). Un lien significatif peut refléter un **décalage** : le marché réagit après que le troupeau ait déjà exprimé son avis.

3. **Dispersion des opinions**  
   Si plusieurs sources sont disponibles (Reddit, news, influenceurs), mesurer la **dispersion** des sentiments (écart-type, part de « neutre » vs extrêmes). Une **dispersion faible** = tout le monde dit la même chose = environnement grégaire. Croiser avec la **volatilité** ou les **retournements** : les phases très unanimes précèdent-elles des krachs ou des bulles ?

4. **Sentiment extrême vs rendements futurs**  
   Quand le sentiment est **très extrême** (très bullish ou très bearish), regarder les **rendements ultérieurs** : sont-ils en moyenne plus faibles ou inversés (effet contrarian) ? Cela peut indiquer que le mouton va trop dans un sens et que le marché corrige ensuite.

5. **VAR et causalité**  
   Comme dans le projet (tests ADF, Granger, VAR) : modéliser la relation entre **série de sentiment agrégé** et **série de prix** (ou rendements). Si le sentiment **précède** le prix (Granger), c’est cohérent avec un effet de suivi du marché par rapport à la foule.

### 3.4 Limites

- Les réseaux sociaux ne représentent **pas tout le marché** ; le comportement grégaire peut aussi exister côté ordres de bourse (non observés ici).
- On peut **identifier des patterns cohérents** avec du comportement grégaire (unanimité, retard, etc.), pas « prouver » le mouton de façon définitive.

---

## 4. Résumé des bases (vue d’ensemble)

| Source | Plage | Type | Intérêt pour 2020→2026 et « mouton » |
|--------|--------|------|--------------------------------------|
| **vmintam/reddit_dataset_66** | Fév–Avr **2025** | Reddit (280k) | Données très récentes ; unanimité / retard |
| **PulseReddit (GitHub)** | ~2023–2024 | Reddit + prix HF | Sentiment + prix haute fréquence ; retard |
| **Kaggle Bitcoin News** | 2021–2024 | News + prix | Récit dominant ; sentiment médias |
| **Mendeley influenceurs** | 2021–2023 | Twitter | Foule vs leaders (qui suit qui) |
| **Zenodo Reddit** | 2021–2022 | Reddit + prix | 2 ans, bien adapté ; unanimité, Granger, VAR |
| **SoBigData** | Oct 2020–Mar 2021 | Twitter | Volume 2020–2021 |
| **BMC (influenceurs)** | Fév 2021–Juin 2023 | Twitter + sentiment | Jusqu’à mi‑2023 |
| **Votre scraping** | 2023–2026 | Multi | Compléter jusqu’à 2026 |
| **Kaggle BTC Tweets** | 1 jour 2018 | Twitter | Tests d’import uniquement |

**Recommandation** : pour une **première analyse « comportement de mouton »**, combiner **Zenodo Reddit (2021–2022)** (texte + prix) avec votre pipeline NLP (FinBERT/CryptoBERT) et les outils économétriques du projet (ADF, Granger, VAR). Ensuite, ajouter **PulseReddit** ou **vmintam/reddit_dataset_66** pour des données plus récentes.

---

## 4.1 Combler le creux 2022-01 → 2023-04

Si le graphique « Nombre de posts par date » montre un **creux** entre début 2022 et avril 2023, les sources suivantes permettent de le combler (à importer ou à vérifier).

| Source | Couverture du creux | Action |
|--------|----------------------|--------|
| **Kaggle – Sentiment Analysis of Bitcoin News 2021–2024** | **2021–2024** (toute la période) | Déjà intégré au projet. Placer `bitcoin_sentiments_21_24.csv` dans `data_externe/bitcoinesentiment/`, puis `transform_bitcoin_sentiments_21_24.py` + `import_bitcoin_sentiments_21_24.py --from-transformed`. |
| **BMC – Twitter influenceurs** | Fév 2021 – Juin 2023 | Couvre 2022 et début 2023. Vérifier que l’import BMC est bien fait (`import_bmc_influencers.py`) et que les dates du CSV sont bien réparties sur 2022. |
| **Zenodo – Reddit r/cryptocurrency** | Jan 2021 – Déc 2022 | Couvre 2022 mais ~770 posts au total : peu de points par jour. Import : `transform_reddit_zenodo.py` puis `import_reddit_zenodo.py --from-transformed`. |
| **Kaggle – Bitcoin Twitter Sentiment 2013–2023** | 2013–2023 | Tweets Bitcoin avec sentiment. À télécharger sur [Kaggle](https://www.kaggle.com/datasets/andreapenasmartinez/bitcoin-twitter-sentiment-dataset-20132023), puis adapter un script d’import (même format que BMC : date, texte, sentiment). |
| **Kaggle – Cryptocurrency Tweets** | À vérifier sur la page | [Kaggle – infsceps/cryptocurrency-tweets](https://www.kaggle.com/datasets/infsceps/cryptocurrency-tweets). Si les dates incluent 2022–2023, ajouter un script d’import vers `posts2`. |

**Ordre recommandé** pour combler le creux :

1. **Bitcoin News 2021–2024** : une fois le CSV en place, l’import alimente `posts2` avec des **news** (source = type Yahoo/bitcoinesentiment) sur toute la période.
2. **Vérifier BMC** : s’assurer que les tweets BMC sont bien en base et que les `created_utc` couvrent 2022 (par requête SQL ou via le dashboard par date).
3. **Zenodo Reddit** : importer si pas déjà fait ; volume faible mais réparti sur 2021–2022.
4. **Nouveau jeu Kaggle** (Bitcoin Twitter 2013–2023 ou Cryptocurrency Tweets) : télécharger, inspecter les colonnes et les dates, puis écrire un petit script de transformation/import sur le modèle de `import_bmc_influencers.py` ou `import_reddit_zenodo.py`.

Après import, relancer l’analyse de sentiment en lot sur les posts sans score (`python scripts/analyze_posts_batch.py --model cryptobert --only-missing`) puis régénérer le graphique par date pour vérifier que le creux est atténué.

---

## 5. Import des données externes

### Script Bitcoin News 2021–2024 (Yahoo Finance)

Fichier source : `data_externe/bitcoinesentiment/bitcoin_sentiments_21_24.csv`  
Colonnes : **Date**, **Short Description**, **Accurate Sentiments**.

**Workflow en deux étapes (recommandé)** : d’abord mettre les données au format de la table cloud, puis envoyer au cloud.

1. **Modifier / transformer** (données → même structure que la table cloud)  
   Produit `bitcoin_sentiments_cloud_format.csv` avec les colonnes : **uid**, **id**, **source**, **method**, **title**, **text**, **score**, **created_utc**, **human_label**, **author**, **subreddit**, **url**, **num_comments**, **scraped_at**, **sentiment_score**.

   ```bash
   python scripts/transform_bitcoin_sentiments_21_24.py
   python scripts/transform_bitcoin_sentiments_21_24.py --limit 100   # test
   ```

2. **Envoyer au cloud** (lecture du fichier transformé puis upload)

   ```bash
   python scripts/import_bitcoin_sentiments_21_24.py --from-transformed
   ```

**Alternative en une étape** (transformation en mémoire puis envoi) :

```bash
python scripts/import_bitcoin_sentiments_21_24.py
python scripts/import_bitcoin_sentiments_21_24.py --limit 100
python scripts/import_bitcoin_sentiments_21_24.py --dry-run
```

**Avant le premier envoi au cloud** : exécuter la migration sur Supabase (ajout de `sentiment_score` si besoin) :  
→ SQL Editor : `ALTER TABLE posts2 ADD COLUMN IF NOT EXISTS sentiment_score REAL;`  
(Voir `scripts/migrations/001_add_sentiment_score.sql`.)

---

### Script Kaggle (BTC Tweets – 1 jour, 2018)

Si vous avez téléchargé `BTC_Tweets_Updated.csv` dans `data_externe/` (par exemple pour tester l’import) :

```bash
# Depuis la racine du projet, environnement activé (poetry ou .venv)
python scripts/import_kaggle_btc_tweets.py          # import complet
python scripts/import_kaggle_btc_tweets.py --limit 1000   # test
python scripts/import_kaggle_btc_tweets.py --dry-run --limit 10   # afficher sans insérer
```

Les posts sont enregistrés avec `source=twitter` et `method=kaggle_import`. Le label de sentiment (positive/negative/neutral) est mappé vers `human_label`.

---

### Reddit r/cryptocurrency + prix BTC (Zenodo) — `data_externe/rediit_cryptocurrency_et_prixBTC`

Fichiers : **reddit_posts.csv**, **BTC-USD.csv** (et reddit_comments.csv non utilisé). On n’importe **que les posts** (pas les commentaires).

**1. Reddit (posts uniquement) → table `posts2`**

- **Transformer** (format cloud) :  
  `python scripts/transform_reddit_zenodo.py`  
  → Produit `reddit_zenodo_cloud_format.csv` (colonnes = posts2).
- **Envoyer au cloud** :  
  `python scripts/import_reddit_zenodo.py --from-transformed`

En une seule étape : `python scripts/import_reddit_zenodo.py` (avec option `--limit` pour tester).

Les posts sont enregistrés avec **source=reddit**, **method=zenodo_posts**. Subreddit = `CryptoCurrency`.

**2. Prix BTC quotidiens → table `btc_usd`**

- **Créer la table** (une fois sur le cloud) : exécuter `scripts/migrations/002_create_btc_usd.sql` dans le SQL Editor Supabase.
- **Importer le CSV** :  
  `python scripts/import_btc_usd.py`

L’import des prix nécessite une **connexion SQL directe** (DATABASE_URL ou SQLite). En mode Supabase REST uniquement (sans DATABASE_URL), créer la table à la main puis utiliser une connexion PostgreSQL (DATABASE_URL) pour lancer le script.

La table **btc_usd** (date, open, high, low, close, adj_close, volume) permet ensuite de joindre par **date** avec les posts Reddit pour les analyses sentiment–prix.

**Prix BTC 2021 → aujourd'hui (plage complète)** — Pour la page Streamlit « Scores de sentiment », vous pouvez :

1. **Téléchargement automatique (Yahoo)** : `pip install yfinance` puis `python scripts/download_btc_usd_2021_now.py`. Cela génère `data_externe/rediit_cryptocurrency_et_prixBTC/BTC-USD.csv` sur 2021–aujourd’hui. Puis `python scripts/import_btc_usd.py`.
2. **Kaggle** : [BTC-USD Historical price (2014-2024)](https://www.kaggle.com/datasets/kannapat/btc-usd-historical-price-2014-2024) — télécharger le CSV, le placer dans le même dossier (nommer `BTC-USD.csv`, colonnes : date, Open, High, Low, Close, Adj.Close, Volume), puis lancer `import_btc_usd.py`.
3. **CryptoDataDownload** : [cryptodatadownload.com/data](https://www.cryptodatadownload.com/data/) — données gratuites par exchange (Coinbase, Binance, etc.) ; adapter les colonnes au format ci-dessus si besoin.

---

### BMC – Twitter influenceurs crypto (2021–2023) — `data_externe/BMC_Twitter_influenceur`

Fichier principal : **dataset_52-person-from-2021-02-05_2023-06-12_*_with_sentiment.csv** (tweets de 52 influenceurs avec sentiment : created_at, full_text, compound, sentiment_type, favorite_count, reply_count, new_coins, etc.).

**Workflow (comme pour les autres sources)** :

1. **Transformer** (format cloud) :  
   `python scripts/transform_bmc_influencers.py`  
   → Produit `bmc_influencers_cloud_format.csv` (colonnes = posts2).
2. **Envoyer au cloud** :  
   `python scripts/import_bmc_influencers.py --from-transformed`

En une étape : `python scripts/import_bmc_influencers.py` (option `--limit` pour tester).

Les lignes sont enregistrées avec **source=bmc_influencers**, **method=bmc_import**, et **is_influencer = true** pour les identifier comme posts d’influenceurs. **sentiment_score** = colonne `compound`, **human_label** = `sentiment_type` (POSITIVE / NEGATIVE / NEUTRAL). La table doit avoir les colonnes **sentiment_score** (migration 001) et **is_influencer** (migration 003). Sur Supabase : exécuter `scripts/migrations/003_add_is_influencer.sql` avant le premier import BMC.

Les fichiers **btc_selected_with_sentiment_*.csv** et **eth_selected_with_sentiment_*.csv** sont des agrégats quotidiens (prix + sentiment par jour) ; ils ne sont pas importés dans posts2 mais peuvent servir pour des analyses jointes (comme btc_usd).

---

*Dernière mise à jour : février 2025.*
