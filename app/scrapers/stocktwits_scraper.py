"""
StockTwits Scraper - Selenium uniquement (bypass Cloudflare)
Labels humains Bullish/Bearish inclus!
"""

import time
import random
import re
import json

try:
    from selenium import webdriver
    from selenium.webdriver.common.by import By
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from bs4 import BeautifulSoup
    SELENIUM_OK = True
except ImportError:
    SELENIUM_OK = False

# Limites pour eviter le ban
LIMITS = {
    "selenium": 300  # Seule methode disponible pour StockTwits
}


def get_limits():
    """Retourne les limites par methode"""
    return LIMITS


def scrape_stocktwits(symbol: str, limit: int = 100, method: str = "selenium") -> list:
    """
    Scrape StockTwits avec Selenium (bypass Cloudflare)
    Note: HTTP ne fonctionne pas (Cloudflare), seul Selenium est disponible

    Args:
        symbol: Symbole crypto (ex: BTC.X, ETH.X)
        limit: Nombre de posts souhaites
        method: Ignore (toujours selenium)

    Returns:
        Liste de posts avec human_label (Bullish/Bearish/None)
    """
    if not SELENIUM_OK:
        print("Selenium non installe")
        return []

    posts = []
    seen_ids = set()
    limit = min(limit, LIMITS["selenium"])

    # Config Chrome
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)

    try:
        driver = webdriver.Chrome(options=options)
        driver.execute_cdp_cmd('Network.setUserAgentOverride', {
            "userAgent": 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        })
    except Exception as e:
        print(f"Erreur Chrome: {e}")
        return []

    try:
        url = f"https://stocktwits.com/symbol/{symbol}"
        print(f"Loading {url}...")
        driver.get(url)

        # Attendre que Cloudflare passe (5-8 sec)
        time.sleep(random.uniform(5, 8))

        # Essayer d'abord d'extraire le JSON embarque (methode la plus fiable)
        posts = extract_json_data(driver, limit)

        if posts:
            print(f"Extracted {len(posts)} posts from JSON")
            driver.quit()
            return posts

        # Fallback: parser le HTML
        print("JSON extraction failed, falling back to HTML parsing...")

        # Attendre le contenu
        try:
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.TAG_NAME, "article"))
            )
        except:
            pass

        time.sleep(2)

        # Scroll pour charger plus
        last_height = driver.execute_script("return document.body.scrollHeight")
        scroll_attempts = 0
        max_scrolls = (limit // 15) + 5

        while len(posts) < limit and scroll_attempts < max_scrolls:
            # Parse HTML
            new_posts = parse_html_posts(driver.page_source, seen_ids)
            posts.extend(new_posts)

            if len(posts) >= limit:
                break

            # Scroll
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(random.uniform(1.5, 2.5))

            new_height = driver.execute_script("return document.body.scrollHeight")
            if new_height == last_height:
                scroll_attempts += 1
                if scroll_attempts >= 3:
                    break
            else:
                scroll_attempts = 0
                last_height = new_height

        print(f"Scraped {len(posts)} posts from StockTwits HTML")

    except Exception as e:
        print(f"Erreur StockTwits Selenium: {e}")
    finally:
        driver.quit()

    return posts[:limit]


def extract_json_data(driver, limit: int) -> list:
    """Extraire les donnees du JSON __NEXT_DATA__ embarque dans la page"""
    posts = []

    try:
        soup = BeautifulSoup(driver.page_source, "lxml")
        script = soup.find("script", {"id": "__NEXT_DATA__"})

        if not script or not script.string:
            return []

        data = json.loads(script.string)

        # Naviguer dans la structure Next.js
        page_props = data.get("props", {}).get("pageProps", {})

        # Plusieurs chemins possibles
        messages = (
            page_props.get("stream", {}).get("messages", []) or
            page_props.get("messages", []) or
            page_props.get("initialMessages", []) or
            []
        )

        for msg in messages[:limit]:
            # Sentiment humain
            sentiment = msg.get("entities", {}).get("sentiment", {})
            human_label = sentiment.get("basic") if sentiment else None

            # Likes
            likes_data = msg.get("likes", {})
            likes = likes_data.get("total", 0) if isinstance(likes_data, dict) else 0

            posts.append({
                "id": str(msg.get("id", "")),
                "title": msg.get("body", ""),
                "text": "",
                "score": likes,
                "created_utc": msg.get("created_at"),
                "source": "stocktwits",
                "method": "selenium",
                "human_label": human_label
            })

    except json.JSONDecodeError:
        print("Failed to parse JSON")
    except Exception as e:
        print(f"JSON extraction error: {e}")

    return posts


def parse_html_posts(page_source: str, seen_ids: set) -> list:
    """Parser les posts depuis le HTML"""
    posts = []
    soup = BeautifulSoup(page_source, "lxml")

    # Selecteurs pour les messages StockTwits
    selectors = [
        "article",
        "[class*='MessageCard']",
        "[class*='StreamMessage']",
        "[data-testid='message']",
        "div[class*='Message']"
    ]

    messages = []
    for selector in selectors:
        messages = soup.select(selector)
        if messages:
            break

    for msg in messages:
        try:
            # ID unique
            msg_id = (
                msg.get("data-id") or
                msg.get("id") or
                msg.get("data-message-id") or
                str(hash(msg.get_text()[:50]))
            )

            if msg_id in seen_ids:
                continue
            seen_ids.add(msg_id)

            # Texte
            body = ""
            for body_selector in ["[class*='body']", "[class*='Body']", "p", ".message-text"]:
                body_el = msg.select_one(body_selector)
                if body_el:
                    body = body_el.get_text(strip=True)
                    break

            if not body:
                body = msg.get_text(strip=True)[:500]

            if not body or len(body) < 5:
                continue

            # Sentiment
            human_label = None
            msg_html = str(msg).lower()

            if "bullish" in msg_html:
                human_label = "Bullish"
            elif "bearish" in msg_html:
                human_label = "Bearish"

            # Likes
            likes = 0
            likes_el = msg.select_one("[class*='like'] span, [class*='Like'] span")
            if likes_el:
                match = re.search(r"(\d+)", likes_el.get_text())
                if match:
                    likes = int(match.group(1))

            # Timestamp
            time_el = msg.select_one("time, [datetime]")
            timestamp = time_el.get("datetime") if time_el else None

            posts.append({
                "id": msg_id,
                "title": body[:500],
                "text": "",
                "score": likes,
                "created_utc": timestamp,
                "source": "stocktwits",
                "method": "selenium",
                "human_label": human_label
            })

        except Exception:
            continue

    return posts