"""
Reddit Scraper - HTTP et Selenium
"""

import requests
import time
import random

# Limites pour eviter le ban
LIMITS = {
    "http": 1000,      # Reddit JSON API limite a ~1000 posts
    "selenium": 200    # Plus lent, limite pour eviter detection
}


def scrape_reddit_http(subreddit: str, limit: int = 100) -> list:
    """Scrape Reddit via HTTP/JSON (rapide)"""
    posts = []
    after = None
    headers = {"User-Agent": "Mozilla/5.0 Chrome/120.0.0.0"}
    
    limit = min(limit, LIMITS["http"])
    
    while len(posts) < limit:
        url = f"https://old.reddit.com/r/{subreddit}/new.json"
        params = {"limit": min(100, limit - len(posts))}
        if after:
            params["after"] = after
        
        try:
            resp = requests.get(url, headers=headers, params=params, timeout=15)
            data = resp.json()
        except Exception as e:
            print(f"Erreur Reddit HTTP: {e}")
            break
        
        children = data.get("data", {}).get("children", [])
        if not children:
            break
        
        for child in children:
            d = child.get("data", {})
            posts.append({
                "id": d.get("id"),
                "title": d.get("title", ""),
                "text": d.get("selftext", ""),
                "score": d.get("score", 0),
                "created_utc": d.get("created_utc"),
                "source": "reddit",
                "method": "http",
                "human_label": None
            })
            if len(posts) >= limit:
                break
        
        after = data.get("data", {}).get("after")
        if not after:
            break
        
        time.sleep(0.3)
    
    return posts


def scrape_reddit_selenium(subreddit: str, limit: int = 100) -> list:
    """Scrape Reddit via Selenium (simule navigateur)"""
    try:
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.chrome.options import Options
        from selenium.common.exceptions import NoSuchElementException
        from bs4 import BeautifulSoup
    except ImportError:
        print("Selenium non installe")
        return []
    
    limit = min(limit, LIMITS["selenium"])
    posts = []
    
    # Config Chrome
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--window-size=1920,1080")
    
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) Chrome/120.0.0.0",
    ]
    options.add_argument(f"--user-agent={random.choice(user_agents)}")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    
    try:
        driver = webdriver.Chrome(options=options)
    except Exception as e:
        print(f"Erreur Chrome: {e}")
        return []
    
    try:
        url = f"https://old.reddit.com/r/{subreddit}/new/"
        print(f"Selenium: Loading {url}...")
        driver.get(url)
        time.sleep(random.uniform(2, 4))
        
        pages = 0
        max_pages = (limit // 25) + 2
        
        while len(posts) < limit and pages < max_pages:
            # Scroll
            for _ in range(2):
                px = random.randint(300, 600)
                driver.execute_script(f"window.scrollBy(0, {px});")
                time.sleep(random.uniform(0.5, 1.0))
            
            # Parse HTML
            soup = BeautifulSoup(driver.page_source, "lxml")
            elements = soup.select("div.thing.link")
            
            for elem in elements:
                if "stickied" in elem.get("class", []):
                    continue
                if "promoted" in elem.get("class", []):
                    continue
                
                post_id = elem.get("data-fullname", "")
                if any(p["id"] == post_id for p in posts):
                    continue
                
                title_el = elem.select_one("a.title")
                title = title_el.get_text(strip=True) if title_el else ""
                
                score_el = elem.select_one("div.score.unvoted")
                score_txt = score_el.get_text(strip=True) if score_el else "0"
                try:
                    score = int(score_txt) if score_txt != "•" else 0
                except:
                    score = 0
                
                time_el = elem.select_one("time")
                timestamp = time_el.get("datetime", "") if time_el else ""
                
                if title:
                    posts.append({
                        "id": post_id,
                        "title": title,
                        "text": "",
                        "score": score,
                        "created_utc": timestamp,
                        "source": "reddit",
                        "method": "selenium",
                        "human_label": None
                    })
                
                if len(posts) >= limit:
                    break
            
            # Page suivante
            try:
                next_btn = driver.find_element(By.CSS_SELECTOR, "span.next-button a")
                next_btn.click()
                time.sleep(random.uniform(2, 3))
                pages += 1
            except NoSuchElementException:
                break
        
        print(f"Selenium: {len(posts)} posts scraped")
        
    except Exception as e:
        print(f"Erreur Selenium Reddit: {e}")
    finally:
        driver.quit()
    
    return posts[:limit]


def scrape_reddit(subreddit: str, limit: int = 100, method: str = "http") -> list:
    """
    Scrape Reddit avec la methode choisie
    
    Args:
        subreddit: Nom du subreddit
        limit: Nombre de posts
        method: "http" (rapide, max 1000) ou "selenium" (lent, max 200)
    """
    if method == "selenium":
        return scrape_reddit_selenium(subreddit, limit)
    else:
        return scrape_reddit_http(subreddit, limit)


def get_limits():
    """Retourne les limites par methode"""
    return LIMITS
