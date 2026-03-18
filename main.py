"""
BAZAAR WATCH — Free Data Backend v2
=====================================
Uses only FREE data sources with no IP restrictions:
  • Google Finance  → All market prices (works on any server)
  • NSE India JSON  → Indian indices (near real-time)
  • RSS feeds       → Live news (ET Markets, Moneycontrol etc.)

Install:
    pip install fastapi uvicorn requests feedparser beautifulsoup4 python-dotenv

Run:
    uvicorn main:app --host 0.0.0.0 --port $PORT
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import requests
import feedparser
import time
import re
from datetime import datetime

app = FastAPI(title="Bazaar Watch API v2")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Cache ─────────────────────────────────────────────────────────────────────
_cache = {}
CACHE_TTL = 60

def cache_get(key):
    if key in _cache:
        data, ts = _cache[key]
        if time.time() - ts < CACHE_TTL:
            return data
    return None

def cache_set(key, data):
    _cache[key] = (data, time.time())

# ── Headers ───────────────────────────────────────────────────────────────────
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}

# ── Google Finance scraper ────────────────────────────────────────────────────
def fetch_google_finance(ticker: str, exchange: str = ""):
    """
    Fetch price from Google Finance.
    Examples:
      fetch_google_finance("NSEI", "INDEXNSE")
      fetch_google_finance("DJI", "INDEXDJX")
      fetch_google_finance("INR", "CURRENCY")
    """
    try:
        if exchange:
            url = f"https://www.google.com/finance/quote/{ticker}:{exchange}"
        else:
            url = f"https://www.google.com/finance/quote/{ticker}"

        r = requests.get(url, headers=HEADERS, timeout=10)
        html = r.text

        # Extract price using regex patterns
        # Google Finance shows price in a specific div
        price_patterns = [
            r'"(\d{1,3}(?:,\d{3})*(?:\.\d+)?)"[^>]*data-last-price',
            r'data-last-price="([^"]+)"',
            r'class="YMlKec fxKbKc"[^>]*>([^<]+)<',
            r'class="kf1m0"[^>]*>\s*([0-9,\.]+)',
        ]

        price = None
        for pattern in price_patterns:
            match = re.search(pattern, html)
            if match:
                try:
                    price = float(match.group(1).replace(",", ""))
                    break
                except:
                    continue

        # Extract change percentage
        ch_patterns = [
            r'([+-]?\d+\.?\d*)\s*%',
            r'data-pct-change="([^"]+)"',
        ]

        ch = 0.0
        for pattern in ch_patterns:
            match = re.search(pattern, html)
            if match:
                try:
                    ch = float(match.group(1))
                    break
                except:
                    continue

        if price:
            return round(price, 2), round(ch, 2)
        return None, None

    except Exception as e:
        print(f"Google Finance error for {ticker}: {e}")
        return None, None


# ── NSE India ─────────────────────────────────────────────────────────────────
NSE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/json",
    "Referer": "https://www.nseindia.com/",
    "Accept-Language": "en-US,en;q=0.9",
}

def fetch_nse_indices():
    """Fetch Indian indices from NSE official website."""
    try:
        session = requests.Session()
        session.get("https://www.nseindia.com", headers=NSE_HEADERS, timeout=10)
        time.sleep(1)
        r = session.get(
            "https://www.nseindia.com/api/allIndices",
            headers=NSE_HEADERS, timeout=10
        )
        data = r.json().get("data", [])
        result = {}
        index_map = {
            "NIFTY 50":    "nifty50",
            "NIFTY BANK":  "bankNifty",
            "NIFTY IT":    "niftyIT",
        }
        for item in data:
            key = index_map.get(item.get("index", ""))
            if key:
                price = float(item.get("last", 0))
                ch    = float(item.get("percentChange", 0))
                result[key] = {
                    "price": round(price, 2),
                    "ch":    round(ch, 2),
                    "high":  round(float(item.get("dayHigh", 0)), 2),
                    "low":   round(float(item.get("dayLow", 0)), 2),
                }
        return result
    except Exception as e:
        print(f"NSE error: {e}")
        return {}


# ── MCX Gold/Silver prices ─────────────────────────────────────────────────────
def fetch_mcx_prices():
    """Fetch MCX commodity prices from moneycontrol (free, no key needed)."""
    try:
        result = {}

        # Gold MCX
        r = requests.get(
            "https://priceapi.moneycontrol.com/pricefeed/commodity/M/GOLD",
            headers=HEADERS, timeout=8
        )
        d = r.json()
        if d.get("data"):
            price = float(d["data"].get("pricecurrent", 0))
            prev  = float(d["data"].get("priceprevclose", price))
            ch    = round((price - prev) / prev * 100, 2) if prev else 0
            result["gold"] = {"price": round(price, 0), "ch": ch, "unit": "₹/10g"}

        # Silver MCX
        r = requests.get(
            "https://priceapi.moneycontrol.com/pricefeed/commodity/M/SILVER",
            headers=HEADERS, timeout=8
        )
        d = r.json()
        if d.get("data"):
            price = float(d["data"].get("pricecurrent", 0))
            prev  = float(d["data"].get("priceprevclose", price))
            ch    = round((price - prev) / prev * 100, 2) if prev else 0
            result["silver"] = {"price": round(price, 0), "ch": ch, "unit": "₹/kg"}

        # Crude MCX
        r = requests.get(
            "https://priceapi.moneycontrol.com/pricefeed/commodity/M/CRUDEOIL",
            headers=HEADERS, timeout=8
        )
        d = r.json()
        if d.get("data"):
            price = float(d["data"].get("pricecurrent", 0))
            prev  = float(d["data"].get("priceprevclose", price))
            ch    = round((price - prev) / prev * 100, 2) if prev else 0
            result["crude"] = {"price": round(price, 0), "ch": ch, "unit": "₹/bbl"}

        return result

    except Exception as e:
        print(f"MCX error: {e}")
        return {}


# ── RSS News ──────────────────────────────────────────────────────────────────
RSS_FEEDS = [
    ("ET Markets",    "https://economictimes.indiatimes.com/markets/rssfeeds/1977021501.cms"),
    ("Moneycontrol",  "https://www.moneycontrol.com/rss/business.xml"),
    ("Business Std.", "https://www.business-standard.com/rss/markets-106.rss"),
    ("CNBC TV18",     "https://www.cnbctv18.com/commonfeeds/v1/cne/rss/market.xml"),
    ("Reuters India", "https://feeds.reuters.com/reuters/INbusinessNews"),
]

def fetch_news(limit: int = 20):
    cached = cache_get("news")
    if cached:
        return cached
    articles = []
    for source, url in RSS_FEEDS:
        try:
            feed = feedparser.parse(url)
            for entry in feed.entries[:4]:
                published = entry.get("published_parsed") or entry.get("updated_parsed")
                time_str = datetime(*published[:6]).strftime("%H:%M") if published else "—"
                articles.append({
                    "time":     time_str,
                    "source":   source,
                    "headline": entry.get("title", ""),
                    "link":     entry.get("link", ""),
                    "summary":  entry.get("summary", "")[:120] if entry.get("summary") else "",
                })
        except Exception as e:
            print(f"RSS error {source}: {e}")
    articles.sort(key=lambda x: x["time"], reverse=True)
    result = articles[:limit]
    cache_set("news", result)
    return result


# ══════════════════════════════════════════════════════════════════════════════
# API ROUTES
# ══════════════════════════════════════════════════════════════════════════════

@app.get("/")
def root():
    return {"status": "Bazaar Watch API running", "time": datetime.now().isoformat()}


@app.get("/api/indices")
def get_indices():
    cached = cache_get("indices")
    if cached:
        return cached

    result = {}

    # Try NSE first (most accurate)
    nse = fetch_nse_indices()
    result.update(nse)

    # Sensex from Google Finance
    price, ch = fetch_google_finance("SENSEX", "INDEXBOM")
    if price:
        result["sensex"] = {"price": price, "ch": ch or 0}

    # Gift Nifty proxy — Nifty + small premium
    if "nifty50" in result:
        result["giftNifty"] = {
            "price": round(result["nifty50"]["price"] * 1.002, 2),
            "ch":    result["nifty50"]["ch"]
        }

    print(f"Indices result: {result}")
    cache_set("indices", result)
    return result


@app.get("/api/commodities")
def get_commodities():
    cached = cache_get("commodities")
    if cached:
        return cached

    result = {}

    # USD/INR from Google Finance
    price, ch = fetch_google_finance("USD-INR", "")
    if price:
        result["usdinr"] = {"price": price, "ch": ch or 0}
    else:
        result["usdinr"] = {"price": 83.5, "ch": 0}

    rate = result["usdinr"]["price"]

    # MCX prices (INR directly)
    mcx = fetch_mcx_prices()
    result.update(mcx)

    # Base metals from Google Finance (USD) → convert to INR
    metals = {
        "copper":    ("COPPER", "COMMODITY"),
        "aluminium": ("ALU",    "COMMODITY"),
        "zinc":      ("ZINC",   "COMMODITY"),
        "nickel":    ("NICKEL", "COMMODITY"),
    }
    for key, (ticker, exchange) in metals.items():
        price, ch = fetch_google_finance(ticker, exchange)
        if price:
            result[key] = {
                "price": round(price * rate * 2.20462, 2),
                "ch":    ch or 0,
                "unit":  "₹/kg"
            }

    print(f"Commodities result: {result}")
    cache_set("commodities", result)
    return result


@app.get("/api/us-markets")
def get_us_markets():
    cached = cache_get("us_markets")
    if cached:
        return cached

    result = {}

    markets = {
        "dji":        ("DJI",    "INDEXDJX"),
        "sp500":      ("INX",    "INDEXSP"),
        "dowFutures": ("YMW00",  "CBOT"),
        "dxy":        ("DXY",    ""),
        "bond10y":    ("US10Y",  ""),
    }

    for key, (ticker, exchange) in markets.items():
        price, ch = fetch_google_finance(ticker, exchange)
        if price:
            result[key] = {"price": price, "ch": ch or 0}

    print(f"US Markets result: {result}")
    cache_set("us_markets", result)
    return result


@app.get("/api/world-markets")
def get_world_markets():
    cached = cache_get("world_markets")
    if cached:
        return cached

    world = [
        ("Nikkei 225",  "NI225",   "INDEXNIKKEI"),
        ("Hang Seng",   "HSI",     "INDEXHANGSENG"),
        ("Shanghai",    "000001",  "SHA"),
        ("FTSE 100",    "UKX",     "INDEXFTSE"),
        ("DAX",         "DAX",     "INDEXDB"),
        ("CAC 40",      "PX1",     "INDEXEURONEXT"),
        ("KOSPI",       "KOSPI",   "INDEXKRX"),
        ("ASX 200",     "XJO",     "INDEXASX"),
        ("Taiwan Wt.",  "TWII",    "INDEXTAIEX"),
        ("Jakarta",     "COMPOSITE","INDEXIDX"),
        ("STI (SGX)",   "STI",     "INDEXFTSE"),
        ("IBOVESPA",    "IBOV",    "INDEXBVMF"),
    ]

    result = []
    for name, ticker, exchange in world:
        price, ch = fetch_google_finance(ticker, exchange)
        if price:
            result.append({
                "name":      name,
                "price":     price,
                "ch":        ch or 0,
                "formatted": f"{price:,.2f}"
            })

    print(f"World markets: {len(result)} fetched")
    cache_set("world_markets", result)
    return result


@app.get("/api/news")
def get_news():
    return fetch_news(limit=20)


@app.get("/api/all")
def get_all():
    return {
        "indices":      get_indices(),
        "commodities":  get_commodities(),
        "usMarkets":    get_us_markets(),
        "worldMarkets": get_world_markets(),
        "news":         fetch_news(limit=15),
        "lastUpdated":  datetime.now().isoformat(),
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)

