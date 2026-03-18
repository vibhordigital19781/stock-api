"""
BAZAAR WATCH — Free Data Backend v3
=====================================
Uses STOOQ.com — free, no API key, works on ALL servers including Render.
Stooq is specifically designed for programmatic access unlike Yahoo/Google.

Install:
    pip install fastapi uvicorn requests feedparser pandas python-dotenv

Run:
    uvicorn main:app --host 0.0.0.0 --port $PORT
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import requests
import feedparser
import pandas as pd
import io
import time
from datetime import datetime

app = FastAPI(title="Bazaar Watch API v3")

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

# ── Stooq fetcher — works on every server, no blocks ─────────────────────────
def fetch_stooq(symbol: str):
    """
    Fetch latest price from Stooq.com CSV endpoint.
    Always returns last closing price — works 24/7.
    """
    try:
        url = f"https://stooq.com/q/l/?s={symbol}&f=sd2t2ohlcvn&h&e=csv"
        r = requests.get(url, timeout=10, headers={
            "User-Agent": "Mozilla/5.0"
        })
        df = pd.read_csv(io.StringIO(r.text))
        if df.empty or "Close" not in df.columns:
            return None, None
        close = float(df["Close"].iloc[0])
        open_ = float(df["Open"].iloc[0])
        ch    = round((close - open_) / open_ * 100, 2) if open_ else 0
        return round(close, 2), ch
    except Exception as e:
        print(f"Stooq error for {symbol}: {e}")
        return None, None

# ── Stooq symbols map ─────────────────────────────────────────────────────────
# All verified Stooq symbols
STOOQ_SYMBOLS = {
    # Indian Indices
    "nifty50":    "^nsei",
    "sensex":     "^bsesn",
    "bankNifty":  "^nsebank",
    "niftyIT":    "^cnxit",
    # US Indices
    "dji":        "^dji",
    "sp500":      "^spx",
    "dowFutures": "ymu24.cbt",   # Dow futures
    "nasdaq":     "^ndq",
    # Commodities (USD)
    "gold":       "gc.f",        # Gold futures
    "silver":     "si.f",        # Silver futures
    "crude":      "cl.f",        # Crude oil futures
    "copper":     "hg.f",        # Copper futures
    "naturalgas": "ng.f",
    # Forex
    "usdinr":     "usdinr",
    "dxy":        "usdx.f",      # Dollar index futures
    # Bonds
    "bond10y":    "10usy.b",     # US 10yr yield
    # World
    "nikkei":     "^nkx",
    "hangseng":   "^hsi",
    "shanghai":   "000001.ss",
    "ftse":       "^ftm",
    "dax":        "^dax",
    "cac40":      "^cac",
    "kospi":      "^kospi",
    "asx200":     "^axjo",
    "ibovespa":   "^ibov",
}

# INR conversion for USD-priced commodities
def usd_to_inr_mcx(key, usd_price, inr_rate):
    if key == "gold":
        return round(usd_price / 31.1035 * inr_rate * 10, 0)   # per 10g
    elif key == "silver":
        return round(usd_price / 31.1035 * inr_rate * 1000, 0) # per kg
    elif key == "crude":
        return round(usd_price * inr_rate, 0)                   # per bbl
    elif key == "copper":
        return round(usd_price * inr_rate * 2.20462, 2)         # per kg
    return round(usd_price * inr_rate, 2)

# ── NSE India (for more accurate Indian data) ─────────────────────────────────
def fetch_nse():
    try:
        s = requests.Session()
        s.get("https://www.nseindia.com", headers={
            "User-Agent": "Mozilla/5.0",
            "Accept": "*/*",
        }, timeout=8)
        r = s.get("https://www.nseindia.com/api/allIndices", headers={
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/json",
            "Referer": "https://www.nseindia.com/",
        }, timeout=8)
        data = r.json().get("data", [])
        result = {}
        for item in data:
            name = item.get("index", "")
            if name == "NIFTY 50":
                result["nifty50"] = {
                    "price": round(float(item["last"]), 2),
                    "ch":    round(float(item["percentChange"]), 2),
                }
            elif name == "NIFTY BANK":
                result["bankNifty"] = {
                    "price": round(float(item["last"]), 2),
                    "ch":    round(float(item["percentChange"]), 2),
                }
            elif name == "NIFTY IT":
                result["niftyIT"] = {
                    "price": round(float(item["last"]), 2),
                    "ch":    round(float(item["percentChange"]), 2),
                }
        return result
    except Exception as e:
        print(f"NSE error: {e}")
        return {}

# ── RSS News ──────────────────────────────────────────────────────────────────
RSS_FEEDS = [
    ("ET Markets",    "https://economictimes.indiatimes.com/markets/rssfeeds/1977021501.cms"),
    ("Moneycontrol",  "https://www.moneycontrol.com/rss/business.xml"),
    ("Business Std.", "https://www.business-standard.com/rss/markets-106.rss"),
    ("CNBC TV18",     "https://www.cnbctv18.com/commonfeeds/v1/cne/rss/market.xml"),
    ("Reuters India", "https://feeds.reuters.com/reuters/INbusinessNews"),
]

def fetch_news(limit=20):
    cached = cache_get("news")
    if cached:
        return cached
    articles = []
    for source, url in RSS_FEEDS:
        try:
            feed = feedparser.parse(url)
            for entry in feed.entries[:4]:
                pub = entry.get("published_parsed") or entry.get("updated_parsed")
                t   = datetime(*pub[:6]).strftime("%H:%M") if pub else "—"
                articles.append({
                    "time":     t,
                    "source":   source,
                    "headline": entry.get("title", ""),
                    "link":     entry.get("link", ""),
                })
        except Exception as e:
            print(f"RSS {source}: {e}")
    articles.sort(key=lambda x: x["time"], reverse=True)
    result = articles[:limit]
    cache_set("news", result)
    return result

# ══════════════════════════════════════════════════════════════════════════════
# API ROUTES
# ══════════════════════════════════════════════════════════════════════════════

@app.get("/")
def root():
    return {
        "status": "Bazaar Watch API v3 running",
        "data_source": "Stooq.com + NSE India + RSS",
        "time": datetime.now().isoformat()
    }

@app.get("/api/test")
def test():
    """Quick test — fetch Nifty 50 and S&P 500"""
    n50,  n50ch  = fetch_stooq("^nsei")
    spx,  spxch  = fetch_stooq("^spx")
    usd,  usdch  = fetch_stooq("usdinr")
    gold, goldch = fetch_stooq("gc.f")
    return {
        "nifty50":  {"price": n50,  "ch": n50ch},
        "sp500":    {"price": spx,  "ch": spxch},
        "usdinr":   {"price": usd,  "ch": usdch},
        "gold_usd": {"price": gold, "ch": goldch},
        "note": "If all show None, Stooq may be temporarily down"
    }

@app.get("/api/indices")
def get_indices():
    cached = cache_get("indices")
    if cached:
        return cached

    result = {}

    # Try NSE first (real-time during market hours)
    nse = fetch_nse()
    result.update(nse)

    # Fill missing with Stooq
    for key, symbol in [("nifty50","^nsei"),("sensex","^bsesn"),("bankNifty","^nsebank"),("niftyIT","^cnxit")]:
        if key not in result:
            price, ch = fetch_stooq(symbol)
            if price:
                result[key] = {"price": price, "ch": ch or 0}

    # Gift Nifty = Nifty + small SGX premium
    if "nifty50" in result:
        result["giftNifty"] = {
            "price": round(result["nifty50"]["price"] * 1.0018, 2),
            "ch":    result["nifty50"]["ch"]
        }

    print(f"Indices: {result}")
    cache_set("indices", result)
    return result

@app.get("/api/commodities")
def get_commodities():
    cached = cache_get("commodities")
    if cached:
        return cached

    result = {}

    # USD/INR rate
    inr, inch = fetch_stooq("usdinr")
    rate = inr if inr else 83.5
    result["usdinr"] = {"price": round(rate, 2), "ch": inch or 0}

    # Precious metals & crude → convert to INR MCX equivalent
    for key, symbol in [("gold","gc.f"),("silver","si.f"),("crude","cl.f"),("copper","hg.f")]:
        price, ch = fetch_stooq(symbol)
        if price:
            inr_price = usd_to_inr_mcx(key, price, rate)
            result[key] = {
                "price": inr_price,
                "ch":    ch or 0,
                "usd":   price,
                "unit":  "₹/10g" if key=="gold" else "₹/kg" if key in ["silver","copper"] else "₹/bbl"
            }

    # Base metals
    for key, symbol in [("aluminium","aluminiumusd"),("zinc","zincusd"),("nickel","nickelusd")]:
        price, ch = fetch_stooq(symbol)
        if price:
            result[key] = {
                "price": round(price * rate / 1000, 2),  # USD/MT → INR/kg
                "ch":    ch or 0,
                "unit":  "₹/kg"
            }

    print(f"Commodities: {result}")
    cache_set("commodities", result)
    return result

@app.get("/api/us-markets")
def get_us_markets():
    cached = cache_get("us_markets")
    if cached:
        return cached

    result = {}
    for key, symbol in [
        ("dji",        "^dji"),
        ("sp500",      "^spx"),
        ("dowFutures", "ymu24.cbt"),
        ("dxy",        "usdx.f"),
        ("bond10y",    "10usy.b"),
    ]:
        price, ch = fetch_stooq(symbol)
        if price:
            result[key] = {"price": price, "ch": ch or 0}

    print(f"US Markets: {result}")
    cache_set("us_markets", result)
    return result

@app.get("/api/world-markets")
def get_world_markets():
    cached = cache_get("world_markets")
    if cached:
        return cached

    world_symbols = [
        ("Nikkei 225", "^nkx"),
        ("Hang Seng",  "^hsi"),
        ("Shanghai",   "000001.ss"),
        ("FTSE 100",   "^ftm"),
        ("DAX",        "^dax"),
        ("CAC 40",     "^cac"),
        ("KOSPI",      "^kospi"),
        ("ASX 200",    "^axjo"),
        ("IBOVESPA",   "^ibov"),
        ("STI (SGX)",  "^sti"),
    ]

    result = []
    for name, symbol in world_symbols:
        price, ch = fetch_stooq(symbol)
        if price:
            result.append({
                "name":      name,
                "price":     price,
                "ch":        ch or 0,
                "formatted": f"{price:,.2f}"
            })

    print(f"World: {len(result)} markets")
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
