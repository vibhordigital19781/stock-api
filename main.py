"""
BAZAAR WATCH — Free Data Backend
=================================
Uses only FREE data sources:
  • yfinance         → Indices, commodities, forex, US markets (15-min delay)
  • NSE India JSON   → Live Indian market data (unofficial, free)
  • RSS feeds        → ET Markets, Moneycontrol, Business Standard news

Install:
    pip install fastapi uvicorn yfinance feedparser requests python-dotenv

Run:
    uvicorn main:app --reload --port 8000

Then open your frontend and point it to http://localhost:8000
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import yfinance as yf
import feedparser
import requests
import asyncio
from datetime import datetime
from functools import lru_cache
import time

app = FastAPI(title="Bazaar Watch API")
@app.get("/debug")
def debug():
    return {"status": "Server alive", "cache": len(_cache)}

# Allow your frontend to call this backend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Simple in-memory cache (avoids hammering free APIs) ───────────────────────
_cache = {}
CACHE_TTL = 60  # seconds — refresh every 60s (free APIs rate-limit heavily)

def cache_get(key):
    if key in _cache:
        data, ts = _cache[key]
        if time.time() - ts < CACHE_TTL:
            return data
    return None

def cache_set(key, data):
    _cache[key] = (data, time.time())

# ── Yahoo Finance symbols ─────────────────────────────────────────────────────
YF_SYMBOLS = {
    # Indian Indices
    "nifty50":    "^NSEI",
    "sensex":     "^BSESN",
    "bankNifty":  "^NSEBANK",
    "niftyIT":    "^CNXIT",
    # Commodities (USD-denominated, we convert to INR below)
    "gold":       "GC=F",
    "silver":     "SI=F",
    "crude":      "CL=F",
    "copper":     "HG=F",
    "aluminium":  "ALI=F",
    "zinc":       "ZNC=F",
    "nickel":     "^LMENIK",  # fallback to MCX scrape if this fails
    # Forex
    "usdinr":     "INR=X",
    "dxy":        "DX-Y.NYB",
    # US Markets
    "dji":        "^DJI",
    "sp500":      "^GSPC",
    "dowFutures": "YM=F",
    "bond10y":    "^TNX",
}

# MCX conversion multipliers (approximate, USD commodity → INR)
# Gold: per troy oz → per 10g   |  Silver: per troy oz → per kg
# Copper/Zinc etc: per lb → per kg
MCX_CONVERT = {
    "gold":      lambda usd, rate: round(usd * rate / 31.1035 * 10, 0),  # per 10g
    "silver":    lambda usd, rate: round(usd * rate / 31.1035 * 1000, 0), # per kg
    "crude":     lambda usd, rate: round(usd * rate, 0),                   # per bbl
    "copper":    lambda usd, rate: round(usd * rate * 2.20462, 2),         # per kg
    "aluminium": lambda usd, rate: round(usd * rate * 2.20462, 2),         # per kg
    "zinc":      lambda usd, rate: round(usd * rate * 2.20462, 2),         # per kg
    "nickel":    lambda usd, rate: round(usd * rate * 2.20462, 2),         # per kg
}

def fetch_yf_price(symbol: str):
    """Fetch latest price and % change from Yahoo Finance."""
    try:
        ticker = yf.Ticker(symbol)
        hist = ticker.history(period="2d", interval="1d")
        if hist.empty or len(hist) < 1:
            return None, None
        latest = float(hist["Close"].iloc[-1])
        prev   = float(hist["Close"].iloc[-2]) if len(hist) >= 2 else latest
        ch     = round((latest - prev) / prev * 100, 2)
        return latest, ch
    except Exception as e:
        print(f"yfinance error for {symbol}: {e}")
        return None, None

def fetch_yf_history(symbol: str, points: int = 24):
    """Fetch intraday history for chart data."""
    try:
        ticker = yf.Ticker(symbol)
        hist = ticker.history(period="1d", interval="15m")
        if hist.empty:
            hist = ticker.history(period="5d", interval="1h")
        closes = hist["Close"].dropna().tolist()[-points:]
        times  = [str(t)[-8:-3] for t in hist.index.tolist()[-points:]]
        return [{"t": t, "v": round(v, 2)} for t, v in zip(times, closes)]
    except Exception as e:
        print(f"yfinance history error for {symbol}: {e}")
        return []

# ── NSE India unofficial endpoints ────────────────────────────────────────────
NSE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/json",
    "Referer": "https://www.nseindia.com/",
}

def fetch_nse_indices():
    """Fetch Nifty 50, Bank Nifty etc from NSE's own website (free, near real-time)."""
    try:
        session = requests.Session()
        # First hit the main page to get cookies
        session.get("https://www.nseindia.com", headers=NSE_HEADERS, timeout=8)
        r = session.get(
            "https://www.nseindia.com/api/allIndices",
            headers=NSE_HEADERS, timeout=8
        )
        data = r.json().get("data", [])
        result = {}
        index_map = {
            "NIFTY 50":         "nifty50",
            "NIFTY BANK":       "bankNifty",
            "NIFTY IT":         "niftyIT",
            "NIFTY NEXT 50":    "niftyNext50",
            "NIFTY MIDCAP 100": "niftyMidcap",
        }
        for item in data:
            key = index_map.get(item.get("index", ""))
            if key:
                result[key] = {
                    "price": round(float(item.get("last", 0)), 2),
                    "ch":    round(float(item.get("percentChange", 0)), 2),
                    "open":  round(float(item.get("open", 0)), 2),
                    "high":  round(float(item.get("dayHigh", 0)), 2),
                    "low":   round(float(item.get("dayLow", 0)), 2),
                }
        return result
    except Exception as e:
        print(f"NSE API error: {e}")
        return {}

def fetch_nse_stock(symbol: str):
    """Fetch individual NSE stock quote."""
    try:
        session = requests.Session()
        session.get("https://www.nseindia.com", headers=NSE_HEADERS, timeout=8)
        r = session.get(
            f"https://www.nseindia.com/api/quote-equity?symbol={symbol}",
            headers=NSE_HEADERS, timeout=8
        )
        d = r.json()
        price_data = d.get("priceInfo", {})
        return {
            "price": round(float(price_data.get("lastPrice", 0)), 2),
            "ch":    round(float(price_data.get("pChange", 0)), 2),
            "open":  round(float(price_data.get("open", 0)), 2),
            "high":  round(float(price_data.get("intraDayHighLow", {}).get("max", 0)), 2),
            "low":   round(float(price_data.get("intraDayHighLow", {}).get("min", 0)), 2),
        }
    except Exception as e:
        print(f"NSE stock error for {symbol}: {e}")
        return None

# ── Free RSS news feeds ───────────────────────────────────────────────────────
RSS_FEEDS = [
    ("ET Markets",    "https://economictimes.indiatimes.com/markets/rssfeeds/1977021501.cms"),
    ("Moneycontrol",  "https://www.moneycontrol.com/rss/business.xml"),
    ("Business Std.", "https://www.business-standard.com/rss/markets-106.rss"),
    ("CNBC TV18",     "https://www.cnbctv18.com/commonfeeds/v1/cne/rss/market.xml"),
    ("Reuters India", "https://feeds.reuters.com/reuters/INbusinessNews"),
]

def fetch_news(limit: int = 20):
    """Fetch news from multiple free RSS feeds."""
    articles = []
    cached = cache_get("news")
    if cached:
        return cached

    for source, url in RSS_FEEDS:
        try:
            feed = feedparser.parse(url)
            for entry in feed.entries[:4]:
                published = entry.get("published_parsed") or entry.get("updated_parsed")
                if published:
                    dt = datetime(*published[:6])
                    time_str = dt.strftime("%H:%M")
                else:
                    time_str = "—"
                articles.append({
                    "time":     time_str,
                    "source":   source,
                    "headline": entry.get("title", ""),
                    "link":     entry.get("link", ""),
                    "summary":  entry.get("summary", "")[:120] if entry.get("summary") else "",
                })
        except Exception as e:
            print(f"RSS error for {source}: {e}")

    # Sort by time descending
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
    """Indian + Gift Nifty data. Uses NSE first, falls back to yfinance."""
    cached = cache_get("indices")
    if cached:
        return cached

    result = {}

    # Try NSE unofficial API first (most accurate, near real-time)
    nse_data = fetch_nse_indices()
    if nse_data:
        result.update(nse_data)

    # Fill gaps with yfinance (15-min delay but reliable)
    for key in ["nifty50", "bankNifty", "niftyIT", "sensex"]:
        if key not in result:
            symbol = YF_SYMBOLS.get(key)
            if symbol:
                price, ch = fetch_yf_price(symbol)
                if price:
                    result[key] = {"price": round(price, 2), "ch": ch or 0}

    # Gift Nifty — use SGX Nifty proxy (NSE SGX)
    # Real Gift Nifty requires NSE IFSC data; use Nifty 50 + small premium as proxy
    if "nifty50" in result:
        n50 = result["nifty50"]["price"]
        result["giftNifty"] = {
            "price": round(n50 * 1.0015, 2),  # ~0.15% SGX premium
            "ch":    result["nifty50"]["ch"]
        }

    cache_set("indices", result)
    return result

@app.get("/api/commodities")
def get_commodities():
    """Gold, Silver, Crude, Base Metals — MCX prices in INR."""
    cached = cache_get("commodities")
    if cached:
        return cached

    # Get USD/INR first
    usdinr_price, usdinr_ch = fetch_yf_price("INR=X")
    rate = usdinr_price if usdinr_price else 83.5

    result = {"usdinr": {"price": round(rate, 2), "ch": usdinr_ch or 0}}

    for key in ["gold", "silver", "crude", "copper", "aluminium", "zinc", "nickel"]:
        symbol = YF_SYMBOLS.get(key)
        if not symbol:
            continue
        price, ch = fetch_yf_price(symbol)
        if price and key in MCX_CONVERT:
            inr_price = MCX_CONVERT[key](price, rate)
            result[key] = {
                "price": inr_price,
                "ch":    ch or 0,
                "usd":   round(price, 4),
                "unit":  "₹/10g" if key=="gold" else "₹/kg" if key in ["silver","copper","aluminium","zinc","nickel"] else "₹/bbl"
            }

    cache_set("commodities", result)
    return result

@app.get("/api/us-markets")
def get_us_markets():
    """DJI, S&P 500, Dow Futures, DXY, 10Y Bond."""
    cached = cache_get("us_markets")
    if cached:
        return cached

    result = {}
    us_symbols = {
        "dji":        "^DJI",
        "sp500":      "^GSPC",
        "dowFutures": "YM=F",
        "dxy":        "DX-Y.NYB",
        "bond10y":    "^TNX",
    }
    for key, symbol in us_symbols.items():
        price, ch = fetch_yf_price(symbol)
        if price:
            result[key] = {"price": round(price, 3 if key=="bond10y" else 2), "ch": ch or 0}

    cache_set("us_markets", result)
    return result

@app.get("/api/world-markets")
def get_world_markets():
    """12 global indices."""
    cached = cache_get("world_markets")
    if cached:
        return cached

    world_symbols = {
        "Nikkei 225":   "^N225",
        "Hang Seng":    "^HSI",
        "Shanghai":     "000001.SS",
        "FTSE 100":     "^FTSE",
        "DAX":          "^GDAXI",
        "CAC 40":       "^FCHI",
        "KOSPI":        "^KS11",
        "ASX 200":      "^AXJO",
        "Taiwan Wt.":   "^TWII",
        "Jakarta":      "^JKSE",
        "STI (SGX)":    "^STI",
        "IBOVESPA":     "^BVSP",
    }

    result = []
    for name, symbol in world_symbols.items():
        price, ch = fetch_yf_price(symbol)
        if price:
            result.append({
                "name": name,
                "price": round(price, 2),
                "ch": ch or 0,
                "formatted": f"{price:,.0f}"
            })

    cache_set("world_markets", result)
    return result

@app.get("/api/chart/{key}")
def get_chart(key: str):
    """Intraday chart data for any instrument."""
    cached = cache_get(f"chart_{key}")
    if cached:
        return cached

    symbol = YF_SYMBOLS.get(key)
    if not symbol:
        return {"error": f"Unknown symbol: {key}"}

    data = fetch_yf_history(symbol, points=30)
    cache_set(f"chart_{key}", data)
    return data

@app.get("/api/heatmap")
def get_heatmap():
    """
    Nifty 500 stock data for heatmap.
    Fetches batch quotes from NSE for top stocks.
    Falls back to yfinance for missing ones.
    """
    cached = cache_get("heatmap")
    if cached:
        return cached

    # Key stocks to fetch (NSE symbols)
    STOCKS = [
        "HDFCBANK","ICICIBANK","SBIN","KOTAKBANK","AXISBANK","INDUSINDBK",
        "TCS","INFY","HCLTECH","WIPRO","TECHM","LTIM",
        "RELIANCE","ONGC","NTPC","POWERGRID","ADANIGREEN","TATAPOWER",
        "BAJFINANCE","BAJAJFINSV","HDFCLIFE","SBILIFE",
        "MARUTI","TATAMOTORS","M&M","BAJAJ-AUTO","EICHERMOT",
        "HINDUNILVR","ITC","NESTLEIND","BRITANNIA",
        "LT","HAL","BEL","SIEMENS","BHEL","POLYCAB",
        "SUNPHARMA","DRREDDY","CIPLA","DIVISLAB","APOLLOHOSP",
        "TATASTEEL","JSWSTEEL","HINDALCO","VEDL","COALINDIA",
        "TITAN","ZOMATO","DMART","TRENT",
        "DLF","GODREJPROP","ULTRACEMCO","GRASIM",
        "BHARTIARTL","PIDILITIND","INDIGO",
    ]

    result = {}
    # Use yfinance batch download — much faster than individual calls
    try:
        symbols_ns = [f"{s}.NS" for s in STOCKS]
        data = yf.download(symbols_ns, period="2d", interval="1d",
                           group_by="ticker", auto_adjust=True, progress=False)
        for s, sym_ns in zip(STOCKS, symbols_ns):
            try:
                closes = data[sym_ns]["Close"].dropna()
                if len(closes) >= 2:
                    latest = float(closes.iloc[-1])
                    prev   = float(closes.iloc[-2])
                    ch     = round((latest - prev) / prev * 100, 2)
                    result[s] = {"price": round(latest, 2), "ch": ch}
            except:
                pass
    except Exception as e:
        print(f"Batch yfinance error: {e}")

    cache_set("heatmap", result)
    return result

@app.get("/api/news")
def get_news():
    """Financial news from free RSS feeds."""
    return fetch_news(limit=20)

@app.get("/api/all")
def get_all():
    """Single endpoint — returns everything at once (reduces frontend requests)."""
    return {
        "indices":      get_indices(),
        "commodities":  get_commodities(),
        "usMarkets":    get_us_markets(),
        "worldMarkets": get_world_markets(),
        "news":         fetch_news(limit=15),
        "lastUpdated":  datetime.now().isoformat(),
    }

# ── Run directly ──────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
