"""
Bazaar Watch — Backend API v5
=============================
Data sources (all confirmed working on cloud servers):

  Indian Indices  → NseIndiaApi (server=True, HTTP/2) — confirmed works on AWS/Render
  Sensex          → BSE unofficial endpoint
  Gold / Silver   → gold-api.com (free, no key)
  Base Metals     → metals.dev ($1.49/month or 100 free/month)
  Crude Oil       → Finnhub OANDA (free key)
  US Markets      → Finnhub DIA/SPY/QQQ (free key)
  News            → RSS feeds (always free)
  Gift Nifty      → NOT available via any free server-side API
"""

import os, json, time, asyncio, logging
from datetime import datetime
from functools import lru_cache
import httpx
import feedparser
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("bazaar")

# ── API KEYS ──────────────────────────────────────────────────────────────────
FINNHUB_KEY  = os.getenv("FINNHUB_KEY",  "d6ubijpr01qp1k9busogd6ubijpr01qp1k9busp0")
METALS_KEY   = os.getenv("METALS_KEY",   "URTHHJXMHCOMH9ULHDJT872ULHDJT")
FRONTEND_URL = os.getenv("FRONTEND_URL", "*")

app = FastAPI(title="Bazaar Watch API")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

# ── IN-MEMORY CACHE ───────────────────────────────────────────────────────────
cache = {
    "indices":    {"data": {}, "ts": 0},
    "metals":     {"data": {}, "ts": 0},
    "us":         {"data": {}, "ts": 0},
    "news":       {"data": [], "ts": 0},
}
CACHE_TTL = 60  # seconds

# ── NSE INDIA — HTTP/2 session (confirmed works on Render/AWS) ────────────────
# Uses httpx with HTTP/2 — bypasses NSE's HTTP/1.1 block on cloud servers
NSE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Referer": "https://www.nseindia.com/",
    "Origin": "https://www.nseindia.com",
}

async def get_nse_session() -> httpx.AsyncClient:
    """Create an httpx HTTP/2 client with NSE cookies."""
    client = httpx.AsyncClient(http2=True, headers=NSE_HEADERS, timeout=15.0)
    try:
        # Hit homepage first to get cookies — required by NSE
        await client.get("https://www.nseindia.com/", follow_redirects=True)
        await asyncio.sleep(1)
    except Exception as e:
        log.warning(f"NSE session init: {e}")
    return client

async def fetch_nse_indices() -> dict:
    """Fetch all NSE indices using HTTP/2."""
    result = {}
    try:
        async with await get_nse_session() as client:
            r = await client.get(
                "https://www.nseindia.com/api/allIndices",
                headers={**NSE_HEADERS, "X-Requested-With": "XMLHttpRequest"}
            )
            data = r.json().get("data", [])
            
        INDEX_MAP = {
            "NIFTY 50":          "nifty50",
            "NIFTY BANK":        "banknifty",
            "NIFTY IT":          "niftyit",
            "NIFTY MIDCAP 100":  "midcap100",
            "NIFTY NEXT 50":     "niftynext50",
            "NIFTY PHARMA":      "niftypharma",
            "NIFTY AUTO":        "niftyauto",
            "INDIA VIX":         "indiavix",
        }
        for item in data:
            key = INDEX_MAP.get(item.get("index", ""))
            if key:
                result[key] = {
                    "price":   round(float(item.get("last", 0)), 2),
                    "change":  round(float(item.get("change", 0)), 2),
                    "pchange": round(float(item.get("percentChange", 0)), 2),
                    "open":    round(float(item.get("open", 0)), 2),
                    "high":    round(float(item.get("yearHigh", 0)), 2),
                    "low":     round(float(item.get("yearLow", 0)), 2),
                    "prev":    round(float(item.get("previousClose", 0)), 2),
                }
        log.info(f"✅ NSE indices fetched: {list(result.keys())}")

    except Exception as e:
        log.error(f"❌ NSE fetch failed: {e}")

    # Sensex via BSE
    try:
        async with httpx.AsyncClient(http2=True, timeout=10) as client:
            r = await client.get("https://api.bseindia.com/BseIndiaAPI/api/GetSensexData/w")
            d = r.json()
            price = float(d.get("CurrValue", 0))
            prev  = float(d.get("PrevClose", price))
            ch    = round(price - prev, 2)
            pch   = round((ch / prev * 100) if prev else 0, 2)
            result["sensex"] = {
                "price": round(price, 2), "change": ch,
                "pchange": pch, "prev": round(prev, 2),
            }
            log.info(f"✅ Sensex: {price}")
    except Exception as e:
        log.error(f"❌ Sensex fetch failed: {e}")

    return result


async def fetch_metals() -> dict:
    """
    Gold + Silver from gold-api.com (free, no key)
    Base metals from metals.dev (100 free/month, $1.49 paid)
    """
    result = {}
    async with httpx.AsyncClient(timeout=10) as client:

        # ── Gold ────────────────────────────────────────────────────────────
        try:
            r = await client.get("https://www.gold-api.com/price/XAU")
            d = r.json()
            result["gold_usd"] = {
                "price":   round(float(d.get("price", 0)), 2),
                "pchange": round(float(d.get("chp", 0)), 2),
                "change":  round(float(d.get("ch", 0)), 2),
            }
            log.info(f"✅ Gold: {result['gold_usd']['price']}")
        except Exception as e:
            log.error(f"❌ Gold fetch: {e}")

        # ── Silver ──────────────────────────────────────────────────────────
        try:
            r = await client.get("https://www.gold-api.com/price/XAG")
            d = r.json()
            result["silver_usd"] = {
                "price":   round(float(d.get("price", 0)), 2),
                "pchange": round(float(d.get("chp", 0)), 2),
                "change":  round(float(d.get("ch", 0)), 2),
            }
            log.info(f"✅ Silver: {result['silver_usd']['price']}")
        except Exception as e:
            log.error(f"❌ Silver fetch: {e}")

        # ── Base metals + more from metals.dev ──────────────────────────────
        if METALS_KEY:
            try:
                r = await client.get(
                    f"https://api.metals.dev/v1/latest?api_key={METALS_KEY}&currency=USD&unit=toz"
                )
                d = r.json().get("metals", {})
                metal_map = {
                    "copper":    "copper_usd",
                    "aluminum":  "aluminium_usd",
                    "zinc":      "zinc_usd",
                    "nickel":    "nickel_usd",
                    "platinum":  "platinum_usd",
                    "palladium": "palladium_usd",
                    "lead":      "lead_usd",
                }
                for src, dst in metal_map.items():
                    if src in d:
                        result[dst] = {
                            "price":   round(float(d[src]), 4),
                            "pchange": 0,
                            "source":  "metals.dev"
                        }
                # Also get MCX-linked gold/silver from metals.dev as backup
                if "gold" in d and "gold_usd" not in result:
                    result["gold_usd"] = {"price": round(float(d["gold"]), 2), "pchange": 0}
                if "silver" in d and "silver_usd" not in result:
                    result["silver_usd"] = {"price": round(float(d["silver"]), 4), "pchange": 0}
                log.info(f"✅ metals.dev: {list(result.keys())}")
            except Exception as e:
                log.error(f"❌ metals.dev: {e}")

        # ── Crude Oil via Finnhub OANDA ──────────────────────────────────────
        if FINNHUB_KEY:
            try:
                r = await client.get(
                    f"https://finnhub.io/api/v1/quote?symbol=OANDA:USOIL&token={FINNHUB_KEY}"
                )
                d = r.json()
                if d.get("c"):
                    result["crude_usd"] = {
                        "price":   round(float(d["c"]), 2),
                        "pchange": round(float(d.get("dp", 0)), 2),
                        "high":    round(float(d.get("h", 0)), 2),
                        "low":     round(float(d.get("l", 0)), 2),
                    }
                    log.info(f"✅ Crude: {result['crude_usd']['price']}")
            except Exception as e:
                log.error(f"❌ Crude fetch: {e}")

    return result


async def fetch_us_markets() -> dict:
    """US markets via Finnhub — DIA/SPY/QQQ ETFs (free tier confirmed working)."""
    result = {}
    if not FINNHUB_KEY:
        log.warning("No FINNHUB_KEY set — skipping US markets")
        return result

    symbols = {
        "DIA": "dow",    # Dow Jones ETF
        "SPY": "sp500",  # S&P 500 ETF
        "QQQ": "nasdaq", # Nasdaq ETF
    }

    async with httpx.AsyncClient(timeout=10) as client:
        for sym, key in symbols.items():
            try:
                r = await client.get(
                    f"https://finnhub.io/api/v1/quote?symbol={sym}&token={FINNHUB_KEY}"
                )
                d = r.json()
                if d.get("c"):
                    result[key] = {
                        "price":   round(float(d["c"]), 2),
                        "pchange": round(float(d.get("dp", 0)), 2),
                        "change":  round(float(d.get("d", 0)), 2),
                        "high":    round(float(d.get("h", 0)), 2),
                        "low":     round(float(d.get("l", 0)), 2),
                        "symbol":  sym,
                    }
                    log.info(f"✅ {sym}: {result[key]['price']}")
                await asyncio.sleep(0.2)  # rate limit friendly
            except Exception as e:
                log.error(f"❌ Finnhub {sym}: {e}")

        # Dollar Index
        try:
            r = await client.get(
                f"https://finnhub.io/api/v1/quote?symbol=OANDA:USD_IDX&token={FINNHUB_KEY}"
            )
            d = r.json()
            if d.get("c"):
                result["dxy"] = {
                    "price":   round(float(d["c"]), 3),
                    "pchange": round(float(d.get("dp", 0)), 2),
                }
        except Exception as e:
            log.error(f"❌ DXY: {e}")

    return result


async def fetch_news() -> list:
    """RSS feeds — always free, always working."""
    feeds = [
        "https://economictimes.indiatimes.com/markets/rss.cms",
        "https://www.moneycontrol.com/rss/latestnews.xml",
        "https://www.business-standard.com/rss/markets-106.rss",
    ]
    items = []
    for url in feeds:
        try:
            feed = feedparser.parse(url)
            for e in feed.entries[:5]:
                items.append({
                    "title":  e.get("title", ""),
                    "link":   e.get("link", ""),
                    "source": feed.feed.get("title", ""),
                    "time":   e.get("published", ""),
                })
        except Exception as e:
            log.error(f"RSS {url}: {e}")
    return items[:20]


# ── BACKGROUND REFRESH ─────────────────────────────────────────────────────────
async def refresh_cache():
    """Refresh all data sources in background every 60s."""
    while True:
        try:
            log.info("🔄 Refreshing cache...")

            indices = await fetch_nse_indices()
            if indices:
                cache["indices"] = {"data": indices, "ts": time.time()}

            metals = await fetch_metals()
            if metals:
                cache["metals"] = {"data": metals, "ts": time.time()}

            us = await fetch_us_markets()
            if us:
                cache["us"] = {"data": us, "ts": time.time()}

            news = await fetch_news()
            if news:
                cache["news"] = {"data": news, "ts": time.time()}

            log.info("✅ Cache refreshed")
        except Exception as e:
            log.error(f"Cache refresh error: {e}")

        await asyncio.sleep(60)


@app.on_event("startup")
async def startup():
    asyncio.create_task(refresh_cache())


# ── API ENDPOINTS ──────────────────────────────────────────────────────────────
@app.get("/")
def root():
    return {"status": "Bazaar Watch API v5 running", "time": datetime.now().isoformat()}


@app.get("/api/all")
def get_all():
    return JSONResponse({
        "indices":   cache["indices"]["data"],
        "metals":    cache["metals"]["data"],
        "us":        cache["us"]["data"],
        "news":      cache["news"]["data"],
        "timestamp": datetime.now().isoformat(),
        "cache_age": {
            "indices": round(time.time() - cache["indices"]["ts"]) if cache["indices"]["ts"] else None,
            "metals":  round(time.time() - cache["metals"]["ts"])  if cache["metals"]["ts"]  else None,
            "us":      round(time.time() - cache["us"]["ts"])      if cache["us"]["ts"]      else None,
        }
    })


@app.get("/api/indices")
def get_indices():
    return JSONResponse(cache["indices"]["data"])


@app.get("/api/metals")
def get_metals():
    return JSONResponse(cache["metals"]["data"])


@app.get("/api/us")
def get_us():
    return JSONResponse(cache["us"]["data"])


@app.get("/api/news")
def get_news():
    return JSONResponse(cache["news"]["data"])


@app.get("/api/health")
def health():
    return {
        "status": "ok",
        "finnhub_key": "set" if FINNHUB_KEY else "missing",
        "metals_key":  "set" if METALS_KEY  else "missing (base metals disabled)",
        "cache": {k: {"age_s": round(time.time()-v["ts"]) if v["ts"] else None} for k,v in cache.items()}
    }


# ── GIFT NIFTY — using investiny package (confirmed working) ─────────────────
# investiny uses tvc6.investing.com correctly — handles URL hash + headers
# pip install investiny  (add to requirements.txt)
# Gift Nifty 50 Futures investing.com ID = 2283114

GIFT_NIFTY_ID = 2283114

INTERVAL_MAP = {
    "1":  "1",
    "5":  "5",
    "15": "15",
    "30": "30",
    "60": "60",
    "D":  "D",
}

async def fetch_gift_nifty_ohlc(resolution: str = "5", bars: int = 200) -> list:
    """
    Fetch Gift Nifty OHLC using investiny package.
    investiny uses tvc6.investing.com — confirmed working on cloud servers.
    """
    try:
        from investiny import historical_data
        from datetime import datetime, timedelta
        import asyncio

        # Calculate date range — go back enough to get required bars
        minutes_per_bar = {"1":1,"5":5,"15":15,"30":30,"60":60,"D":1440}.get(resolution, 5)
        days_needed = max(3, (bars * minutes_per_bar) // (6 * 60) + 2)
        to_date   = datetime.now()
        from_date = to_date - timedelta(days=days_needed)

        from_str = from_date.strftime("%m/%d/%Y")
        to_str   = to_date.strftime("%m/%d/%Y")

        # Run in thread pool since investiny uses sync requests
        loop = asyncio.get_event_loop()
        raw = await loop.run_in_executor(
            None,
            lambda: historical_data(
                investing_id=GIFT_NIFTY_ID,
                from_date=from_str,
                to_date=to_str,
                interval=INTERVAL_MAP.get(resolution, "5")
            )
        )

        # investiny returns {"Open":[], "High":[], "Low":[], "Close":[], "Volume":[], "Date":[]}
        dates  = raw.get("Date",   [])
        opens  = raw.get("Open",   [])
        highs  = raw.get("High",   [])
        lows   = raw.get("Low",    [])
        closes = raw.get("Close",  [])
        vols   = raw.get("Volume", [])

        result = []
        for i in range(len(dates)):
            try:
                # Parse date to timestamp
                dt = datetime.strptime(str(dates[i])[:19], "%Y-%m-%d %H:%M:%S")
                ts = int(dt.timestamp())
            except:
                ts = i
            result.append({
                "time":   ts,
                "open":   round(float(opens[i]),  2),
                "high":   round(float(highs[i]),  2),
                "low":    round(float(lows[i]),   2),
                "close":  round(float(closes[i]), 2),
                "volume": round(float(vols[i]),   2) if i < len(vols) else 0,
            })

        # Sort by time ascending (required by lightweight-charts)
        result.sort(key=lambda x: x["time"])
        log.info(f"✅ Gift Nifty via investiny: {len(result)} candles")
        return result[-bars:]  # Return last N bars

    except Exception as e:
        log.error(f"❌ Gift Nifty investiny failed: {e}")
        return []


@app.get("/api/giftnifty")
async def get_gift_nifty(resolution: str = "5", bars: int = 200):
    """Gift Nifty OHLC for lightweight-charts. ?resolution=1|5|15|30|60|D"""
    data = await fetch_gift_nifty_ohlc(resolution, int(bars))
    return JSONResponse({
        "symbol":     "Gift Nifty 50 Futures",
        "pair_id":    GIFT_NIFTY_ID,
        "resolution": resolution,
        "bars":       len(data),
        "data":       data,
        "timestamp":  datetime.now().isoformat(),
    })


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main_v5:app", host="0.0.0.0", port=8000, reload=False)
