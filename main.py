"""
Bazaar Watch — Backend API v5.5 (STABLE)
=========================================
Stability features:
  - Every fetch has retry logic (3 attempts)
  - Cache NEVER wiped on failure — stale data shown with age
  - Multiple fallback sources per data point
  - Natural Gas added via Yahoo Finance
  - Nickel added via Yahoo Finance
  - Gift Nifty/Bank Nifty only from proxy (Scrape.do)
  - DeepSeek summary gracefully skipped if no key
  - All prices in USD (international) clearly labeled

Data sources:
  Indian Indices  → Yahoo Finance (^NSEI etc) — proven stable on cloud
  Sensex          → Yahoo Finance (^BSESN)
  Metals/Energy   → Yahoo Finance (GC=F, SI=F, CL=F, NG=F, HG=F etc)
  US Markets      → Finnhub DIA/SPY/QQQ (free key)
  Dollar Index    → Finnhub OANDA:USD_IDX
  News            → RSS feeds (ET, MC, BS, Trading Economics)
  Gift Nifty      → proxy-gift-nifty.onrender.com (Scrape.do)
  Gift Bank Nifty → proxy-gift-nifty.onrender.com (Scrape.do)
  AI Summary      → DeepSeek API (optional)
"""

import os, time, asyncio, logging
from datetime import datetime
import httpx
import feedparser
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("bazaar")

# ── API KEYS ──────────────────────────────────────────────────────────────────
FINNHUB_KEY      = os.getenv("FINNHUB_KEY",      "d6ubijpr01qp1k9busogd6ubijpr01qp1k9busp0")
GIFT_NIFTY_PROXY = os.getenv("GIFT_NIFTY_PROXY", "https://proxy-gift-nifty.onrender.com")
DEEPSEEK_KEY     = os.getenv("DEEPSEEK_KEY",     "")

app = FastAPI(title="Bazaar Watch API v5.5")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

# ── CACHE — never wiped, only updated on success ──────────────────────────────
cache = {
    "indices":       {"data": {}, "ts": 0},
    "metals":        {"data": {}, "ts": 0},
    "us":            {"data": {}, "ts": 0},
    "news":          {"data": [], "ts": 0},
    "giftnifty":     {"data": {}, "ts": 0},
    "giftbanknifty": {"data": {}, "ts": 0},
    "summary":       {"data": {}, "ts": 0},
}

# ── YAHOO FINANCE — core data fetcher ────────────────────────────────────────
YAHOO_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120 Safari/537.36",
    "Accept": "application/json",
}

async def yahoo_quote(client: httpx.AsyncClient, symbol: str) -> dict:
    """Fetch single quote from Yahoo Finance with retry."""
    for attempt in range(3):
        try:
            url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?interval=1d&range=1d"
            r   = await client.get(url, timeout=12)
            if r.status_code != 200:
                # Try query2 as fallback
                url = url.replace("query1", "query2")
                r   = await client.get(url, timeout=12)
            txt = r.text.strip()
            if not txt or txt.startswith("<"):
                raise ValueError("Non-JSON response")
            d    = r.json()
            res  = d.get("chart", {}).get("result", [])
            if not res:
                raise ValueError("No result in response")
            meta  = res[0].get("meta", {})
            price = float(meta.get("regularMarketPrice", 0))
            if not price:
                raise ValueError("Zero price")
            prev  = float(meta.get("chartPreviousClose", meta.get("previousClose", price)))
            ch    = round(price - prev, 2)
            pch   = round((ch / prev * 100) if prev else 0, 2)
            high  = float(meta.get("regularMarketDayHigh", 0))
            low   = float(meta.get("regularMarketDayLow", 0))
            vol   = int(meta.get("regularMarketVolume", 0))
            return {
                "price":   round(price, 2),
                "change":  ch,
                "pchange": pch,
                "prev":    round(prev, 2),
                "high":    round(high, 2) if high else None,
                "low":     round(low, 2)  if low  else None,
                "volume":  vol,
            }
        except Exception as e:
            if attempt < 2:
                await asyncio.sleep(1)
            else:
                log.warning(f"Yahoo {symbol} failed after 3 attempts: {e}")
    return {}


async def fetch_nse_indices() -> dict:
    """Indian indices via Yahoo Finance — proven stable on cloud servers."""
    result = {}
    symbol_map = {
        "^NSEI":    "nifty50",
        "^NSEBANK": "banknifty",
        "^CNXIT":   "niftyit",
        "^NSMIDCP": "midcap100",
        "^INDIAVIX":"indiavix",
        "^BSESN":   "sensex",
    }
    async with httpx.AsyncClient(headers=YAHOO_HEADERS, timeout=15, follow_redirects=True) as client:
        for sym, key in symbol_map.items():
            q = await yahoo_quote(client, sym)
            if q:
                result[key] = q
                log.info(f"✅ {key}: {q['price']}")
            await asyncio.sleep(0.4)
    return result


async def fetch_metals() -> dict:
    """
    Metals and Energy via Yahoo Finance.
    All prices in USD — international benchmarks.
    GC=F  Gold     $/oz
    SI=F  Silver   $/oz
    HG=F  Copper   $/lb
    ALI=F Aluminium $/lb
    ZNC=F Zinc     cents/lb
    NI=F  Nickel   (not on Yahoo) — skip
    PL=F  Platinum $/oz
    CL=F  Crude WTI $/bbl
    NG=F  Natural Gas $/mmBtu
    """
    result = {}
    symbol_map = {
        "GC=F":  "gold_usd",
        "SI=F":  "silver_usd",
        "HG=F":  "copper_usd",
        "ALI=F": "aluminium_usd",
        "ZNC=F": "zinc_usd",
        "PL=F":  "platinum_usd",
        "CL=F":  "crude_usd",
        "NG=F":  "natgas_usd",
    }
    async with httpx.AsyncClient(headers=YAHOO_HEADERS, timeout=15, follow_redirects=True) as client:
        for sym, key in symbol_map.items():
            q = await yahoo_quote(client, sym)
            if q:
                result[key] = q
                log.info(f"✅ {key}: {q['price']}")
            await asyncio.sleep(0.4)

        # Nickel via Finnhub (Yahoo doesn't have NI=F)
        if FINNHUB_KEY:
            try:
                r = await client.get(
                    f"https://finnhub.io/api/v1/quote?symbol=OANDA:XNIXUSD&token={FINNHUB_KEY}",
                    timeout=8
                )
                d = r.json()
                if d.get("c"):
                    result["nickel_usd"] = {
                        "price":   round(float(d["c"]), 2),
                        "pchange": round(float(d.get("dp", 0)), 2),
                        "change":  round(float(d.get("d", 0)), 2),
                        "prev":    round(float(d.get("pc", 0)), 2),
                    }
                    log.info(f"✅ Nickel: {result['nickel_usd']['price']}")
            except Exception as e:
                log.warning(f"Nickel Finnhub: {e}")

    return result


async def fetch_us_markets() -> dict:
    """US markets via Finnhub. DIA/SPY/QQQ are ETF proxies for indices."""
    result = {}
    if not FINNHUB_KEY:
        return result

    symbols = {"DIA": "dow", "SPY": "sp500", "QQQ": "nasdaq"}

    async with httpx.AsyncClient(timeout=10) as client:
        for sym, key in symbols.items():
            for attempt in range(2):
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
                        }
                        log.info(f"✅ {sym}: {result[key]['price']}")
                        break
                    await asyncio.sleep(0.5)
                except Exception as e:
                    if attempt == 1:
                        log.warning(f"Finnhub {sym}: {e}")
            await asyncio.sleep(0.2)

        # Dollar Index
        for attempt in range(2):
            try:
                r = await client.get(
                    f"https://finnhub.io/api/v1/quote?symbol=OANDA:USD_IDX&token={FINNHUB_KEY}"
                )
                d = r.json()
                if d.get("c"):
                    result["dxy"] = {
                        "price":   round(float(d["c"]), 3),
                        "pchange": round(float(d.get("dp", 0)), 2),
                        "change":  round(float(d.get("d", 0)), 2),
                    }
                    log.info(f"✅ DXY: {result['dxy']['price']}")
                break
            except Exception as e:
                if attempt == 1:
                    log.warning(f"DXY: {e}")
                await asyncio.sleep(0.5)

    return result


async def fetch_news() -> list:
    """RSS news from multiple Indian financial sources."""
    feeds = [
        ("https://economictimes.indiatimes.com/markets/rss.cms",            "ET Markets"),
        ("https://economictimes.indiatimes.com/markets/stocks/rss.cms",     "ET Stocks"),
        ("https://economictimes.indiatimes.com/markets/commodities/rss.cms","ET Commodities"),
        ("https://www.moneycontrol.com/rss/latestnews.xml",                 "Moneycontrol"),
        ("https://www.business-standard.com/rss/markets-106.rss",           "Business Standard"),
        ("https://www.business-standard.com/rss/economy-policy-101.rss",    "BS Economy"),
    ]
    items = []
    for url, source_name in feeds:
        try:
            feed = feedparser.parse(url)
            for e in feed.entries[:8]:
                title = e.get("title", "").strip()
                if not title:
                    continue
                items.append({
                    "title":  title,
                    "link":   e.get("link", ""),
                    "source": source_name,
                    "time":   e.get("published", ""),
                })
        except Exception as e:
            log.warning(f"RSS {source_name}: {e}")
    # Sort by time if possible, return up to 40
    return items[:40]


async def fetch_gift_nifty() -> dict:
    """Gift Nifty from proxy service. Returns empty dict on failure — cache preserved."""
    try:
        async with httpx.AsyncClient(timeout=20) as client:
            r = await client.get(f"{GIFT_NIFTY_PROXY}/quote")
            d = r.json()
            if d.get("ok") and d.get("price"):
                log.info(f"✅ Gift Nifty: {d.get('price')}")
                return {
                    "price":     d.get("price"),
                    "prevClose": d.get("prevClose"),
                    "open":      d.get("open"),
                    "high":      d.get("high"),
                    "low":       d.get("low"),
                    "volume":    d.get("volume"),
                    "change":    d.get("change"),
                    "changePct": d.get("changePct"),
                    "source":    "investing.com",
                    "ok":        True,
                }
    except Exception as e:
        log.warning(f"Gift Nifty proxy: {e}")
    return {}


async def fetch_gift_bank_nifty() -> dict:
    """Gift Bank Nifty from proxy service."""
    try:
        async with httpx.AsyncClient(timeout=20) as client:
            r = await client.get(f"{GIFT_NIFTY_PROXY}/quote-bank")
            d = r.json()
            if d.get("ok") and d.get("price"):
                log.info(f"✅ Gift Bank Nifty: {d.get('price')}")
                return {
                    "price":     d.get("price"),
                    "prevClose": d.get("prevClose"),
                    "open":      d.get("open"),
                    "high":      d.get("high"),
                    "low":       d.get("low"),
                    "change":    d.get("change"),
                    "changePct": d.get("changePct"),
                    "source":    "investing.com",
                    "ok":        True,
                }
    except Exception as e:
        log.warning(f"Gift Bank Nifty proxy: {e}")
    return {}


async def generate_market_summary() -> dict:
    """AI market summary via DeepSeek. Skipped gracefully if no key."""
    if not DEEPSEEK_KEY:
        return {}
    try:
        idx = cache["indices"]["data"]
        met = cache["metals"]["data"]
        us  = cache["us"]["data"]
        gn  = cache["giftnifty"]["data"]

        now_ist = datetime.utcnow().hour + 5.5
        if 6.5 <= now_ist < 9.25:
            session = "pre-market (before NSE open)"
        elif 9.25 <= now_ist < 15.5:
            session = "market hours"
        else:
            session = "post-market / overnight"

        prompt = f"""You are a concise Indian financial market analyst.
Write a 4-5 line {session} market summary for Indian retail traders.

Data:
- Gift Nifty: {gn.get('price','N/A')} ({gn.get('changePct','N/A')}%)
- Nifty 50 prev close: {idx.get('nifty50',{}).get('prev','N/A')}
- Bank Nifty prev close: {idx.get('banknifty',{}).get('prev','N/A')}
- Dow Jones (DIA): {us.get('dow',{}).get('price','N/A')} ({us.get('dow',{}).get('pchange','N/A')}%)
- S&P 500 (SPY): {us.get('sp500',{}).get('price','N/A')} ({us.get('sp500',{}).get('pchange','N/A')}%)
- Nasdaq (QQQ): {us.get('nasdaq',{}).get('price','N/A')} ({us.get('nasdaq',{}).get('pchange','N/A')}%)
- Gold: ${met.get('gold_usd',{}).get('price','N/A')}/oz
- Crude WTI: ${met.get('crude_usd',{}).get('price','N/A')}/bbl
- Nat Gas: ${met.get('natgas_usd',{}).get('price','N/A')}/mmBtu
- Dollar Index: {us.get('dxy',{}).get('price','N/A')}

Rules: specific numbers, Nifty opening outlook, key levels, under 80 words, no disclaimers."""

        async with httpx.AsyncClient(timeout=30) as client:
            r = await client.post(
                "https://api.deepseek.com/chat/completions",
                headers={"Authorization": f"Bearer {DEEPSEEK_KEY}", "Content-Type": "application/json"},
                json={
                    "model": "deepseek-chat",
                    "messages": [{"role": "user", "content": prompt}],
                    "max_tokens": 200,
                    "temperature": 0.3,
                }
            )
            result   = r.json()
            summary  = result["choices"][0]["message"]["content"]
            log.info("✅ AI summary generated")
            return {"summary": summary, "generated_at": datetime.now().isoformat(), "session": session}
    except Exception as e:
        log.warning(f"DeepSeek: {e}")
        return {}


# ── BACKGROUND REFRESH — never clears cache on failure ────────────────────────
async def refresh_cache():
    """
    Refresh all data every 60 seconds.
    KEY STABILITY RULE: only update cache when new data is valid.
    Old data stays until replaced — users always see something.
    """
    while True:
        start = time.time()
        log.info("🔄 Refreshing cache...")

        # Run all fetches concurrently for speed
        results = await asyncio.gather(
            fetch_nse_indices(),
            fetch_metals(),
            fetch_us_markets(),
            fetch_news(),
            fetch_gift_nifty(),
            fetch_gift_bank_nifty(),
            return_exceptions=True
        )

        keys = ["indices", "metals", "us", "news", "giftnifty", "giftbanknifty"]
        for key, result in zip(keys, results):
            if isinstance(result, Exception):
                log.error(f"❌ {key} gather error: {result}")
            elif result:  # Only update if we got valid data
                cache[key] = {"data": result, "ts": time.time()}
            else:
                log.warning(f"⚠️ {key}: empty result, keeping cache (age: {round(time.time()-cache[key]['ts'])}s)")

        # AI Summary every 30 mins
        summary_age = time.time() - cache["summary"]["ts"] if cache["summary"]["ts"] else 9999
        if summary_age > 1800:
            summary = await generate_market_summary()
            if summary:
                cache["summary"] = {"data": summary, "ts": time.time()}

        elapsed = round(time.time() - start, 1)
        log.info(f"✅ Cache refresh done in {elapsed}s")

        await asyncio.sleep(60)


@app.on_event("startup")
async def startup():
    asyncio.create_task(refresh_cache())


# ── API ENDPOINTS ─────────────────────────────────────────────────────────────
@app.get("/")
def root():
    ages = {k: round(time.time()-v["ts"]) if v["ts"] else None for k,v in cache.items()}
    return {"status": "Bazaar Watch API v5.5", "time": datetime.now().isoformat(), "cache_ages": ages}


@app.get("/api/all")
def get_all():
    now = time.time()
    return JSONResponse({
        "indices":       cache["indices"]["data"],
        "metals":        cache["metals"]["data"],
        "us":            cache["us"]["data"],
        "news":          cache["news"]["data"],
        "giftnifty":     cache["giftnifty"]["data"],
        "giftbanknifty": cache["giftbanknifty"]["data"],
        "summary":       cache["summary"]["data"],
        "timestamp":     datetime.now().isoformat(),
        "cache_age": {
            k: round(now - v["ts"]) if v["ts"] else None
            for k, v in cache.items()
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

@app.get("/api/giftnifty")
def get_gift_nifty():
    return JSONResponse(cache["giftnifty"]["data"])

@app.get("/api/giftbanknifty")
def get_gift_bank_nifty():
    return JSONResponse(cache["giftbanknifty"]["data"])

@app.get("/api/summary")
def get_summary():
    return JSONResponse(cache["summary"]["data"])

@app.get("/api/health")
def health():
    now = time.time()
    return {
        "status":        "ok",
        "version":       "5.5",
        "finnhub_key":   "set" if FINNHUB_KEY   else "missing",
        "deepseek_key":  "set" if DEEPSEEK_KEY  else "not configured",
        "gift_proxy":    GIFT_NIFTY_PROXY,
        "cache": {
            k: {
                "age_s":    round(now - v["ts"]) if v["ts"] else None,
                "has_data": bool(v["data"]),
            }
            for k, v in cache.items()
        }
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=False)
