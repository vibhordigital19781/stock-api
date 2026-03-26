"""
Bazaar Watch — Backend API v5.4
================================
Data sources:

  Indian Indices  → NseIndiaApi (HTTP/2) — confirmed works on Render
  Sensex          → BSE unofficial endpoint
  Gold / Silver   → gold-api.com (free, no key)
  Base Metals     → metals.dev ($1.49/month or 100 free/month)
  Crude Oil       → Finnhub OANDA (free key)
  US Markets      → Finnhub DIA/SPY/QQQ (free key)
  News            → RSS feeds (always free)
  Gift Nifty      → proxy-gift-nifty.onrender.com (ScraperAPI + investing.com)
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
FINNHUB_KEY       = os.getenv("FINNHUB_KEY",       "d6ubijpr01qp1k9busogd6ubijpr01qp1k9busp0")
METALS_KEY        = os.getenv("METALS_KEY",        "URTHHJXMHCOMH9ULHDJT872ULHDJT")
FRONTEND_URL      = os.getenv("FRONTEND_URL",      "*")
GIFT_NIFTY_PROXY  = os.getenv("GIFT_NIFTY_PROXY",  "https://proxy-gift-nifty.onrender.com")
DEEPSEEK_KEY      = os.getenv("DEEPSEEK_KEY",      "")
GIFT_BANK_PAIR_ID = "1209768"  # Gift Bank Nifty on investing.com

app = FastAPI(title="Bazaar Watch API")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

# ── IN-MEMORY CACHE ───────────────────────────────────────────────────────────
cache = {
    "indices":       {"data": {}, "ts": 0},
    "metals":        {"data": {}, "ts": 0},
    "us":            {"data": {}, "ts": 0},
    "news":          {"data": [], "ts": 0},
    "giftnifty":     {"data": {}, "ts": 0},
    "giftbanknifty": {"data": {}, "ts": 0},
    "summary":       {"data": {}, "ts": 0},
}
CACHE_TTL = 60  # seconds

# ── NSE INDIA — HTTP/2 session ────────────────────────────────────────────────
NSE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Referer": "https://www.nseindia.com/",
    "Origin": "https://www.nseindia.com",
}

async def get_nse_session() -> httpx.AsyncClient:
    client = httpx.AsyncClient(http2=True, headers=NSE_HEADERS, timeout=15.0)
    try:
        await client.get("https://www.nseindia.com/", follow_redirects=True)
        await asyncio.sleep(1)
    except Exception as e:
        log.warning(f"NSE session init: {e}")
    return client

async def yahoo_quote(client: httpx.AsyncClient, symbol: str) -> dict:
    """Fetch a single quote from Yahoo Finance. Works on all cloud servers."""
    try:
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?interval=1d&range=1d"
        r   = await client.get(url, timeout=10)
        txt = r.text.strip()
        if not txt: return {}
        d   = r.json()
        res = d.get("chart", {}).get("result", [])
        if not res: return {}
        meta = res[0].get("meta", {})
        price = float(meta.get("regularMarketPrice", 0))
        prev  = float(meta.get("chartPreviousClose", meta.get("previousClose", price)))
        ch    = round(price - prev, 2)
        pch   = round((ch / prev * 100) if prev else 0, 2)
        return {"price": round(price, 2), "change": ch, "pchange": pch, "prev": round(prev, 2)}
    except Exception as e:
        log.error(f"Yahoo {symbol}: {e}")
        return {}


async def fetch_nse_indices() -> dict:
    """Fetch Indian indices via Yahoo Finance — works on all cloud servers."""
    result = {}
    symbol_map = {
        "^NSEI":    "nifty50",
        "^NSEBANK": "banknifty",
        "^CNXIT":   "niftyit",
        "^NSMIDCP": "midcap100",
        "^INDIAVIX":"indiavix",
        "^BSESN":   "sensex",
    }
    async with httpx.AsyncClient(
        headers={"User-Agent": "Mozilla/5.0"},
        timeout=15, follow_redirects=True
    ) as client:
        for sym, key in symbol_map.items():
            q = await yahoo_quote(client, sym)
            if q:
                result[key] = q
                log.info(f"✅ {key}: {q['price']}")
            await asyncio.sleep(0.3)
    return result


async def fetch_metals() -> dict:
    """Fetch metals via Yahoo Finance — free, no quota, works on Render."""
    result = {}
    symbol_map = {
        "GC=F":  "gold_usd",
        "SI=F":  "silver_usd",
        "HG=F":  "copper_usd",
        "ALI=F": "aluminium_usd",
        "ZNC=F": "zinc_usd",
        "CL=F":  "crude_usd",
        "PL=F":  "platinum_usd",
        "PA=F":  "palladium_usd",
    }
    async with httpx.AsyncClient(
        headers={"User-Agent": "Mozilla/5.0"},
        timeout=15, follow_redirects=True
    ) as client:
        for sym, key in symbol_map.items():
            q = await yahoo_quote(client, sym)
            if q:
                result[key] = q
                log.info(f"✅ {key}: {q['price']}")
            await asyncio.sleep(0.3)

        # Also get Nickel and Lead from Finnhub if available
        if FINNHUB_KEY:
            try:
                r = await client.get(
                    f"https://finnhub.io/api/v1/quote?symbol=OANDA:USOIL&token={FINNHUB_KEY}"
                )
                d = r.json()
                if d.get("c"):
                    # Use Finnhub crude as backup if Yahoo failed
                    if "crude_usd" not in result:
                        result["crude_usd"] = {
                            "price":   round(float(d["c"]), 2),
                            "pchange": round(float(d.get("dp", 0)), 2),
                            "change":  round(float(d.get("d", 0)), 2),
                            "prev":    round(float(d.get("pc", 0)), 2),
                        }
                        log.info(f"✅ Crude (Finnhub): {result['crude_usd']['price']}")
            except Exception as e:
                log.error(f"❌ Finnhub crude: {e}")

    return result


async def fetch_us_markets() -> dict:
    result = {}
    if not FINNHUB_KEY:
        log.warning("No FINNHUB_KEY — skipping US markets")
        return result

    symbols = {
        "DIA": "dow",
        "SPY": "sp500",
        "QQQ": "nasdaq",
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
                await asyncio.sleep(0.2)
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
    feeds = [
        "https://economictimes.indiatimes.com/markets/rss.cms",
        "https://economictimes.indiatimes.com/markets/stocks/rss.cms",
        "https://economictimes.indiatimes.com/markets/commodities/rss.cms",
        "https://www.moneycontrol.com/rss/latestnews.xml",
        "https://www.business-standard.com/rss/markets-106.rss",
        "https://www.business-standard.com/rss/economy-policy-101.rss",
        "https://tradingeconomics.com/rss/news.aspx",
    ]
    items = []
    for url in feeds:
        try:
            feed = feedparser.parse(url)
            for e in feed.entries[:8]:
                items.append({
                    "title":  e.get("title", ""),
                    "link":   e.get("link", ""),
                    "source": feed.feed.get("title", ""),
                    "time":   e.get("published", ""),
                })
        except Exception as e:
            log.error(f"RSS {url}: {e}")
    return items[:40]


async def fetch_gift_nifty() -> dict:
    """
    Fetch Gift Nifty live quote from our Node.js proxy service.
    proxy-gift-nifty.onrender.com scrapes investing.com via ScraperAPI.
    Returns: price, prevClose, open, high, low, volume, change, changePct
    """
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            r = await client.get(f"{GIFT_NIFTY_PROXY}/quote")
            d = r.json()
            if d.get("ok"):
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
                }
    except Exception as e:
        log.error(f"❌ Gift Nifty fetch failed: {e}")
    return {}


async def fetch_gift_bank_nifty() -> dict:
    """Fetch Gift Bank Nifty from investing.com via ScraperAPI."""
    import re, json as _json
    try:
        page_url   = f"https://in.investing.com/indices/gift-nifty-bank-c1-futures"
        scraper_key = os.getenv("SCRAPER_API_KEY", "")
        if not scraper_key:
            return {}
        proxy_url = f"http://api.scraperapi.com?api_key={scraper_key}&url={page_url}"
        async with httpx.AsyncClient(timeout=20) as client:
            r    = await client.get(proxy_url)
            html = r.text
        match = re.search(r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>', html, re.DOTALL)
        if not match:
            raise Exception("__NEXT_DATA__ not found")
        data_str = _json.dumps(_json.loads(match.group(1)))
        def get_val(pattern):
            m = re.search(pattern, data_str)
            return float(m.group(1)) if m else None
        price     = get_val(r'"(?:last|lastPrice|currentPrice|price|lastNumeric)"\s*:\s*([\d.]+)')
        prev      = get_val(r'"(?:prevClose|previousClose|lastClose|prev_close)"\s*:\s*([\d.]+)')
        high      = get_val(r'"(?:high|highPrice|dayHigh)"\s*:\s*([\d.]+)')
        low       = get_val(r'"(?:low|lowPrice|dayLow)"\s*:\s*([\d.]+)')
        opn       = get_val(r'"(?:open|openPrice)"\s*:\s*([\d.]+)')
        calc_chg  = round(price - prev, 2) if price and prev else None
        calc_pct  = round((calc_chg / prev * 100), 2) if calc_chg and prev else None
        if not price:
            raise Exception("Price not found")
        log.info(f"✅ Gift Bank Nifty: {price}")
        return {"price": price, "prevClose": prev, "open": opn, "high": high, "low": low,
                "change": calc_chg, "changePct": calc_pct, "source": "investing.com", "ok": True}
    except Exception as e:
        log.error(f"❌ Gift Bank Nifty: {e}")
        return {}


async def generate_market_summary() -> dict:
    """Generate AI market summary using DeepSeek."""
    if not DEEPSEEK_KEY:
        log.warning("No DEEPSEEK_KEY — skipping summary")
        return {}
    try:
        idx = cache["indices"]["data"]
        met = cache["metals"]["data"]
        us  = cache["us"]["data"]
        gn  = cache["giftnifty"]["data"]
        gbn = cache["giftbanknifty"]["data"]

        ist_hour = datetime.now().hour + 5 + (datetime.now().minute + 30) / 60
        session  = "pre-market morning" if 5 <= ist_hour < 9.25 else                    "market hours" if 9.25 <= ist_hour < 15.5 else "post-market/overnight"

        prompt = f"""You are a concise Indian financial market analyst writing for retail traders.
Write a 4-5 line market summary for the current {session} session.

Live Data:
- Gift Nifty 50: {gn.get('price', 'N/A')} (Change: {gn.get('changePct', 'N/A')}%)
- Gift Bank Nifty: {gbn.get('price', 'N/A')} (Change: {gbn.get('changePct', 'N/A')}%)
- Nifty 50 prev close: {idx.get('nifty50', {}).get('prev', 'N/A')}
- Bank Nifty prev close: {idx.get('banknifty', {}).get('prev', 'N/A')}
- Dow Jones: {us.get('dow', {}).get('price', 'N/A')} ({us.get('dow', {}).get('pchange', 'N/A')}%)
- S&P 500: {us.get('sp500', {}).get('price', 'N/A')} ({us.get('sp500', {}).get('pchange', 'N/A')}%)
- Nasdaq: {us.get('nasdaq', {}).get('price', 'N/A')} ({us.get('nasdaq', {}).get('pchange', 'N/A')}%)
- Gold: ${met.get('gold_usd', {}).get('price', 'N/A')}/oz
- Crude Oil: ${met.get('crude_usd', {}).get('price', 'N/A')}/bbl
- Dollar Index: {us.get('dxy', {}).get('price', 'N/A')}

Rules:
- Be specific with numbers
- Mention Nifty opening direction outlook
- Note key levels to watch
- Keep it under 80 words
- No disclaimers or fluff
- Write for Indian retail traders"""

        async with httpx.AsyncClient(timeout=30) as client:
            r = await client.post(
                "https://api.deepseek.com/chat/completions",
                headers={"Authorization": f"Bearer {DEEPSEEK_KEY}", "Content-Type": "application/json"},
                json={"model": "deepseek-chat", "messages": [{"role": "user", "content": prompt}],
                      "max_tokens": 200, "temperature": 0.3}
            )
            result = r.json()
            summary = result["choices"][0]["message"]["content"]
            log.info(f"✅ DeepSeek summary generated")
            return {"summary": summary, "generated_at": datetime.now().isoformat(), "session": session}
    except Exception as e:
        log.error(f"❌ DeepSeek summary failed: {e}")
        return {}


# ── BACKGROUND REFRESH ────────────────────────────────────────────────────────
async def refresh_cache():
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

            giftnifty = await fetch_gift_nifty()
            if giftnifty:
                cache["giftnifty"] = {"data": giftnifty, "ts": time.time()}

            giftbanknifty = await fetch_gift_bank_nifty()
            if giftbanknifty:
                cache["giftbanknifty"] = {"data": giftbanknifty, "ts": time.time()}

            # Generate summary every 30 mins only
            summary_age = time.time() - cache["summary"]["ts"] if cache["summary"]["ts"] else 9999
            if summary_age > 1800:
                summary = await generate_market_summary()
                if summary:
                    cache["summary"] = {"data": summary, "ts": time.time()}

            log.info("✅ Cache refreshed")
        except Exception as e:
            log.error(f"Cache refresh error: {e}")

        await asyncio.sleep(60)


@app.on_event("startup")
async def startup():
    asyncio.create_task(refresh_cache())


# ── API ENDPOINTS ─────────────────────────────────────────────────────────────
@app.get("/")
def root():
    return {"status": "Bazaar Watch API v5.4 running", "time": datetime.now().isoformat()}


@app.get("/api/all")
def get_all():
    return JSONResponse({
        "indices":    cache["indices"]["data"],
        "metals":     cache["metals"]["data"],
        "us":         cache["us"]["data"],
        "news":       cache["news"]["data"],
        "giftnifty":     cache["giftnifty"]["data"],
        "giftbanknifty": cache["giftbanknifty"]["data"],
        "summary":       cache["summary"]["data"],
        "timestamp":  datetime.now().isoformat(),
        "cache_age": {
            "indices":   round(time.time() - cache["indices"]["ts"])   if cache["indices"]["ts"]   else None,
            "metals":    round(time.time() - cache["metals"]["ts"])    if cache["metals"]["ts"]    else None,
            "us":        round(time.time() - cache["us"]["ts"])        if cache["us"]["ts"]        else None,
            "giftnifty": round(time.time() - cache["giftnifty"]["ts"]) if cache["giftnifty"]["ts"] else None,
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
    return {
        "status": "ok",
        "finnhub_key":      "set" if FINNHUB_KEY      else "missing",
        "metals_key":       "set" if METALS_KEY        else "missing",
        "gift_nifty_proxy": GIFT_NIFTY_PROXY,
        "cache": {k: {"age_s": round(time.time()-v["ts"]) if v["ts"] else None} for k,v in cache.items()}
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main_v5-4:app", host="0.0.0.0", port=8000, reload=False)
