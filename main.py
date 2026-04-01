"""
Bazaar Watch — Backend API v5.7
========================================
Changes from v5.6:
  - NSE corporate announcements scraper added
  - /api/nse-news endpoint
  - NSE news merged into /api/all response
  - NSE news refreshed every 5 minutes in background
  - Uses direct httpx first, Scrape.do session as fallback
"""

import os, time, asyncio, logging, threading
from datetime import datetime, timedelta, timezone
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
CLAUDE_KEY       = os.getenv("CLAUDE_KEY",       "")
SCRAPER_API_KEY  = os.getenv("SCRAPER_API_KEY",  "")

app = FastAPI(title="Bazaar Watch API v5.7")
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
    "nse_news":      {"data": [], "ts": 0},   # ← NEW: NSE corporate announcements
    "giftnifty":     {"data": {}, "ts": 0},
    "giftbanknifty": {"data": {}, "ts": 0},
    "summary":       {"data": {}, "ts": 0},
    "pre_market":    {"data": {}, "ts": 0},
    "hourly":        {"data": {}, "ts": 0},
    "post_market":   {"data": {}, "ts": 0},
    "heatmap":       {"data": [], "ts": 0},
    "sparklines":    {"data": {}, "ts": 0},
    "options":       {"data": {}, "ts": 0},
}

# ── YAHOO FINANCE — core data fetcher ────────────────────────────────────────
YAHOO_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120 Safari/537.36",
    "Accept": "application/json",
}

async def yahoo_quote(client: httpx.AsyncClient, symbol: str) -> dict:
    for attempt in range(3):
        try:
            url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?interval=1d&range=1d"
            r   = await client.get(url, timeout=12)
            if r.status_code != 200:
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

        if FINNHUB_KEY:
            try:
                r = await client.get(
                    f"https://finnhub.io/api/v1/quote?symbol=OANDA:XNIXUSD&token={FINNHUB_KEY}",
                    timeout=8
                )
                d = r.json()
                if d.get("c") and float(d["c"]) > 0:
                    result["nickel_usd"] = {
                        "price":   round(float(d["c"]), 2),
                        "pchange": round(float(d.get("dp", 0)), 2),
                        "change":  round(float(d.get("d", 0)), 2),
                        "prev":    round(float(d.get("pc", 0)), 2),
                    }
            except Exception as e:
                log.warning(f"Nickel Finnhub: {e}")
    return result


async def fetch_us_markets() -> dict:
    result = {}
    symbol_map = {
        "^DJI":     "dow",
        "^GSPC":    "sp500",
        "^IXIC":    "nasdaq",
        "DX-Y.NYB": "dxy",
        "^VIX":     "vix_us",
    }
    async with httpx.AsyncClient(headers=YAHOO_HEADERS, timeout=15, follow_redirects=True) as client:
        for sym, key in symbol_map.items():
            q = await yahoo_quote(client, sym)
            if q:
                result[key] = q
                log.info(f"✅ {key} ({sym}): {q['price']}")
            await asyncio.sleep(0.3)
    return result


async def fetch_news() -> list:
    feeds = [
        ("https://economictimes.indiatimes.com/markets/rss.cms",              "ET Markets"),
        ("https://economictimes.indiatimes.com/markets/commodities/rss.cms",  "ET Commodities"),
        ("https://economictimes.indiatimes.com/economy/rss.cms",              "ET Economy"),
        ("https://economictimes.indiatimes.com/markets/stocks/rss.cms",       "ET Stocks"),
        ("https://www.business-standard.com/rss/markets-106.rss",             "Business Standard"),
        ("https://www.business-standard.com/rss/economy-policy-101.rss",      "BS Economy"),
        ("https://www.moneycontrol.com/rss/economy.xml",                      "Moneycontrol"),
        ("https://feeds.reuters.com/reuters/businessNews",                    "Reuters Business"),
        ("https://feeds.reuters.com/reuters/commoditiesNews",                 "Reuters Commodities"),
        ("https://www.investing.com/rss/news_25.rss",                         "Investing.com"),
        ("https://feeds.a.dj.com/rss/RSSMarketsMain.xml",                     "WSJ Markets"),
        ("https://oilprice.com/rss/main",                                     "OilPrice.com"),
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
    return items[:50]


# ══════════════════════════════════════════════════════════════════════════════
#  NSE CORPORATE ANNOUNCEMENTS SCRAPER  (NEW in v5.7)
# ══════════════════════════════════════════════════════════════════════════════

NSE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    ),
    "Accept":          "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Referer":         "https://www.nseindia.com/",
    "Connection":      "keep-alive",
}

# In-memory NSE session cache (cookies are valid ~5 min)
_nse_session_cookies: dict = {}
_nse_session_ts: float = 0
NSE_SESSION_TTL = 270  # refresh every 4.5 minutes


async def _get_nse_cookies(client: httpx.AsyncClient) -> dict:
    """Get fresh NSE session cookies by visiting the homepage first."""
    global _nse_session_cookies, _nse_session_ts
    now = time.time()
    if _nse_session_cookies and (now - _nse_session_ts) < NSE_SESSION_TTL:
        return _nse_session_cookies
    try:
        # Step 1 — hit homepage to receive cookies
        r1 = await client.get("https://www.nseindia.com", timeout=12)
        cookies = dict(r1.cookies)
        # Step 2 — hit a secondary page to strengthen session
        await asyncio.sleep(0.8)
        r2 = await client.get(
            "https://www.nseindia.com/market-data/live-equity-market",
            cookies=cookies, timeout=10
        )
        cookies.update(dict(r2.cookies))
        _nse_session_cookies = cookies
        _nse_session_ts = now
        log.info(f"✅ NSE session refreshed ({len(cookies)} cookies)")
        return cookies
    except Exception as e:
        log.warning(f"NSE session init: {e}")
        return {}


async def _fetch_nse_api_direct(path: str) -> list:
    """
    Call NSE internal API directly (no proxy).
    Works on most cloud servers — Render included.
    Falls back to empty list on any error.
    """
    url = f"https://www.nseindia.com{path}"
    try:
        async with httpx.AsyncClient(
            headers=NSE_HEADERS, follow_redirects=True, timeout=18
        ) as client:
            cookies = await _get_nse_cookies(client)
            r = await client.get(url, cookies=cookies, timeout=15)

            if r.status_code in (401, 403):
                # Force session refresh and retry once
                global _nse_session_ts
                _nse_session_ts = 0
                cookies = await _get_nse_cookies(client)
                r = await client.get(url, cookies=cookies, timeout=15)

            if r.status_code != 200:
                log.warning(f"NSE direct {path}: HTTP {r.status_code}")
                return []

            payload = r.json()
            if isinstance(payload, list):
                return payload
            if isinstance(payload, dict):
                for key in ("data", "announcements", "results"):
                    if key in payload and isinstance(payload[key], list):
                        return payload[key]
            return []
    except Exception as e:
        log.warning(f"NSE direct {path}: {e}")
        return []


async def _fetch_nse_api_via_scraperapi(path: str) -> list:
    """
    Fallback: route NSE API calls through Scrape.do session.
    Only used if SCRAPER_API_KEY is set and direct call failed.
    """
    if not SCRAPER_API_KEY:
        return []
    from urllib.parse import quote
    import random, string
    sid = "nse-ann-" + "".join(random.choices(string.ascii_lowercase + string.digits, k=6))
    nse_home = "https://www.nseindia.com/"
    nse_url  = f"https://www.nseindia.com{path}"
    try:
        async with httpx.AsyncClient(timeout=35) as client:
            # Step 1: warm session
            await client.get(
                f"https://api.scrape.do?token={SCRAPER_API_KEY}"
                f"&url={quote(nse_home, safe='')}&sessionId={sid}",
                timeout=25
            )
            await asyncio.sleep(1.5)
            # Step 2: hit API
            r = await client.get(
                f"https://api.scrape.do?token={SCRAPER_API_KEY}"
                f"&url={quote(nse_url, safe='')}&sessionId={sid}",
                timeout=25
            )
            if r.status_code != 200:
                return []
            payload = r.json()
            if isinstance(payload, list):
                return payload
            if isinstance(payload, dict):
                for key in ("data", "announcements"):
                    if key in payload and isinstance(payload[key], list):
                        return payload[key]
    except Exception as e:
        log.warning(f"NSE scrape.do {path}: {e}")
    return []


def _classify_announcement(subject: str) -> str:
    """Map announcement subject to a display tag."""
    s = (subject or "").lower()
    if any(k in s for k in ("dividend", "bonus", "split", "buyback", "rights")):
        return "CORP ACTION"
    if any(k in s for k in ("result", "profit", "revenue", "financial", "quarterly",
                             "q1", "q2", "q3", "q4", "annual", "earnings")):
        return "RESULT"
    if any(k in s for k in ("ipo", "listing", "allotment", "offer for sale", "ofs")):
        return "IPO"
    if any(k in s for k in ("sebi", "penalty", "circuit", "ban", "order",
                             "investigation", "show cause", "suspension")):
        return "ALERT"
    if any(k in s for k in ("merger", "acquisition", "stake", "deal", "mou",
                             "agreement", "joint venture", "takeover")):
        return "DEAL"
    if any(k in s for k in ("fii", "fpi", "dii", "institutional", "bulk deal", "block deal")):
        return "FII/DII"
    return "NSE/BSE"


def _parse_nse_date(date_str: str) -> str:
    """Parse NSE date formats to ISO string with IST offset."""
    if not date_str:
        return ""
    for fmt in ("%d-%b-%Y %H:%M:%S", "%d-%b-%Y", "%d-%m-%Y %H:%M:%S", "%d-%m-%Y"):
        try:
            dt = datetime.strptime(date_str.strip(), fmt)
            return dt.strftime("%Y-%m-%dT%H:%M:%S") + "+05:30"
        except ValueError:
            continue
    return date_str  # return raw if unparseable


async def fetch_nse_announcements() -> list:
    """
    Fetch NSE corporate announcements for the last 7 days.
    Tries direct call first; falls back to Scrape.do if configured.
    Returns normalised list of news items.
    """
    today     = datetime.now()
    from_date = (today - timedelta(days=7)).strftime("%d-%m-%Y")
    to_date   = today.strftime("%d-%m-%Y")

    # ── 1. Corporate announcements (results, AGM, SEBI, board outcomes)
    ann_path = (
        f"/api/corporate-announcements?index=equities"
        f"&from_date={from_date}&to_date={to_date}"
    )
    ann_raw = await _fetch_nse_api_direct(ann_path)
    if not ann_raw:
        log.info("NSE direct announcements failed — trying Scrape.do")
        ann_raw = await _fetch_nse_api_via_scraperapi(ann_path)

    # ── 2. Corporate actions (dividends, splits, bonuses, buybacks)
    act_path = (
        f"/api/corporates-corporateActions?index=equities"
        f"&from_date={from_date}&to_date={to_date}"
    )
    act_raw = await _fetch_nse_api_direct(act_path)
    if not act_raw:
        act_raw = await _fetch_nse_api_via_scraperapi(act_path)

    # ── 3. Board meetings (upcoming)
    bm_path = "/api/corporate-board-meetings?index=equities"
    bm_raw  = await _fetch_nse_api_direct(bm_path)
    if not bm_raw:
        bm_raw = await _fetch_nse_api_via_scraperapi(bm_path)

    results = []

    # Normalise announcements
    for item in ann_raw[:80]:
        subject = (item.get("subject") or item.get("desc") or "").strip()
        symbol  = (item.get("symbol") or "").strip()
        company = (item.get("comp") or item.get("companyName") or symbol).strip()
        an_time = item.get("an_dt") or item.get("bm_date") or ""

        if not subject:
            continue

        headline = f"{company}: {subject}" if company and company != subject else subject

        results.append({
            "title":    headline,
            "source":   "NSE India",
            "link":     "https://www.nseindia.com/companies-listing/corporate-filings-announcements",
            "time":     _parse_nse_date(an_time),
            "category": _classify_announcement(subject),
            "symbol":   symbol,
        })

    # Normalise corporate actions
    for item in act_raw[:40]:
        symbol  = (item.get("symbol") or "").strip()
        company = (item.get("comp") or symbol).strip()
        purpose = (item.get("purpose") or item.get("subject") or "").strip()
        exdate  = (item.get("exDate") or item.get("ex_date") or "").strip()

        if not purpose:
            continue

        headline = f"{company}: {purpose}"
        if exdate:
            headline += f" — Ex-date {exdate}"

        results.append({
            "title":    headline,
            "source":   "NSE India",
            "link":     "https://www.nseindia.com/companies-listing/corporate-filings-corporate-actions",
            "time":     _parse_nse_date(exdate),
            "category": _classify_announcement(purpose),
            "symbol":   symbol,
        })

    # Normalise board meetings
    for item in bm_raw[:30]:
        symbol  = (item.get("symbol") or "").strip()
        company = (item.get("companyName") or symbol).strip()
        purpose = (item.get("purpose") or "").strip()
        bm_date = (item.get("meeting_date") or item.get("bm_date") or "").strip()

        if not purpose:
            continue

        headline = f"{company}: Board Meeting — {purpose}"
        if bm_date:
            headline += f" (on {bm_date})"

        results.append({
            "title":    headline,
            "source":   "NSE India",
            "link":     "https://www.nseindia.com/companies-listing/corporate-filings-board-meetings",
            "time":     _parse_nse_date(bm_date),
            "category": "RESULT" if "result" in purpose.lower() else "CORP ACTION",
            "symbol":   symbol,
        })

    # Deduplicate by first 60 chars of title
    seen, unique = set(), []
    for r in results:
        key = r["title"][:60].lower()
        if key not in seen:
            seen.add(key)
            unique.append(r)

    log.info(f"✅ NSE announcements: {len(unique)} items "
             f"(ann={len(ann_raw)}, act={len(act_raw)}, bm={len(bm_raw)})")
    return unique


# ══════════════════════════════════════════════════════════════════════════════
#  REST OF ORIGINAL FUNCTIONS (unchanged)
# ══════════════════════════════════════════════════════════════════════════════

async def fetch_gift_nifty() -> dict:
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
    if not CLAUDE_KEY:
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
                "https://api.anthropic.com/v1/messages",
                headers={"x-api-key": CLAUDE_KEY, "anthropic-version": "2023-06-01", "Content-Type": "application/json"},
                json={
                    "model": "claude-haiku-4-5-20251001",
                    "messages": [{"role": "user", "content": prompt}],
                    "max_tokens": 200,
                    "temperature": 0.3,
                }
            )
            result  = r.json()
            summary = result["content"][0]["text"]
            log.info("✅ AI summary generated")
            return {"summary": summary, "generated_at": datetime.now().isoformat(), "session": session}
    except Exception as e:
        log.warning(f"AI summary: {e}")
        return {}


SPARKLINE_SYMBOLS = {
    "nifty50":   "^NSEI",
    "sensex":    "^BSESN",
    "banknifty": "^NSEBANK",
    "niftyit":   "^CNXIT",
    "midcap":    "^NSMIDCP",
    "indiavix":  "^INDIAVIX",
    "dow":       "^DJI",
    "sp500":     "^GSPC",
    "nasdaq":    "^IXIC",
    "gold":      "GC=F",
    "silver":    "SI=F",
    "crude":     "CL=F",
    "natgas":    "NG=F",
    "copper":    "HG=F",
    "dxy":       "DX-Y.NYB",
    "usdinr":    "USDINR=X",
}

async def fetch_sparklines() -> dict:
    result = {}
    async with httpx.AsyncClient(headers=YAHOO_HEADERS, timeout=20, follow_redirects=True) as client:
        for key, sym in SPARKLINE_SYMBOLS.items():
            try:
                url = f"https://query1.finance.yahoo.com/v8/finance/chart/{sym}?interval=1h&range=1d"
                r   = await client.get(url, timeout=10)
                d   = r.json()
                res = d.get("chart", {}).get("result", [])
                if not res:
                    continue
                closes = res[0].get("indicators", {}).get("quote", [{}])[0].get("close", [])
                pts = [round(v, 2) for v in closes if v is not None]
                if len(pts) >= 2:
                    result[key] = pts
                await asyncio.sleep(0.2)
            except Exception as e:
                log.warning(f"Sparkline {key}: {e}")
    return result


def get_market_context(cache_data: dict) -> str:
    idx = cache_data.get("indices", {})
    met = cache_data.get("metals", {})
    us  = cache_data.get("us", {})
    gn  = cache_data.get("giftnifty", {})
    lines = []
    if idx.get("nifty50"):    lines.append(f"Nifty50={idx['nifty50']['price']} ({idx['nifty50']['pchange']:+.2f}%)")
    if idx.get("sensex"):     lines.append(f"Sensex={idx['sensex']['price']} ({idx['sensex']['pchange']:+.2f}%)")
    if idx.get("banknifty"):  lines.append(f"BankNifty={idx['banknifty']['price']} ({idx['banknifty']['pchange']:+.2f}%)")
    if idx.get("indiavix"):   lines.append(f"VIX={idx['indiavix']['price']}")
    if gn.get("price"):       lines.append(f"GiftNifty={gn['price']} ({gn.get('changePct',0):+.2f}%)")
    if us.get("dow"):         lines.append(f"Dow={us['dow']['price']} ({us['dow']['pchange']:+.2f}%)")
    if us.get("sp500"):       lines.append(f"S&P500={us['sp500']['price']} ({us['sp500']['pchange']:+.2f}%)")
    if us.get("dxy"):         lines.append(f"DXY={us['dxy']['price']}")
    if met.get("gold_usd"):   lines.append(f"Gold={met['gold_usd']['price']}")
    if met.get("crude_usd"):  lines.append(f"Crude={met['crude_usd']['price']}")
    return " | ".join(lines)


async def generate_timed_summary(summary_type: str, context: str) -> dict:
    prompts = {
        "pre_market": f"""You are a sharp Indian market analyst. Write a crisp pre-market brief for Indian traders.

Data: {context}

3 short paragraphs:
1. Overnight global cues — 2-3 sentences
2. What to watch at open — key levels, sectors, triggers — 2-3 sentences
3. One-line market bias

Specific numbers. Max 120 words.""",

        "hourly": f"""Live Indian market commentator. Write a 60-second hourly update.

Data: {context}

- Market pulse: trend in 1 sentence
- Movers: what's driving it
- Watch: key level next hour

Max 80 words.""",

        "post_market": f"""Indian market analyst. Post-market closing summary.

Data: {context}

3 paragraphs:
1. Day summary — key moves, final closes
2. Drivers — why market moved
3. Tomorrow's setup — what to watch

Max 150 words."""
    }
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            r = await client.post(
                "https://api.anthropic.com/v1/messages",
                headers={"x-api-key": CLAUDE_KEY, "anthropic-version": "2023-06-01", "Content-Type": "application/json"},
                json={
                    "model": "claude-haiku-4-5-20251001",
                    "max_tokens": 350,
                    "messages": [{"role": "user", "content": prompts.get(summary_type, prompts["hourly"])}]
                }
            )
            d    = r.json()
            text = d["content"][0]["text"].strip()
            return {"text": text, "ts": time.time(), "type": summary_type, "context": context[:200]}
    except Exception as e:
        log.warning(f"Timed summary {summary_type}: {e}")
        return {}


FNO_STOCKS = [
    ("HDFCBANK.NS",   "HDFC Bank",      "Banking"),
    ("ICICIBANK.NS",  "ICICI Bank",     "Banking"),
    ("SBIN.NS",       "SBI",            "Banking"),
    ("KOTAKBANK.NS",  "Kotak Bank",     "Banking"),
    ("AXISBANK.NS",   "Axis Bank",      "Banking"),
    ("INDUSINDBK.NS", "IndusInd",       "Banking"),
    ("BANDHANBNK.NS", "Bandhan Bk",     "Banking"),
    ("IDFCFIRSTB.NS", "IDFC First",     "Banking"),
    ("FEDERALBNK.NS", "Federal Bank",   "Banking"),
    ("PNB.NS",        "PNB",            "Banking"),
    ("BANKBARODA.NS", "Bank of Baroda", "Banking"),
    ("CANARABANK.NS", "Canara Bank",    "Banking"),
    ("BAJFINANCE.NS", "Bajaj Fin",      "Finance"),
    ("BAJAJFINSV.NS", "Bajaj Finsv",    "Finance"),
    ("SHRIRAMFIN.NS", "Shriram Fin",    "Finance"),
    ("CHOLAFIN.NS",   "Chola Fin",      "Finance"),
    ("MUTHOOTFIN.NS", "Muthoot Fin",    "Finance"),
    ("M&MFIN.NS",     "M&M Fin",        "Finance"),
    ("SBILIFE.NS",    "SBI Life",       "Insurance"),
    ("HDFCLIFE.NS",   "HDFC Life",      "Insurance"),
    ("ICICIGI.NS",    "ICICI Lombard",  "Insurance"),
    ("LICI.NS",       "LIC",            "Insurance"),
    ("TCS.NS",        "TCS",            "IT"),
    ("INFY.NS",       "Infosys",        "IT"),
    ("HCLTECH.NS",    "HCL Tech",       "IT"),
    ("WIPRO.NS",      "Wipro",          "IT"),
    ("TECHM.NS",      "Tech M",         "IT"),
    ("LTIM.NS",       "LTIMindtree",    "IT"),
    ("MPHASIS.NS",    "Mphasis",        "IT"),
    ("COFORGE.NS",    "Coforge",        "IT"),
    ("PERSISTENT.NS", "Persistent",     "IT"),
    ("OFSS.NS",       "Oracle Fin",     "IT"),
    ("RELIANCE.NS",   "Reliance",       "Energy"),
    ("ONGC.NS",       "ONGC",           "Energy"),
    ("BPCL.NS",       "BPCL",           "Energy"),
    ("IOC.NS",        "IOC",            "Energy"),
    ("HINDPETRO.NS",  "HPCL",           "Energy"),
    ("GAIL.NS",       "GAIL",           "Energy"),
    ("MGL.NS",        "MGL",            "Energy"),
    ("IGL.NS",        "IGL",            "Energy"),
    ("PETRONET.NS",   "Petronet",       "Energy"),
    ("ADANIGREEN.NS", "Adani Green",    "Energy"),
    ("TATAPOWER.NS",  "Tata Power",     "Energy"),
    ("MARUTI.NS",     "Maruti",         "Auto"),
    ("TATAMOTORS.NS", "Tata Motors",    "Auto"),
    ("M&M.NS",        "M&M",            "Auto"),
    ("BAJAJ-AUTO.NS", "Bajaj Auto",     "Auto"),
    ("HEROMOTOCO.NS", "Hero Moto",      "Auto"),
    ("EICHERMOT.NS",  "Eicher Mot",     "Auto"),
    ("ASHOKLEY.NS",   "Ashok Leyland",  "Auto"),
    ("TVSMOTOR.NS",   "TVS Motor",      "Auto"),
    ("MOTHERSON.NS",  "Motherson",      "Auto"),
    ("BHARATFORG.NS", "Bharat Forge",   "Auto"),
    ("APOLLOTYRE.NS", "Apollo Tyre",    "Auto"),
    ("MRF.NS",        "MRF",            "Auto"),
    ("SUNPHARMA.NS",  "Sun Pharma",     "Pharma"),
    ("DRREDDY.NS",    "Dr Reddy",       "Pharma"),
    ("CIPLA.NS",      "Cipla",          "Pharma"),
    ("DIVISLAB.NS",   "Divis Lab",      "Pharma"),
    ("AUROPHARMA.NS", "Aurobindo",      "Pharma"),
    ("LUPIN.NS",      "Lupin",          "Pharma"),
    ("BIOCON.NS",     "Biocon",         "Pharma"),
    ("ALKEM.NS",      "Alkem Lab",      "Pharma"),
    ("TORNTPHARM.NS", "Torrent Pharm",  "Pharma"),
    ("IPCALAB.NS",    "IPCA Lab",       "Pharma"),
    ("TATASTEEL.NS",  "Tata Steel",     "Metals"),
    ("JSWSTEEL.NS",   "JSW Steel",      "Metals"),
    ("HINDALCO.NS",   "Hindalco",       "Metals"),
    ("VEDL.NS",       "Vedanta",        "Metals"),
    ("COALINDIA.NS",  "Coal India",     "Metals"),
    ("NMDC.NS",       "NMDC",           "Metals"),
    ("SAIL.NS",       "SAIL",           "Metals"),
    ("HINDCOPPER.NS", "Hind Copper",    "Metals"),
    ("NATIONALUM.NS", "NALCO",          "Metals"),
    ("HINDUNILVR.NS", "HUL",            "FMCG"),
    ("NESTLEIND.NS",  "Nestle",         "FMCG"),
    ("BRITANNIA.NS",  "Britannia",      "FMCG"),
    ("DABUR.NS",      "Dabur",          "FMCG"),
    ("MARICO.NS",     "Marico",         "FMCG"),
    ("COLPAL.NS",     "Colgate",        "FMCG"),
    ("GODREJCP.NS",   "Godrej CP",      "FMCG"),
    ("ITC.NS",        "ITC",            "FMCG"),
    ("TATACONSUM.NS", "Tata Consumer",  "FMCG"),
    ("VBL.NS",        "Varun Bev",      "FMCG"),
    ("LT.NS",         "L&T",            "Industrials"),
    ("SIEMENS.NS",    "Siemens",        "Industrials"),
    ("ABB.NS",        "ABB",            "Industrials"),
    ("BHEL.NS",       "BHEL",           "Industrials"),
    ("BEL.NS",        "BEL",            "Industrials"),
    ("HAL.NS",        "HAL",            "Industrials"),
    ("ADANIPORTS.NS", "Adani Ports",    "Industrials"),
    ("ADANIENT.NS",   "Adani Ent",      "Industrials"),
    ("GMRINFRA.NS",   "GMR Infra",      "Industrials"),
    ("IRB.NS",        "IRB Infra",      "Industrials"),
    ("CUMMINSIND.NS", "Cummins",        "Industrials"),
    ("THERMAX.NS",    "Thermax",        "Industrials"),
    ("BHARTIARTL.NS", "Airtel",         "Telecom"),
    ("IDEA.NS",       "Vi",             "Telecom"),
    ("INDUSTOWER.NS", "Indus Towers",   "Telecom"),
    ("NTPC.NS",       "NTPC",           "Utilities"),
    ("POWERGRID.NS",  "Power Grid",     "Utilities"),
    ("ADANIPOWER.NS", "Adani Power",    "Utilities"),
    ("TORNTPOWER.NS", "Torrent Power",  "Utilities"),
    ("CESC.NS",       "CESC",           "Utilities"),
    ("DLF.NS",        "DLF",            "Real Estate"),
    ("GODREJPROP.NS", "Godrej Prop",    "Real Estate"),
    ("OBEROIRLTY.NS", "Oberoi Realty",  "Real Estate"),
    ("PRESTIGE.NS",   "Prestige",       "Real Estate"),
    ("PHOENIXLTD.NS", "Phoenix",        "Real Estate"),
    ("BRIGADE.NS",    "Brigade",        "Real Estate"),
    ("ULTRACEMCO.NS", "UltraCem",       "Cement"),
    ("GRASIM.NS",     "Grasim",         "Cement"),
    ("SHREECEM.NS",   "Shree Cem",      "Cement"),
    ("AMBUJACEM.NS",  "Ambuja Cem",     "Cement"),
    ("ACC.NS",        "ACC",            "Cement"),
    ("RAMCOCEM.NS",   "Ramco Cem",      "Cement"),
    ("TITAN.NS",      "Titan",          "Consumer"),
    ("ASIANPAINT.NS", "Asian Paints",   "Consumer"),
    ("PIDILITIND.NS", "Pidilite",       "Consumer"),
    ("PAGEIND.NS",    "Page Ind",       "Consumer"),
    ("TRENT.NS",      "Trent",          "Consumer"),
    ("DMART.NS",      "DMart",          "Consumer"),
    ("NYKAA.NS",      "Nykaa",          "Consumer"),
    ("ZOMATO.NS",     "Zomato",         "Consumer"),
    ("JUBLFOOD.NS",   "Jubilant Food",  "Consumer"),
    ("DEVYANI.NS",    "Devyani",        "Consumer"),
    ("APOLLOHOSP.NS", "Apollo Hosp",    "Healthcare"),
    ("MAXHEALTH.NS",  "Max Health",     "Healthcare"),
    ("FORTIS.NS",     "Fortis",         "Healthcare"),
    ("METROPOLIS.NS", "Metropolis",     "Healthcare"),
    ("LALPATHLAB.NS", "Dr Lal Path",    "Healthcare"),
    ("SRF.NS",        "SRF",            "Chemicals"),
    ("AARTIIND.NS",   "Aarti Ind",      "Chemicals"),
    ("NAVINFLUOR.NS", "Navin Fluor",    "Chemicals"),
    ("DEEPAKNTR.NS",  "Deepak Nitrite", "Chemicals"),
    ("PIIND.NS",      "PI Ind",         "Chemicals"),
    ("UPL.NS",        "UPL",            "Chemicals"),
    ("BSE.NS",        "BSE",            "Capital Mkts"),
    ("MCX.NS",        "MCX",            "Capital Mkts"),
    ("CDSL.NS",       "CDSL",           "Capital Mkts"),
    ("ANGELONE.NS",   "Angel One",      "Capital Mkts"),
    ("ICICIPRULI.NS", "ICICI Pru Life", "Capital Mkts"),
    ("PAYTM.NS",      "Paytm",          "New Age Tech"),
    ("POLICYBZR.NS",  "PB Fintech",     "New Age Tech"),
    ("DELHIVERY.NS",  "Delhivery",      "New Age Tech"),
    ("IRCTC.NS",      "IRCTC",          "New Age Tech"),
]

async def fetch_heatmap_stocks() -> list:
    result = []
    async with httpx.AsyncClient(headers=YAHOO_HEADERS, timeout=20, follow_redirects=True) as client:
        for sym, name, sector in FNO_STOCKS:
            q = await yahoo_quote(client, sym)
            if q and q.get("price") and q.get("pchange") is not None:
                result.append({
                    "symbol":  sym.replace(".NS", ""),
                    "name":    name,
                    "sector":  sector,
                    "price":   q["price"],
                    "change":  round(q["change"], 2),
                    "pchange": round(q["pchange"], 2),
                    "high":    q.get("high"),
                    "low":     q.get("low"),
                    "prev":    q.get("prev"),
                })
            await asyncio.sleep(0.2)
    log.info(f"✅ Heatmap stocks: {len(result)}/{len(FNO_STOCKS)} fetched")
    return result


_yahoo_crumb = {"crumb": None, "cookies": None, "ts": 0}


async def _fetch_nse_via_session(client: httpx.AsyncClient, symbol: str):
    if not SCRAPER_API_KEY:
        return None
    from urllib.parse import quote
    import random, string
    sid = "nse-" + "".join(random.choices(string.ascii_lowercase + string.digits, k=8))
    nse_home = "https://www.nseindia.com/option-chain"
    nse_api  = f"https://www.nseindia.com/api/option-chain-indices?symbol={symbol}"
    try:
        home_url = (f"https://api.scrape.do?token={SCRAPER_API_KEY}"
                    f"&url={quote(nse_home, safe='')}&sessionId={sid}")
        r1 = await client.get(home_url, timeout=30)
        if r1.status_code != 200:
            return None
        await asyncio.sleep(2)
        api_url = (f"https://api.scrape.do?token={SCRAPER_API_KEY}"
                   f"&url={quote(nse_api, safe='')}&sessionId={sid}")
        r2 = await client.get(api_url, timeout=30)
        if r2.status_code == 200:
            data = r2.json()
            if isinstance(data, dict) and "records" in data and "data" in data.get("records", {}):
                return data
    except Exception as e:
        log.warning(f"NSE session {symbol}: {e}")
    return None


def _process_nse_chain(raw):
    if not raw or "records" not in raw or "data" not in raw["records"]:
        return None
    records = raw["records"]
    data = records["data"]
    expiry_dates = sorted(set(r["expiryDate"] for r in data), key=lambda d: datetime.strptime(d, "%d-%b-%Y"))
    if not expiry_dates: return None
    nearest = expiry_dates[0]
    near = [r for r in data if r["expiryDate"] == nearest]
    total_ce, total_pe = 0, 0
    calls, puts, strike_map = [], [], {}
    for row in near:
        s = row["strikePrice"]
        ce_oi = (row.get("CE", {}).get("openInterest", 0) or 0)
        pe_oi = (row.get("PE", {}).get("openInterest", 0) or 0)
        total_ce += ce_oi; total_pe += pe_oi
        if ce_oi: calls.append({"strike": s, "oi": ce_oi})
        if pe_oi: puts.append({"strike": s, "oi": pe_oi})
        strike_map[s] = 0
    for test_s in sorted(strike_map):
        pain = 0
        for row in near:
            s = row["strikePrice"]
            ce_oi = (row.get("CE", {}).get("openInterest", 0) or 0)
            pe_oi = (row.get("PE", {}).get("openInterest", 0) or 0)
            if test_s > s: pain += (test_s - s) * ce_oi
            if test_s < s: pain += (s - test_s) * pe_oi
        strike_map[test_s] = pain
    maxpain = min(strike_map, key=strike_map.get) if strike_map else None
    pcr = round(total_pe / total_ce, 2) if total_ce > 0 else None
    return {"pcr": pcr, "maxpain": maxpain, "expiry": nearest, "spot": records.get("underlyingValue"),
            "top_call_oi": sorted(calls, key=lambda x: x["oi"], reverse=True)[:3],
            "top_put_oi": sorted(puts, key=lambda x: x["oi"], reverse=True)[:3],
            "total_ce": total_ce, "total_pe": total_pe, "source": "nse"}


async def _get_yahoo_crumb(client: httpx.AsyncClient):
    now = time.time()
    if _yahoo_crumb["crumb"] and (now - _yahoo_crumb["ts"]) < 1800:
        return _yahoo_crumb["crumb"], _yahoo_crumb["cookies"]
    cookies = None
    for url in ["https://fc.yahoo.com", "https://query2.finance.yahoo.com"]:
        try:
            r = await client.get(url, timeout=8)
            if r.cookies:
                cookies = dict(r.cookies)
                break
        except: continue
    if not cookies: return None, None
    try:
        r = await client.get("https://query2.finance.yahoo.com/v1/test/getcrumb", cookies=cookies, timeout=8)
        if r.status_code == 200 and r.text.strip() and len(r.text.strip()) < 50:
            crumb = r.text.strip()
            _yahoo_crumb.update({"crumb": crumb, "cookies": cookies, "ts": now})
            return crumb, cookies
    except Exception as e:
        log.warning(f"Yahoo crumb: {e}")
    return None, None


async def _fetch_yahoo_options(client: httpx.AsyncClient, symbol: str):
    crumb, cookies = await _get_yahoo_crumb(client)
    if not crumb: return None
    for attempt in range(2):
        try:
            host = "query2" if attempt == 0 else "query1"
            r = await client.get(f"https://{host}.finance.yahoo.com/v7/finance/options/{symbol}",
                                 params={"crumb": crumb}, cookies=cookies, timeout=15)
            if r.status_code != 200: continue
            d = r.json()
            chain = (d.get("optionChain", {}).get("result", []) or [None])[0]
            if chain and chain.get("options") and chain["options"][0].get("calls"):
                return chain
        except Exception as e:
            if attempt == 1: log.warning(f"Yahoo options {symbol}: {e}")
    return None


def _process_yahoo_chain(chain):
    if not chain or not chain.get("options"): return None
    nearest = chain["options"][0]
    calls_raw, puts_raw = nearest.get("calls", []), nearest.get("puts", [])
    if not calls_raw and not puts_raw: return None
    exp_ts = nearest.get("expirationDate", 0)
    expiry_str = datetime.utcfromtimestamp(exp_ts).strftime("%d-%b-%Y") if exp_ts else "—"
    total_ce, total_pe = 0, 0
    calls, puts, all_rows = [], [], {}
    for c in calls_raw:
        s, oi = c.get("strike", 0), c.get("openInterest", 0) or 0
        total_ce += oi
        if oi > 0: calls.append({"strike": s, "oi": oi})
        all_rows.setdefault(s, {"ce_oi": 0, "pe_oi": 0})["ce_oi"] = oi
    for p in puts_raw:
        s, oi = p.get("strike", 0), p.get("openInterest", 0) or 0
        total_pe += oi
        if oi > 0: puts.append({"strike": s, "oi": oi})
        all_rows.setdefault(s, {"ce_oi": 0, "pe_oi": 0})["pe_oi"] = oi
    if total_ce == 0 and total_pe == 0: return None
    strike_pain = {}
    for test_s in sorted(all_rows):
        pain = sum((test_s - s) * r["ce_oi"] for s, r in all_rows.items() if test_s > s)
        pain += sum((s - test_s) * r["pe_oi"] for s, r in all_rows.items() if test_s < s)
        strike_pain[test_s] = pain
    return {"pcr": round(total_pe / total_ce, 2) if total_ce else None,
            "maxpain": min(strike_pain, key=strike_pain.get) if strike_pain else None,
            "expiry": expiry_str, "spot": chain.get("quote", {}).get("regularMarketPrice"),
            "top_call_oi": sorted(calls, key=lambda x: x["oi"], reverse=True)[:3],
            "top_put_oi": sorted(puts, key=lambda x: x["oi"], reverse=True)[:3],
            "total_ce": total_ce, "total_pe": total_pe, "source": "yahoo"}


async def fetch_option_chain():
    result = {}
    nse_map   = {"nifty": "NIFTY", "bn": "BANKNIFTY"}
    yahoo_map = {"nifty": "^NSEI", "bn": "^NSEBANK"}
    try:
        async with httpx.AsyncClient(headers=YAHOO_HEADERS, follow_redirects=True) as client:
            for prefix in ["nifty", "bn"]:
                processed = None
                raw = await _fetch_nse_via_session(client, nse_map[prefix])
                if raw: processed = _process_nse_chain(raw)
                if not processed:
                    raw = await _fetch_yahoo_options(client, yahoo_map[prefix])
                    if raw: processed = _process_yahoo_chain(raw)
                if processed:
                    result[f"{prefix}_pcr"]     = processed["pcr"]
                    result[f"{prefix}_maxpain"]  = processed["maxpain"]
                    result[f"{prefix}_expiry"]   = processed["expiry"]
                    result[f"{prefix}_spot"]     = processed["spot"]
                    result[f"{prefix}_call_oi"]  = processed["top_call_oi"]
                    result[f"{prefix}_put_oi"]   = processed["top_put_oi"]
                    result[f"{prefix}_total_ce"] = processed["total_ce"]
                    result[f"{prefix}_total_pe"] = processed["total_pe"]
                    result[f"{prefix}_source"]   = processed.get("source", "unknown")
                await asyncio.sleep(1)
        if result: result["updated_at"] = datetime.now().isoformat()
    except Exception as e:
        log.error(f"Option chain error: {e}")
    return result


# ── BACKGROUND REFRESH ────────────────────────────────────────────────────────
async def refresh_cache():
    while True:
        start = time.time()
        log.info("🔄 Refreshing cache...")

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
                log.error(f"❌ {key}: {result}")
            elif result:
                cache[key] = {"data": result, "ts": time.time()}
            else:
                log.warning(f"⚠️ {key}: empty, keeping cache")

        # ── NSE announcements — every 5 minutes ──────────────────────────────
        nse_age = time.time() - cache["nse_news"]["ts"] if cache["nse_news"]["ts"] else 9999
        if nse_age > 300:
            try:
                nse_items = await fetch_nse_announcements()
                if nse_items:
                    cache["nse_news"] = {"data": nse_items, "ts": time.time()}
                    log.info(f"✅ NSE news cache: {len(nse_items)} items")
            except Exception as e:
                log.error(f"❌ NSE news: {e}")

        # ── Sparklines — every 5 minutes ─────────────────────────────────────
        if time.time() - cache["sparklines"]["ts"] > 300:
            sparklines = await fetch_sparklines()
            if sparklines:
                cache["sparklines"] = {"data": sparklines, "ts": time.time()}

        # ── Heatmap — every 5 minutes ─────────────────────────────────────────
        if time.time() - cache["heatmap"]["ts"] > 300:
            heatmap = await fetch_heatmap_stocks()
            if heatmap:
                cache["heatmap"] = {"data": heatmap, "ts": time.time()}

        # ── Option chain — every 3 minutes ────────────────────────────────────
        if time.time() - cache["options"]["ts"] > 180:
            options = await fetch_option_chain()
            if options:
                cache["options"] = {"data": options, "ts": time.time()}

        # ── Timed AI summaries ────────────────────────────────────────────────
        if CLAUDE_KEY:
            ist = timezone(timedelta(hours=5, minutes=30))
            now_ist = datetime.now(ist)
            hour, minute = now_ist.hour, now_ist.minute
            context = get_market_context({
                "indices": cache["indices"]["data"],
                "metals":  cache["metals"]["data"],
                "us":      cache["us"]["data"],
                "giftnifty": cache["giftnifty"]["data"],
            })

            if (7 <= hour < 9 or (hour == 9 and minute < 15)):
                if time.time() - cache["pre_market"]["ts"] > 1800:
                    r = await generate_timed_summary("pre_market", context)
                    if r: cache["pre_market"] = {"data": r, "ts": time.time()}

            elif (hour == 9 and minute >= 15) or (10 <= hour < 15) or (hour == 15 and minute <= 30):
                if time.time() - cache["hourly"]["ts"] > 3600:
                    r = await generate_timed_summary("hourly", context)
                    if r: cache["hourly"] = {"data": r, "ts": time.time()}

            elif (hour == 15 and minute > 30) or (hour == 16):
                if time.time() - cache["post_market"]["ts"] > 1800:
                    r = await generate_timed_summary("post_market", context)
                    if r: cache["post_market"] = {"data": r, "ts": time.time()}

            if time.time() - cache["summary"]["ts"] > 1800:
                summary = await generate_market_summary()
                if summary: cache["summary"] = {"data": summary, "ts": time.time()}

        log.info(f"✅ Refresh done in {round(time.time()-start, 1)}s")
        await asyncio.sleep(60)


@app.on_event("startup")
async def startup():
    asyncio.create_task(refresh_cache())


# ── API ENDPOINTS ─────────────────────────────────────────────────────────────
@app.get("/")
def root():
    ages = {k: round(time.time()-v["ts"]) if v["ts"] else None for k,v in cache.items()}
    return {"status": "Bazaar Watch API v5.7", "time": datetime.now().isoformat(), "cache_ages": ages}


@app.get("/api/all")
def get_all():
    now = time.time()

    # Merge NSE news at top of news feed (deduplicated by title prefix)
    rss_news = cache["news"]["data"]
    nse_news = cache["nse_news"]["data"]
    seen_keys = set(n["title"][:50] for n in nse_news)
    merged_news = nse_news + [n for n in rss_news if n["title"][:50] not in seen_keys]

    return JSONResponse({
        "indices":       cache["indices"]["data"],
        "metals":        cache["metals"]["data"],
        "us":            cache["us"]["data"],
        "news":          merged_news,            # ← NSE items at top, RSS below
        "giftnifty":     cache["giftnifty"]["data"],
        "giftbanknifty": cache["giftbanknifty"]["data"],
        "summary":       cache["summary"]["data"],
        "pre_market":    cache["pre_market"]["data"],
        "hourly":        cache["hourly"]["data"],
        "post_market":   cache["post_market"]["data"],
        "sparklines":    cache["sparklines"]["data"],
        "options":       cache["options"]["data"],
        "timestamp":     datetime.now().isoformat(),
        "cache_age": {
            k: round(now - v["ts"]) if v["ts"] else None
            for k, v in cache.items()
        }
    })


@app.get("/api/indices")
def get_indices():   return JSONResponse(cache["indices"]["data"])

@app.get("/api/metals")
def get_metals():    return JSONResponse(cache["metals"]["data"])

@app.get("/api/us")
def get_us():        return JSONResponse(cache["us"]["data"])

@app.get("/api/news")
def get_news():      return JSONResponse(cache["news"]["data"])

@app.get("/api/giftnifty")
def get_gift_nifty():      return JSONResponse(cache["giftnifty"]["data"])

@app.get("/api/giftbanknifty")
def get_gift_bank_nifty(): return JSONResponse(cache["giftbanknifty"]["data"])

@app.get("/api/summary")
def get_summary():   return JSONResponse(cache["summary"]["data"])

@app.get("/api/heatmap")
def get_heatmap():   return JSONResponse(cache["heatmap"]["data"])

@app.get("/api/pre-market")
def get_pre_market():  return JSONResponse(cache["pre_market"]["data"])

@app.get("/api/hourly")
def get_hourly():      return JSONResponse(cache["hourly"]["data"])

@app.get("/api/post-market")
def get_post_market(): return JSONResponse(cache["post_market"]["data"])

@app.get("/api/sparklines")
def get_sparklines():  return JSONResponse(cache["sparklines"]["data"])

@app.get("/api/options")
def get_options():     return JSONResponse(cache["options"]["data"])


# ── NEW in v5.7 ───────────────────────────────────────────────────────────────
@app.get("/api/nse-news")
def get_nse_news():
    """NSE corporate announcements, board meetings, corporate actions."""
    return JSONResponse({
        "ok":    True,
        "items": cache["nse_news"]["data"],
        "count": len(cache["nse_news"]["data"]),
        "age_s": round(time.time() - cache["nse_news"]["ts"]) if cache["nse_news"]["ts"] else None,
    })


@app.get("/api/debug-nse-news")
async def debug_nse_news():
    """Test NSE scraper live — bypass cache. Use to verify it's working."""
    try:
        items = await fetch_nse_announcements()
        return JSONResponse({
            "ok":    True,
            "count": len(items),
            "items": items[:5],   # first 5 as preview
        })
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)})


@app.get("/api/debug-options")
async def debug_options():
    from urllib.parse import quote
    import random, string
    results = {}
    async with httpx.AsyncClient(headers=YAHOO_HEADERS, follow_redirects=True, timeout=40) as client:
        if SCRAPER_API_KEY:
            sid = "dbg-" + "".join(random.choices(string.ascii_lowercase + string.digits, k=6))
            try:
                r1 = await client.get(
                    f"https://api.scrape.do?token={SCRAPER_API_KEY}"
                    f"&url={quote('https://www.nseindia.com/option-chain', safe='')}&sessionId={sid}",
                    timeout=30
                )
                results["session_step1_status"] = r1.status_code
                await asyncio.sleep(2)
                r2 = await client.get(
                    f"https://api.scrape.do?token={SCRAPER_API_KEY}"
                    f"&url={quote('https://www.nseindia.com/api/option-chain-indices?symbol=NIFTY', safe='')}&sessionId={sid}",
                    timeout=30
                )
                results["session_step2_status"] = r2.status_code
                results["session_step2_has_records"] = '"records"' in r2.text[:2000]
            except Exception as e:
                results["session_error"] = str(e)
        try:
            crumb, cookies = await _get_yahoo_crumb(client)
            results["yahoo_crumb"] = crumb[:10] + "..." if crumb else None
            results["yahoo_cookies"] = bool(cookies)
        except Exception as e:
            results["yahoo_error"] = str(e)
    return JSONResponse(results)


@app.get("/api/health")
def health():
    now = time.time()
    return {
        "status":      "ok",
        "version":     "5.7",
        "finnhub_key": "set" if FINNHUB_KEY    else "missing",
        "claude_key":  "set" if CLAUDE_KEY     else "not configured",
        "scraper_key": "set" if SCRAPER_API_KEY else "missing",
        "gift_proxy":  GIFT_NIFTY_PROXY,
        "cache": {
            k: {
                "age_s":    round(now - v["ts"]) if v["ts"] else None,
                "has_data": bool(v["data"]),
                "count":    len(v["data"]) if isinstance(v["data"], list) else None,
            }
            for k, v in cache.items()
        }
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=False)
