"""
Bazaar Watch — Backend API v6.1
========================================
Changes from v5.6:
  - BSE + Google News + RSS India stocks news
  - ADMIN_KEY protected summary + article endpoints
  - Article CRUD: POST/DELETE /api/admin/article
  - File persistence: articles + summary survive restarts
  - CORS fixed for local file:// admin panel
  - POST/DELETE methods allowed
"""

import os, time, asyncio, logging, threading
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor
import httpx
import feedparser
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, FileResponse

# ── yfinance + nsepython removed (option chain pipeline removed) ──────────────
_HAS_YFINANCE = False
_HAS_NSEPYTHON = False

# Thread pool for running sync library calls inside async functions
_thread_pool = ThreadPoolExecutor(max_workers=4)

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("bazaar")

# ── API KEYS ──────────────────────────────────────────────────────────────────
FINNHUB_KEY      = os.getenv("FINNHUB_KEY",      "d6ubijpr01qp1k9busogd6ubijpr01qp1k9busp0")
SCRAPE_DO_TOKEN  = os.getenv("SCRAPE_DO_TOKEN",  "")   # scrape.do token — for Gift Nifty
CLAUDE_KEY       = os.getenv("CLAUDE_KEY",       "")
SCRAPER_API_KEY  = os.getenv("SCRAPER_API_KEY",  "")
ADMIN_KEY        = os.getenv("ADMIN_KEY",        "")   # Set this in Render environment vars

app = FastAPI(title="Bazaar Watch API v6.1")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*", "null"],   # "null" allows requests from local file:// — needed for admin panel
    allow_methods=["GET", "POST", "DELETE"],
    allow_headers=["*"],
    allow_credentials=False,
)

# ── WWW → non-www 301 redirect ────────────────────────────────────────────────
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import RedirectResponse as _Redirect301

class _WWWRedirect(BaseHTTPMiddleware):
    async def dispatch(self, request, call_next):
        host = request.headers.get("host", "")
        if host.startswith("www.") and "bazaarwatch.in" in host:
            url = str(request.url).replace("://www.", "://", 1)
            return _Redirect301(url, status_code=301)
        return await call_next(request)

app.add_middleware(_WWWRedirect)

# ── CACHE — never wiped, only updated on success ──────────────────────────────
cache = {
    "indices":       {"data": {}, "ts": 0},
    "metals":        {"data": {}, "ts": 0},
    "us":            {"data": {}, "ts": 0},
    "news":          {"data": [], "ts": 0},
    "nse_news":      {"data": [], "ts": 0},   # ← NEW: NSE corporate announcements
    "articles":      {"data": [], "ts": 0},   # ← NEW: Published articles
    "giftnifty":     {"data": {}, "ts": 0},
    "summary":       {"data": {}, "ts": 0},
    "crypto":        {"data": {}, "ts": 0},
    "pre_market":    {"data": {}, "ts": 0},
    "hourly":        {"data": {}, "ts": 0},
    "post_market":   {"data": {}, "ts": 0},
    "heatmap":       {"data": [], "ts": 0},
    "sparklines":    {"data": {}, "ts": 0},
    "futures":       {"data": {}, "ts": 0},   # next-month NSE futures
}

# ── INTRADAY HISTORY — stores price points for canvas charts ──────────────────
from collections import deque

_INTRADAY_MAX = 500   # max points per symbol
_intraday: dict = {}  # key → deque of {"t": epoch_ms, "p": price}

def _intraday_push(key: str, price: float):
    """Append a timestamped price point for intraday chart rendering."""
    if not price or price <= 0:
        return
    if key not in _intraday:
        _intraday[key] = deque(maxlen=_INTRADAY_MAX)
    _intraday[key].append({"t": int(time.time() * 1000), "p": round(price, 4)})

def _intraday_snapshot() -> dict:
    """Return all intraday data as plain dict of lists (JSON-serializable)."""
    return {k: list(v) for k, v in _intraday.items()}

def _intraday_push_from_cache():
    """Push current prices from cache into intraday history."""
    # Indices
    idx = cache.get("indices", {}).get("data", {})
    for key in ("nifty50", "sensex", "banknifty", "indiavix"):
        d = idx.get(key, {})
        if d.get("price"):
            _intraday_push(key, d["price"])

    # Gift Nifty
    gn = _gn_cache.get("giftnifty", {})
    if gn.get("price"):
        _intraday_push("giftnifty", float(gn["price"]))

    # US markets
    us = cache.get("us", {}).get("data", {})
    for key in ("sp500", "nasdaq", "dow", "dxy", "nikkei", "hangseng", "asx200",
                "us10y", "us30y", "us5y", "usdinr", "vix_us", "stoxx50"):
        d = us.get(key, {})
        if d.get("price"):
            _intraday_push(key, d["price"])

    # Metals
    met = cache.get("metals", {}).get("data", {})
    for key in ("gold_usd", "silver_usd", "copper_usd", "aluminium_usd",
                "zinc_usd", "platinum_usd", "crude_usd", "natgas_usd", "nickel_usd"):
        d = met.get(key, {})
        if d.get("price"):
            _intraday_push(key, d["price"])

    # Crypto
    cry = cache.get("crypto", {}).get("data", {})
    for key in ("btc", "eth"):
        d = cry.get(key, {})
        if d.get("price"):
            _intraday_push(key, d["price"])


# ── FILE PERSISTENCE — articles + summary survive restarts ───────────────────
import json as _json, os as _os

DATA_DIR = _os.path.join(_os.path.dirname(__file__), 'data')
_os.makedirs(DATA_DIR, exist_ok=True)

def _persist_path(key: str) -> str:
    return _os.path.join(DATA_DIR, f'{key}.json')

def _load_persisted(key: str):
    """Load data from disk on startup. Returns None if file missing."""
    try:
        path = _persist_path(key)
        if _os.path.exists(path):
            with open(path, 'r', encoding='utf-8') as f:
                data = _json.load(f)
                log.info(f"✅ Loaded persisted {key} from disk")
                return data
    except Exception as e:
        log.warning(f"Could not load persisted {key}: {e}")
    return None

def _save_persisted(key: str, data) -> None:
    """Write data to disk immediately after any save/update."""
    try:
        path = _persist_path(key)
        with open(path, 'w', encoding='utf-8') as f:
            _json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        log.warning(f"Could not persist {key}: {e}")

def _init_persisted_cache():
    """Called at startup — restores articles + summary from disk if available."""
    for key in ('articles', 'summary'):
        data = _load_persisted(key)
        if data is not None:
            cache[key] = {'data': data, 'ts': time.time()}
            log.info(f"✅ Restored {key} from disk: {len(data) if isinstance(data, list) else 'dict'}")

# ── YAHOO FINANCE — core data fetcher ────────────────────────────────────────
YAHOO_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120 Safari/537.36",
    "Accept": "application/json"}

async def yahoo_quote(client: httpx.AsyncClient, symbol: str) -> dict:
    for attempt in range(3):
        try:
            url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?interval=1d&range=1d"
            r   = await client.get(url, timeout=12)
            if r.status_code != 200:
                url = url.replace("query1", "query2")
                r   = await client.get(url, timeout=12)
            ct = r.headers.get("content-type", "")
            if "json" not in ct and "javascript" not in ct:
                raise ValueError("Non-JSON response")
            d    = r.json()
            del r
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
                "volume":  vol}
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
        "^BSESN":   "sensex"}
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
        "NG=F":  "natgas_usd"}
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
                        "prev":    round(float(d.get("pc", 0)), 2)}
            except Exception as e:
                log.warning(f"Nickel Finnhub: {e}")
    return result


async def fetch_us_markets() -> dict:
    result = {}
    symbol_map = {
        "^DJI":       "dow",
        "^GSPC":      "sp500",
        "^IXIC":      "nasdaq",
        "DX-Y.NYB":   "dxy",
        "^VIX":       "vix_us",
        "^TNX":       "us10y",      # US 10Y Treasury Yield
        "^TYX":       "us30y",      # US 30Y Treasury Yield
        "^FVX":       "us5y",       # US 5Y Treasury Yield
        "^N225":      "nikkei",     # Nikkei 225 (Tokyo)
        "^STOXX50E":  "stoxx50",    # EURO STOXX 50
        "^HSI":       "hangseng",   # Hang Seng (Hong Kong)
        "^AXJO":      "asx200",     # ASX 200 (Australia)
        "USDINR=X":   "usdinr",     # USD/INR forex rate
    }
    async with httpx.AsyncClient(headers=YAHOO_HEADERS, timeout=15, follow_redirects=True) as client:
        for sym, key in symbol_map.items():
            q = await yahoo_quote(client, sym)
            if q:
                result[key] = q
                log.info(f"✅ {key} ({sym}): {q['price']}")
            await asyncio.sleep(0.3)
    return result


async def fetch_crypto() -> dict:
    result = {}
    symbol_map = {
        "BTC-USD": "btc",
        "ETH-USD": "eth"}
    async with httpx.AsyncClient(headers=YAHOO_HEADERS, timeout=15, follow_redirects=True) as client:
        for sym, key in symbol_map.items():
            q = await yahoo_quote(client, sym)
            if q:
                result[key] = q
                log.info(f"✅ crypto {key}: {q['price']}")
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
    loop = asyncio.get_running_loop()
    for url, source_name in feeds:
        try:
            feed = await loop.run_in_executor(_thread_pool, feedparser.parse, url)
            for e in feed.entries[:8]:
                title = e.get("title", "").strip()
                if not title:
                    continue
                items.append({
                    "title":  title,
                    "link":   e.get("link", ""),
                    "source": source_name,
                    "time":   e.get("published", "")})
            del feed
        except Exception as e:
            log.warning(f"RSS {source_name}: {e}")
    return items[:50]


# ══════════════════════════════════════════════════════════════════════════════
#  INDIA STOCKS NEWS — v6.1
#  Sources (in priority order):
#    1. BSE India API  — corporate filings, results, dividends, board meetings
#    2. Google News RSS — recent India stock/corporate news aggregated
#    3. Expanded RSS feeds — Livemint, NDTV Profit, BL, Zee Business, FE
# ══════════════════════════════════════════════════════════════════════════════

BSE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0 Safari/537.36"
    ),
    "Referer": "https://www.bseindia.com/",
    "Origin":  "https://www.bseindia.com",
    "Accept":  "application/json, text/plain, */*"}

# BSE announcement category IDs
BSE_CATEGORIES = {
    "30": "RESULT",        # Financial Results
    "6":  "CORP ACTION",   # Dividend
    "13": "CORP ACTION",   # Book Closure
    "4":  "CORP ACTION",   # Board Meeting
    "7":  "CORP ACTION",   # AGM / EGM
    "43": "DEAL",          # Amalgamation / Merger
    "60": "ALERT",         # SEBI / Regulatory
    "48": "IPO",           # Listing / IPO
    "-1": "NSE/BSE",       # All
}


async def fetch_bse_announcements() -> list:
    """
    BSE corporate filings API — the most accessible Indian corporate news source.
    Returns structured announcements: results, dividends, board meetings, mergers.
    """
    from datetime import datetime, timedelta
    today     = datetime.now()
    from_dt   = (today - timedelta(days=7)).strftime("%d%%2F%m%%2F%Y")
    to_dt     = today.strftime("%d%%2F%m%%2F%Y")

    url = (
        f"https://api.bseindia.com/BseIndiaAPI/api/AnnSubCategoryGetData/w"
        f"?pageno=1&strCat=-1&strPrevDate={from_dt}&strScrip="
        f"&strSearch=P&strToDate={to_dt}&strType=C&subcategory=-1"
    )

    try:
        async with httpx.AsyncClient(headers=BSE_HEADERS, timeout=18, follow_redirects=True) as client:
            r = await client.get(url)
            if r.status_code != 200:
                log.warning(f"BSE API HTTP {r.status_code}")
                return []
            data  = r.json()
            table = data.get("Table", [])
            if not table:
                log.warning("BSE API: empty Table")
                return []

            results = []
            seen    = set()
            for item in table[:100]:
                company  = (item.get("SLONGNAME") or item.get("SCRIP_CD") or "").strip()
                headline = (item.get("HEADLINE") or "").strip()
                subcat   = (item.get("SUBCATNAME") or "").strip()
                scrip    = str(item.get("SCRIP_CD") or "")
                newsid   = str(item.get("NEWSID") or "")
                diss_dt  = (item.get("DissemDT") or "").strip()

                if not company or not headline:
                    continue

                # Build readable title
                detail   = subcat if subcat and subcat != headline else ""
                title    = f"{company}: {headline}"
                if detail:
                    title += f" — {detail}"

                # Deduplicate
                key = title[:60].lower()
                if key in seen:
                    continue
                seen.add(key)

                # BSE link to announcement
                attach = item.get("ATTACHMENTNAME", "")
                if attach:
                    link = f"https://www.bseindia.com/xml-data/corpfiling/AttachLive/{attach}"
                else:
                    link = (f"https://www.bseindia.com/corporates/Ann.html"
                            f"?scrip={scrip}&head={headline}&newsid={newsid}")

                # Parse timestamp
                ts_str = ""
                try:
                    dt = datetime.strptime(diss_dt, "%d %b %Y %H:%M:%S")
                    ts_str = dt.strftime("%Y-%m-%dT%H:%M:%S") + "+05:30"
                except Exception:
                    ts_str = diss_dt

                # Classify
                cat_id  = str(item.get("CATEGORYID") or "-1")
                tag     = BSE_CATEGORIES.get(cat_id, _classify_by_text(headline + " " + subcat))

                results.append({
                    "title":    title,
                    "source":   "BSE India",
                    "link":     link,
                    "time":     ts_str,
                    "category": tag,
                    "symbol":   item.get("SCRIP_CD", "")})

            log.info(f"✅ BSE API: {len(results)} announcements")
            return results

    except Exception as e:
        log.warning(f"BSE API: {e}")
        return []


async def fetch_google_news_india() -> list:
    """
    Google News RSS — highly reliable, aggregates from all Indian financial sources.
    Returns recent India stock/corporate news.
    """
    queries = [
        "India+stock+results+quarterly+earnings+NSE+BSE",
        "India+dividend+bonus+split+buyback+stock+announcement",
        "India+corporate+merger+acquisition+deal+stock",
        "SEBI+India+stock+market+corporate+filing",
    ]
    base = "https://news.google.com/rss/search?hl=en-IN&gl=IN&ceid=IN:en&q="
    items = []
    seen  = set()

    try:
        async with httpx.AsyncClient(timeout=15, follow_redirects=True) as client:
            for q in queries:
                try:
                    r    = await client.get(base + q, timeout=10)
                    feed = await loop.run_in_executor(_thread_pool, feedparser.parse, r.text)
                    for e in feed.entries[:12]:
                        title = (e.get("title") or "").strip()
                        if not title or title[:50] in seen:
                            continue
                        seen.add(title[:50])
                        # Google News wraps source in title as "Title - Source"
                        src = "Google News"
                        if " - " in title:
                            parts = title.rsplit(" - ", 1)
                            title = parts[0].strip()
                            src   = parts[1].strip()
                        items.append({
                            "title":    title,
                            "source":   src,
                            "link":     e.get("link", ""),
                            "time":     e.get("published", ""),
                            "category": _classify_by_text(title),
                            "symbol":   ""})
                    await asyncio.sleep(0.3)
                except Exception as e:
                    log.warning(f"Google News query '{q[:30]}': {e}")

    except Exception as e:
        log.warning(f"Google News: {e}")

    log.info(f"✅ Google News India: {len(items)} items")
    return items


def _classify_by_text(text: str) -> str:
    """Classify a news item by keywords in its text."""
    t = (text or "").lower()
    if any(k in t for k in ("dividend", "bonus share", "stock split", "buyback", "rights issue", "ex-date")):
        return "CORP ACTION"
    if any(k in t for k in ("result", "profit", "revenue", "ebitda", "pat", "quarterly", "earnings",
                             "q1", "q2", "q3", "q4", "annual result", "net loss")):
        return "RESULT"
    if any(k in t for k in ("ipo", "listing", "allotment", "offer for sale", "ofs", "sme ipo")):
        return "IPO"
    if any(k in t for k in ("sebi", "penalty", "order", "circuit", "ban", "suspension",
                             "investigation", "show cause", "fraud")):
        return "ALERT"
    if any(k in t for k in ("merger", "acquisition", "acquires", "stake sale", "deal",
                             "mou", "agreement", "joint venture", "takeover", "block deal", "bulk deal")):
        return "DEAL"
    if any(k in t for k in ("fii", "fpi", "dii", "institutional", "foreign investor")):
        return "FII/DII"
    return "NSE/BSE"


async def fetch_india_stock_rss() -> list:
    """
    Extended Indian financial RSS feeds — specific to stocks/corporate news.
    Supplements BSE + Google News with source-specific feeds.
    """
    feeds = [
        # MoneyControl — specific feeds for stocks
        ("https://www.moneycontrol.com/rss/results.xml",          "Moneycontrol"),
        ("https://www.moneycontrol.com/rss/corporateactions.xml", "Moneycontrol"),
        ("https://www.moneycontrol.com/rss/latestnews.xml",       "Moneycontrol"),
        ("https://www.moneycontrol.com/rss/marketreports.xml",    "Moneycontrol"),
        # Livemint
        ("https://www.livemint.com/rss/companies",                "Livemint"),
        ("https://www.livemint.com/rss/markets",                  "Livemint"),
        ("https://www.livemint.com/rss/money",                    "Livemint"),
        # The Hindu BusinessLine
        ("https://www.thehindubusinessline.com/markets/?service=rss",   "BusinessLine"),
        ("https://www.thehindubusinessline.com/companies/?service=rss", "BusinessLine"),
        # NDTV Profit
        ("https://www.ndtv.com/business/rss",                     "NDTV Profit"),
        # Financial Express
        ("https://www.financialexpress.com/market/rss",           "Financial Express"),
        ("https://www.financialexpress.com/industry/rss",         "Financial Express"),
        # Zee Business
        ("https://www.zeebiz.com/rss",                            "Zee Business"),
        # Economic Times — more specific
        ("https://economictimes.indiatimes.com/markets/stocks/news/rssfeeds/2143736.cms",    "ET Stocks"),
        ("https://economictimes.indiatimes.com/markets/stocks/earnings/rssfeeds/2143759.cms","ET Earnings"),
        ("https://economictimes.indiatimes.com/markets/ipo/rss.cms",                         "ET IPO"),
        # Business Today
        ("https://www.businesstoday.in/rssfeeds/1260657.cms",     "Business Today"),
        # Outlook Money
        ("https://www.outlookmoney.com/feed",                     "Outlook Money"),
        # The Ken (free articles)
        ("https://the-ken.com/feed/",                             "The Ken"),
        # Entrackr (startup/IPO)
        ("https://entrackr.com/feed/",                            "Entrackr"),
        # Inc42 Markets
        ("https://inc42.com/feed/",                               "Inc42"),
        # India Infoline
        ("https://www.indiainfoline.com/rss/news.xml",            "IIFL"),
        # 5Paisa
        ("https://www.5paisa.com/blog/feed/",                     "5Paisa"),
        # StockEdge blog
        ("https://blog.stockedge.com/feed/",                      "StockEdge"),
        # Trade Brains
        ("https://tradebrains.in/feed/",                          "Trade Brains"),
        # Finshots (great for retail readers)
        ("https://finshots.in/archive/rss/",                      "Finshots"),
    ]
    items = []
    seen  = set()

    loop = asyncio.get_running_loop()
    for url, source in feeds:
        try:
            feed = await loop.run_in_executor(_thread_pool, feedparser.parse, url)
            for e in feed.entries[:10]:
                title = (e.get("title") or "").strip()
                if not title or title[:50] in seen:
                    continue
                seen.add(title[:50])
                items.append({
                    "title":    title,
                    "source":   source,
                    "link":     e.get("link", ""),
                    "time":     e.get("published", ""),
                    "category": _classify_by_text(title),
                    "symbol":   ""})
            del feed
        except Exception as e:
            log.warning(f"India RSS {source}: {e}")

    log.info(f"✅ India stock RSS: {len(items)} items from {len(feeds)} feeds")
    return items


async def fetch_india_stocks_news() -> list:
    """
    Master function — runs all 3 sources concurrently, merges and deduplicates.
    BSE API items go first (most authoritative), then Google News, then RSS.
    """
    results = await asyncio.gather(
        fetch_bse_announcements(),
        fetch_google_news_india(),
        fetch_india_stock_rss(),
        return_exceptions=True
    )

    bse_items    = results[0] if not isinstance(results[0], Exception) else []
    google_items = results[1] if not isinstance(results[1], Exception) else []
    rss_items    = results[2] if not isinstance(results[2], Exception) else []

    # Merge — BSE first, deduplicate by title prefix
    seen, merged = set(), []
    for item in (bse_items + google_items + rss_items):
        key = (item.get("title") or "")[:55].lower().strip()
        if key and key not in seen:
            seen.add(key)
            merged.append(item)

    log.info(
        f"✅ India stocks news total: {len(merged)} "
        f"(BSE={len(bse_items)}, Google={len(google_items)}, RSS={len(rss_items)})"
    )
    return merged


# ══════════════════════════════════════════════════════════════════════════════
#  REST OF ORIGINAL FUNCTIONS (unchanged)
# ══════════════════════════════════════════════════════════════════════════════
#  REST OF ORIGINAL FUNCTIONS (unchanged)
# ══════════════════════════════════════════════════════════════════════════════

# ── Gift Nifty session logic (IST) ───────────────────────────────────────────
def get_gift_session() -> dict:
    """Return whether Gift Nifty is currently in an active trading session."""
    from datetime import timezone, timedelta
    IST = timezone(timedelta(hours=5, minutes=30))
    now = datetime.now(IST)
    day = now.weekday()   # 0=Mon … 6=Sun
    mins = now.hour * 60 + now.minute

    # Weekend
    if day >= 5:
        return {"active": False, "resumeAt": "Monday 6:30 AM"}

    S1_START, S1_END   = 6*60+30,  9*60+15   # 6:30 AM – 9:15 AM
    S2_START            = 16*60+35             # 4:35 PM
    # Session 2 overnight: midnight → 2:45 AM = 165 mins

    if S1_START <= mins < S1_END:
        return {"active": True, "session": 1, "label": "Session 1 · 6:30–9:15 AM"}
    if mins >= S2_START or mins < 165:
        return {"active": True, "session": 2, "label": "Session 2 · 4:35 PM–2:45 AM"}
    if 165 <= mins < S1_START:
        return {"active": False, "resumeAt": "6:30 AM"}
    return {"active": False, "resumeAt": "4:35 PM"}

# ── Gift Nifty internal cache ─────────────────────────────────────────────────
_gn_cache:    dict = {"giftnifty": {}}
_gn_cache_ts: dict = {"giftnifty": 0.0}
_gn_fetching: dict = {"giftnifty": False}
GN_CACHE_TTL = 45  # seconds

# Moneycontrol appfeeds API — free, no auth, CORS open (Access-Control-Allow-Origin: *)
# Symbol discovered from network tab: in;gsx = Gift Nifty, in;gbnf = Gift Bank Nifty
GN_SOURCES = {
    "giftnifty":     "https://appfeeds.moneycontrol.com/jsonapi/market/indices&format=json&ind_id=in;gsx&source=globalindices",
    }

MC_HEADERS = {
    "User-Agent":  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120 Safari/537.36",
    "Referer":     "https://www.moneycontrol.com/",
    "Accept":      "application/json, */*"}

async def _fetch_moneycontrol(key: str) -> dict:
    """Fetch Gift Nifty price from Moneycontrol appfeeds — free, no scrape.do needed."""
    url = GN_SOURCES[key]
    name = "Gift Nifty 50" if key == "giftnifty" else "Gift Bank Nifty"
    async with httpx.AsyncClient(timeout=10, headers=MC_HEADERS) as client:
        r = await client.get(url)
        if r.status_code != 200:
            raise ValueError(f"Moneycontrol returned HTTP {r.status_code}")
        data = r.json()

    # Actual response: {"indices": {"lastprice":"22,958.00","change":"-43.00","percentchange":"-0.19","open":"23,021.50","high":"23,027.00","low":"22,785.00","prevclose":"23,001.00",...}}
    d = data.get("indices")
    if not d or not isinstance(d, dict):
        raise ValueError(f"Unexpected Moneycontrol response structure for {name}")

    def safe_float(v):
        try: return float(str(v).replace(",", ""))
        except: return None

    price = safe_float(d.get("lastprice"))
    prev  = safe_float(d.get("prevclose"))
    high  = safe_float(d.get("high"))
    low   = safe_float(d.get("low"))
    open_ = safe_float(d.get("open"))
    change     = safe_float(d.get("change")) or (round(price - prev, 2) if price and prev else 0.0)
    change_pct = safe_float(d.get("percentchange")) or (round((change / prev) * 100, 2) if prev else 0.0)

    if not price:
        raise ValueError(f"Cannot parse price from Moneycontrol for {name}. Keys: {list(d.keys())}")

    if not prev:
        prev = price

    log.info(f"[MC] {name}: {price} ({'+' if change_pct>=0 else ''}{change_pct}%) prev={prev}")
    return {
        "ok": True, "price": price, "prevClose": prev,
        "open": open_, "high": high, "low": low,
        "change": change, "changePct": change_pct,
        "source": "moneycontrol/NSEIX"}

def _parse_investing_html(html: str, name: str) -> dict:
    """Extract price fields from Investing.com rendered HTML."""
    import re
    def num(s):
        return float(s.replace(",", "")) if s else None
    def find(*patterns):
        for p in patterns:
            m = re.search(p, html)
            if m and m.group(1):
                return m.group(1)
        return None

    # ── Price ─────────────────────────────────────────────────────────────────
    raw = find(
        r'data-last-price="([\d.,]+)"',
        r'"lastPrice"\s*:\s*"?([\d.,]+)"?',
        r'class="text-5xl[^"]*"[^>]*>([\d,]+\.?\d*)',
        r'"last"\s*:\s*([\d.]+)',
    )
    if not raw:
        raise ValueError(f"Cannot parse {name} price from Investing.com")
    price = num(raw)

    # ── Percentage change — most reliably present on page as visible text ─────
    # Try data attributes first, then visible text patterns like "-0.28%" or "(+0.33%)"
    raw_pct = find(
        r'data-pcp="(-?[\d.]+)"',
        r'"pcp"\s*:\s*(-?[\d.]+)',
        r'data-last-percent="(-?[\d.]+)"',
        r'"percentChange"\s*:\s*(-?[\d.]+)',
        r'(?:class="[^"]*(?:percent|change)[^"]*"[^>]*>)\s*([+-]?[\d.]+)%',
        # Visible text: looks for (±N.NN%) pattern on page
        r'\(([+-]?[\d.]+)%\)',
    )

    # ── Prev close — try direct attributes, then derive from pct ─────────────
    prev = num(find(
        r'data-prev-close-price="([\d.,]+)"',
        r'"prevClose"\s*:\s*"?([\d.,]+)"?',
        r'data-pc="([\d.,]+)"',
        r'"pc"\s*:\s*([\d.]+)',
    ))

    if not prev and raw_pct:
        pct = float(raw_pct)
        # price = prev × (1 + pct/100)  →  prev = price / (1 + pct/100)
        if pct != -100:
            prev = round(price / (1 + pct / 100), 2)

    if not prev:
        prev = price  # last resort — change will show 0

    # ── High / Low / Open ────────────────────────────────────────────────────
    high  = num(find(r'data-high-price="([\d.,]+)"',  r'"high"\s*:\s*([\d.]+)'))  or price
    low   = num(find(r'data-low-price="([\d.,]+)"',   r'"low"\s*:\s*([\d.]+)'))   or price
    open_ = num(find(r'data-open-price="([\d.,]+)"',  r'"open"\s*:\s*([\d.]+)'))  or price

    # ── If pct was found directly, use it — more accurate than derived ────────
    if raw_pct:
        change_pct = float(raw_pct)
        change     = round(price - prev, 2)
    else:
        change     = round(price - prev, 2)
        change_pct = round((change / prev) * 100, 2) if prev else 0.0

    log.info(f"[GN PARSE] {name}: price={price} prev={prev} change={change} pct={change_pct} raw_pct={raw_pct}")
    return {
        "ok": True, "price": price, "prevClose": prev,
        "open": open_, "high": high, "low": low,
        "change": change, "changePct": change_pct,
        "source": "investing.com/NSEIX"}

async def _get_gift_cached(key: str) -> dict:
    """Return cached Gift Nifty data, fetching fresh if cache is stale.
    No session gate — Moneycontrol is free so we fetch 21hrs/day."""
    import time
    now = time.time()
    hit = _gn_cache.get(key)
    age = now - _gn_cache_ts.get(key, 0.0)

    # Cache still fresh — return it
    if hit and age < GN_CACHE_TTL:
        return {**hit, "cached": True, "age_s": int(age)}

    # Fetch lock — prevent simultaneous duplicate fetches
    if _gn_fetching.get(key):
        log.info(f"[GN SKIP] {key} fetch in progress, serving stale")
        if hit:
            return {**hit, "cached": True, "stale": True, "age_s": int(age)}
        return {"ok": False, "error": "fetching"}

    # Fetch fresh from Moneycontrol
    _gn_fetching[key] = True
    try:
        name = "Gift Nifty 50" if key == "giftnifty" else "Gift Bank Nifty"
        log.info(f"[GN FETCH] {name} (cache age={int(age)}s)")
        data = await _fetch_moneycontrol(key)
        _gn_cache[key]    = data
        _gn_cache_ts[key] = time.time()
        return data
    except Exception as e:
        log.warning(f"[GN ERROR] {key}: {e}")
        if hit:
            return {**hit, "stale": True, "error": str(e)}
        raise
    finally:
        _gn_fetching[key] = False

async def fetch_gift_nifty() -> dict:
    try:
        return await _get_gift_cached("giftnifty")
    except Exception as e:
        log.warning(f"Gift Nifty fetch: {e}")
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
                    "temperature": 0.3}
            )
            result  = r.json()
            summary = result["content"][0]["text"]
            log.info("✅ AI summary generated")
            return {"summary": summary, "generated_at": datetime.now(timezone.utc).isoformat(), "session": session}
    except Exception as e:
        log.warning(f"AI summary: {e}")
        return {}


def _next_nse_expiry(months_ahead: int = 1) -> str:
    """Return the last Thursday of N months ahead — NSE F&O expiry date."""
    import calendar
    from datetime import date, timedelta
    today = date.today()
    m, y = today.month + months_ahead, today.year
    while m > 12:
        m -= 12; y += 1
    last = calendar.monthrange(y, m)[1]
    d = date(y, m, last)
    while d.weekday() != 3:   # 3 = Thursday
        d -= timedelta(days=1)
    return d.strftime("%y%b").upper()   # e.g. "26MAY"


async def fetch_nse_futures() -> dict:
    """
    Fetch next-month futures for Nifty, BankNifty, Sensex from Yahoo Finance.
    Yahoo Finance carries NSE futures as: NIFTY{YYMON}FUT.NS
    Falls back to Gift Nifty / Gift Bank Nifty (already cached) if unavailable.
    """
    expiry = _next_nse_expiry(1)
    result = {}

    symbols = {
        "nifty50":   f"NIFTY{expiry}FUT.NS",
        "banknifty": f"BANKNIFTY{expiry}FUT.NS",
        "sensex":    f"SENSEX{expiry}FUT.BO"}
    log.info(f"[FUT] Fetching next-month futures — expiry tag: {expiry}")

    async with httpx.AsyncClient(headers=YAHOO_HEADERS, timeout=12, follow_redirects=True) as client:
        for key, sym in symbols.items():
            try:
                q = await yahoo_quote(client, sym)
                if q and q.get("price"):
                    result[key] = q
                    log.info(f"[FUT ✅] {key}: {q['price']} via {sym}")
                await asyncio.sleep(0.3)
            except Exception as e:
                log.warning(f"[FUT ❌] {key} ({sym}): {e}")

    # ── Fallback: use Gift Nifty / Gift Bank Nifty if Yahoo futures unavailable ─
    if not result.get("nifty50"):
        gn = _gn_cache.get("giftnifty", {})
        if gn.get("price"):
            result["nifty50"] = {
                "price": gn["price"], "change": gn.get("change", 0),
                "pchange": gn.get("changePct", 0), "high": gn.get("high"),
                "low": gn.get("low"), "source": "gift-nifty-proxy"}
            log.info(f"[FUT] Nifty fallback → Gift Nifty: {gn['price']}")

    # Gift Bank Nifty fallback removed

    if not result.get("sensex"):
        # Sensex: no Gift equivalent — use spot with note
        idx = cache["indices"]["data"]
        if idx.get("sensex"):
            result["sensex"] = {**idx["sensex"], "source": "spot-proxy"}

    result["expiry_tag"] = expiry
    return result


SPARKLINE_SYMBOLS = {
    "nifty50":   "^NSEI",
    "sensex":    "^BSESN",
    "banknifty": "^NSEBANK",
    "indiavix":  "^INDIAVIX",
    "dow":       "^DJI",
    "sp500":     "^GSPC",
    "gold":      "GC=F",
    "crude":     "CL=F",
    "usdinr":    "USDINR=X"}

# ── ALL YAHOO SYMBOLS → intraday keys (used for history bootstrap) ────────────
ALL_INTRADAY_SYMBOLS: dict = {
    "^NSEI":      "nifty50",
    "^NSEBANK":   "banknifty",
    "^INDIAVIX":  "indiavix",
    "^BSESN":     "sensex",
    "^DJI":       "dow",
    "^GSPC":      "sp500",
    "^IXIC":      "nasdaq",
    "DX-Y.NYB":   "dxy",
    "^VIX":       "vix_us",
    "^TNX":       "us10y",
    "^TYX":       "us30y",
    "^FVX":       "us5y",
    "^N225":      "nikkei",
    "^STOXX50E":  "stoxx50",
    "^HSI":       "hangseng",
    "^AXJO":      "asx200",
    "USDINR=X":   "usdinr",
    "GC=F":       "gold_usd",
    "SI=F":       "silver_usd",
    "HG=F":       "copper_usd",
    "ALI=F":      "aluminium_usd",
    "ZNC=F":      "zinc_usd",
    "PL=F":       "platinum_usd",
    "CL=F":       "crude_usd",
    "NG=F":       "natgas_usd",
    "BTC-USD":    "btc",
    "ETH-USD":    "eth",
}

async def fetch_intraday_history():
    """
    Bootstrap intraday chart history from Yahoo Finance (5-min candles, last 5 days).
    Called once at startup so charts are never empty, even after a server restart.
    Subsequent calls (every 10 min) keep the data fresh.
    """
    log.info("📈 Fetching intraday history for all symbols…")
    fetched = 0
    async with httpx.AsyncClient(headers=YAHOO_HEADERS, timeout=20, follow_redirects=True) as client:
        for sym, key in ALL_INTRADAY_SYMBOLS.items():
            try:
                url = (f"https://query1.finance.yahoo.com/v8/finance/chart/{sym}"
                       f"?interval=5m&range=5d")
                r = await client.get(url, timeout=15)
                d = r.json()
                res = d.get("chart", {}).get("result", [])
                if not res:
                    continue
                timestamps = res[0].get("timestamp", [])
                closes = (res[0].get("indicators", {})
                                .get("quote", [{}])[0]
                                .get("close", []))
                pts = [
                    {"t": int(ts * 1000), "p": round(float(c), 4)}
                    for ts, c in zip(timestamps, closes)
                    if c is not None and float(c) > 0
                ]
                if pts:
                    # Merge with existing deque so live-accumulated points aren't lost
                    existing = list(_intraday.get(key, deque()))
                    # Only use history points older than what we already have
                    cutoff = existing[0]["t"] if existing else 0
                    new_pts = [p for p in pts if p["t"] < cutoff] if cutoff else pts
                    merged = new_pts + existing
                    # Respect max size
                    merged = merged[-_INTRADAY_MAX:]
                    _intraday[key] = deque(merged, maxlen=_INTRADAY_MAX)
                    fetched += 1
                await asyncio.sleep(0.25)
            except Exception as e:
                log.warning(f"Intraday history {key}: {e}")
    log.info(f"✅ Intraday history loaded for {fetched}/{len(ALL_INTRADAY_SYMBOLS)} symbols")


# Weekly sparklines — 1-month daily data for metals
SPARKLINE_WEEKLY = {
    "copper":    "HG=F",
    "aluminium": "ALI=F",
    "zinc":      "ZNC=F",
    "nickel":    "^LMENI",    # LME Nickel — fallback to CAPITALCOM if unavailable
}

async def fetch_sparklines() -> dict:
    result = {}
    async with httpx.AsyncClient(headers=YAHOO_HEADERS, timeout=20, follow_redirects=True) as client:
        # Intraday — 1h intervals, 1 day range
        for key, sym in SPARKLINE_SYMBOLS.items():
            try:
                url = f"https://query1.finance.yahoo.com/v8/finance/chart/{sym}?interval=1h&range=1d"
                r   = await client.get(url, timeout=10)
                d   = r.json()
                res = d.get("chart", {}).get("result", [])
                if not res: continue
                closes = res[0].get("indicators", {}).get("quote", [{}])[0].get("close", [])
                pts = [round(v, 2) for v in closes if v is not None]
                if len(pts) >= 2: result[key] = pts
                await asyncio.sleep(0.2)
            except Exception as e:
                log.warning(f"Sparkline {key}: {e}")
        # Weekly — 1d intervals, 1 month range
        for key, sym in SPARKLINE_WEEKLY.items():
            try:
                url = f"https://query1.finance.yahoo.com/v8/finance/chart/{sym}?interval=1d&range=1mo"
                r   = await client.get(url, timeout=10)
                d   = r.json()
                res = d.get("chart", {}).get("result", [])
                if not res: continue
                closes = res[0].get("indicators", {}).get("quote", [{}])[0].get("close", [])
                pts = [round(v, 4) for v in closes if v is not None]
                if len(pts) >= 2: result[f"w_{key}"] = pts   # prefix w_ = weekly
                await asyncio.sleep(0.2)
            except Exception as e:
                log.warning(f"Weekly sparkline {key}: {e}")
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
                    "prev":    q.get("prev")})
            await asyncio.sleep(0.2)
    log.info(f"✅ Heatmap stocks: {len(result)}/{len(FNO_STOCKS)} fetched")
    return result


# ── BACKGROUND REFRESH ────────────────────────────────────────────────────────
_refresh_lock = asyncio.Lock()

async def refresh_cache():
    while True:
        if _refresh_lock.locked():
            log.warning("⏳ Previous refresh still running — skipping cycle")
            await asyncio.sleep(60)
            continue
        async with _refresh_lock:
            start = time.time()
            log.info("🔄 Refreshing cache...")

            results = await asyncio.gather(
            fetch_nse_indices(),
            fetch_metals(),
            fetch_us_markets(),
            fetch_crypto(),
            fetch_news(),
            return_exceptions=True
            )

            keys = ["indices", "metals", "us", "crypto", "news"]
            for key, result in zip(keys, results):
                if isinstance(result, Exception):
                    log.error(f"❌ {key}: {result}")
                elif result:
                    cache[key] = {"data": result, "ts": time.time()}
                else:
                    log.warning(f"⚠️ {key}: empty, keeping cache")

            # ── India stocks news — every 5 minutes ─────────────────────────────
            nse_age = time.time() - cache["nse_news"]["ts"] if cache["nse_news"]["ts"] else 9999
            if nse_age > 300:
                try:
                    india_items = await fetch_india_stocks_news()
                    if india_items:
                        cache["nse_news"] = {"data": india_items, "ts": time.time()}
                        log.info(f"✅ India stocks news cache: {len(india_items)} items")
                except Exception as e:
                    log.error(f"❌ India stocks news: {e}")

            # ── Next-month futures — every 15 minutes ────────────────────────────
            if time.time() - cache["futures"]["ts"] > 900:
                try:
                    fut = await fetch_nse_futures()
                    if fut:
                        cache["futures"] = {"data": fut, "ts": time.time()}
                        log.info(f"[FUT] Cache updated: {list(fut.keys())}")
                except Exception as e:
                    log.error(f"❌ futures: {e}")

            # ── Sparklines — every 5 minutes ─────────────────────────────────────
            if time.time() - cache["sparklines"]["ts"] > 300:
                sparklines = await fetch_sparklines()
                if sparklines:
                    cache["sparklines"] = {"data": sparklines, "ts": time.time()}

            # ── Heatmap — every 5 minutes ─────────────────────────────────────────
            if time.time() - cache["heatmap"]["ts"] > 600:
                heatmap = await fetch_heatmap_stocks()
                if heatmap:
                    cache["heatmap"] = {"data": heatmap, "ts": time.time()}
                gc.collect()

            # ── Timed AI summaries ────────────────────────────────────────────────
            if CLAUDE_KEY:
                ist = timezone(timedelta(hours=5, minutes=30))
                now_ist = datetime.now(ist)
                hour, minute = now_ist.hour, now_ist.minute
                context = get_market_context({
                    "indices": cache["indices"]["data"],
                    "metals":  cache["metals"]["data"],
                    "us":      cache["us"]["data"],
                    "giftnifty": cache["giftnifty"]["data"]})

                if (7 <= hour < 9 or (hour == 9 and minute < 15)):
                    if time.time() - cache["pre_market"]["ts"] > 600:
                        r = await generate_timed_summary("pre_market", context)
                        if r: cache["pre_market"] = {"data": r, "ts": time.time()}

                elif (hour == 9 and minute >= 15) or (10 <= hour < 15) or (hour == 15 and minute <= 30):
                    if time.time() - cache["hourly"]["ts"] > 3600:
                        r = await generate_timed_summary("hourly", context)
                        if r: cache["hourly"] = {"data": r, "ts": time.time()}

                elif (hour == 15 and minute > 30) or (hour == 16):
                    if time.time() - cache["post_market"]["ts"] > 600:
                        r = await generate_timed_summary("post_market", context)
                        if r: cache["post_market"] = {"data": r, "ts": time.time()}

                if time.time() - cache["summary"]["ts"] > 600:
                    summary = await generate_market_summary()
                    if summary: cache["summary"] = {"data": summary, "ts": time.time()}

            _intraday_push_from_cache()
            log.info(f"✅ Refresh done in {round(time.time()-start, 1)}s")
            gc.collect()
        await asyncio.sleep(60)


@app.on_event("startup")
async def startup():
    _init_persisted_cache()   # restore articles + summary from disk
    asyncio.create_task(refresh_cache())
    asyncio.create_task(_intraday_history_loop())

async def _intraday_history_loop():
    """Fetch full intraday history at startup, then refresh every 10 minutes."""
    await fetch_intraday_history()   # immediate bootstrap on restart
    while True:
        await asyncio.sleep(600)     # re-fetch every 10 minutes
        await fetch_intraday_history()



# ═══════════════════════════════════════════════════════════════════════════════
# SEO SECTION PAGES — each section has its own URL for Google indexing
# ═══════════════════════════════════════════════════════════════════════════════
def _static(filename: str):
    """Serve an HTML file from the static/ folder."""
    import os
    p = os.path.join(os.path.dirname(__file__), "static", filename)
    if os.path.exists(p):
        return FileResponse(p, media_type="text/html")
    # Fallback to homepage if static file missing
    html_path = os.path.join(os.path.dirname(__file__), "index.html")
    return FileResponse(html_path, media_type="text/html")

@app.get("/gift-nifty")
def page_gift_nifty():
    return _static("gift-nifty.html")

@app.get("/pre-market-cues")
def page_pre_market_cues():
    return _static("pre-market-cues.html")

@app.get("/nifty-50-live")
def page_nifty_50_live():
    return _static("nifty-50-live.html")

@app.get("/gold-price")
def page_gold_price():
    return _static("gold-price.html")

@app.get("/crude-oil-price")
def page_crude_oil_price():
    return _static("crude-oil-price.html")

@app.get("/base-metals")
def page_base_metals():
    return _static("base-metals.html")

@app.get("/india-vix")
def page_india_vix():
    return _static("india-vix.html")

@app.get("/fno-heatmap")
def page_fno_heatmap():
    return _static("fno-heatmap.html")

@app.get("/economic-calendar")
def page_economic_calendar():
    return _static("economic-calendar.html")

@app.get("/us-bond-yields")
def page_us_bond_yields():
    return _static("us-bond-yields.html")

@app.get("/global-markets")
def page_global_markets():
    return _static("global-markets.html")

@app.get("/crypto")
def page_crypto():
    return _static("crypto.html")

@app.get("/market-news")
def page_market_news():
    return _static("market-news.html")

@app.get("/stock-analysis")
def page_analysis():
    return _static("analysis.html")


# ── API ENDPOINTS ─────────────────────────────────────────────────────────────


@app.get("/api/intraday")
def get_intraday():
    """Return all intraday price history for canvas charts."""
    return JSONResponse(_intraday_snapshot())

@app.get("/")
def root():
    """Serve the Bazaar Watch frontend."""
    import os
    html_path = os.path.join(os.path.dirname(__file__), "index.html")
    if os.path.exists(html_path):
        return FileResponse(html_path, media_type="text/html")
    return {"status": "Bazaar Watch API v6.1", "note": "index.html not found in repo"}

@app.get("/sitemap.xml")
def sitemap():
    import os
    p = os.path.join(os.path.dirname(__file__), "sitemap.xml")
    return FileResponse(p, media_type="application/xml") if os.path.exists(p) else JSONResponse({})

@app.get("/robots.txt")
def robots():
    import os
    p = os.path.join(os.path.dirname(__file__), "robots.txt")
    return FileResponse(p, media_type="text/plain") if os.path.exists(p) else JSONResponse({})

@app.get("/health")
def health():
    """API health check — replaces old root JSON."""
    ages = {k: round(time.time()-v["ts"]) if v["ts"] else None for k,v in cache.items()}
    return {"status": "Bazaar Watch API v6.1", "time": datetime.now(timezone.utc).isoformat(), "cache_ages": ages}


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
        "crypto":        cache["crypto"]["data"],
        "intraday":      _intraday_snapshot(),
        "us":            cache["us"]["data"],
        "news":          merged_news,            # ← NSE items at top, RSS below
        "giftnifty":     _gn_cache.get("giftnifty", {}),
        "summary":       cache["summary"]["data"],
        "pre_market":    cache["pre_market"]["data"],
        "hourly":        cache["hourly"]["data"],
        "post_market":   cache["post_market"]["data"],
        "sparklines":    cache["sparklines"]["data"],
        "futures":       cache["futures"]["data"],
        "articles":      cache["articles"]["data"],
        "timestamp":     datetime.now(timezone.utc).isoformat(),
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

# ── Direct Gift Nifty endpoints — polled every 10s by frontend ───────────────
# Replaces the separate Node proxy service entirely.
# Same response shape the frontend already expects.
@app.get("/debug-mc")
async def debug_mc():
    """Show raw Moneycontrol response for Gift Nifty — for debugging parser."""
    url = GN_SOURCES["giftnifty"]
    async with httpx.AsyncClient(timeout=10, headers=MC_HEADERS) as client:
        r = await client.get(url)
        return JSONResponse({
            "status": r.status_code,
            "url": url,
            "raw": r.json() if "json" in r.headers.get("content-type","") else r.text[:2000]
        })

@app.get("/quote")
async def quote_gift_nifty():
    try:
        data = await _get_gift_cached("giftnifty")
        return JSONResponse(data)
    except Exception as e:
        # Serve stale cache if available
        stale = _gn_cache.get("giftnifty")
        if stale:
            return JSONResponse({**stale, "stale": True, "error": str(e)})
        return JSONResponse({"ok": False, "error": str(e)}, status_code=503)


@app.get("/api/summary")
def get_summary():   return JSONResponse(cache["summary"]["data"])


class SummaryPayload(BaseModel):
    key:     str
    text:    str
    session: str = "manual"   # "manual" | "pre_market" | "hourly" | "post_market"

@app.post("/api/admin/summary")
def post_summary(payload: SummaryPayload):
    """
    Push a market summary from the admin panel.
    Requires ADMIN_KEY environment variable to be set on Render.
    """
    if not ADMIN_KEY:
        raise HTTPException(status_code=503, detail="ADMIN_KEY not configured on server")
    if payload.key != ADMIN_KEY:
        raise HTTPException(status_code=401, detail="Invalid admin key")
    if not payload.text or len(payload.text.strip()) < 10:
        raise HTTPException(status_code=400, detail="Summary too short")

    data = {
        "summary":      payload.text.strip(),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "session":      payload.session,
        "source":       "manual"}
    cache["summary"] = {"data": data, "ts": time.time()}
    _save_persisted("summary", data)   # write to disk immediately
    log.info(f"✅ Manual summary pushed ({len(payload.text)} chars, session={payload.session})")
    return {"ok": True, "chars": len(payload.text), "ts": data["generated_at"]}


@app.delete("/api/admin/summary")
def clear_summary(key: str):
    """Clear the current summary."""
    if not ADMIN_KEY or key != ADMIN_KEY:
        raise HTTPException(status_code=401, detail="Invalid admin key")
    cache["summary"] = {"data": {}, "ts": 0}
    _save_persisted("summary", {})
    log.info("Summary cleared")
    return {"ok": True}


# ── ARTICLE ENDPOINTS ─────────────────────────────────────────────────────────

class ArticlePayload(BaseModel):
    key:      str
    id:       str = ""          # empty = new article
    title:    str
    category: str = "stock"     # "stock" | "commodity"
    ticker:   str = ""
    tvSymbol: str = ""
    author:   str = "Bazaar Watch Research"
    body:     str

@app.post("/api/admin/article")
def save_article(payload: ArticlePayload):
    """Create or update a published article."""
    if not ADMIN_KEY:
        raise HTTPException(status_code=503, detail="ADMIN_KEY not configured on server")
    if payload.key != ADMIN_KEY:
        raise HTTPException(status_code=401, detail="Invalid admin key")
    if not payload.title.strip():
        raise HTTPException(status_code=400, detail="Title required")
    if not payload.body.strip():
        raise HTTPException(status_code=400, detail="Body required")

    articles = list(cache["articles"]["data"])

    if payload.id:
        # Update existing
        idx = next((i for i, a in enumerate(articles) if a.get("id") == payload.id), -1)
        if idx >= 0:
            articles[idx] = {
                **articles[idx],
                "title":     payload.title.strip(),
                "category":  payload.category,
                "ticker":    payload.ticker.strip().upper(),
                "tvSymbol":  payload.tvSymbol.strip(),
                "author":    payload.author.strip(),
                "body":      payload.body.strip(),
                "updatedAt": datetime.now(timezone.utc).isoformat()}
            log.info(f"✅ Article updated: {payload.title[:40]}")
        else:
            raise HTTPException(status_code=404, detail="Article not found")
    else:
        # New article — prepend so newest is first
        import time as _time
        new_id = f"art_{int(_time.time() * 1000)}"
        articles.insert(0, {
            "id":       new_id,
            "title":    payload.title.strip(),
            "category": payload.category,
            "ticker":   payload.ticker.strip().upper(),
            "tvSymbol": payload.tvSymbol.strip(),
            "author":   payload.author.strip(),
            "body":     payload.body.strip(),
            "date":     datetime.now(timezone.utc).isoformat(),
            "updatedAt":datetime.now(timezone.utc).isoformat()})
        log.info(f"✅ Article created: {payload.title[:40]} (id={new_id})")

    cache["articles"] = {"data": articles, "ts": time.time()}
    _save_persisted("articles", articles)   # write to disk immediately
    return {"ok": True, "count": len(articles)}


@app.delete("/api/admin/article")
def delete_article(key: str, id: str):
    """Delete an article by id."""
    if not ADMIN_KEY or key != ADMIN_KEY:
        raise HTTPException(status_code=401, detail="Invalid admin key")
    articles = [a for a in cache["articles"]["data"] if a.get("id") != id]
    cache["articles"] = {"data": articles, "ts": time.time()}
    _save_persisted("articles", articles)   # update disk immediately
    log.info(f"✅ Article deleted: {id}")
    return {"ok": True, "count": len(articles)}


@app.get("/api/articles")
def get_articles():
    """Public endpoint — returns all published articles."""
    return JSONResponse(cache["articles"]["data"])

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



# ── ARTICLE + SUMMARY ENDPOINTS ────────────────────────────────────────────────────
@app.get("/api/nse-news")
def get_nse_news():
    """NSE corporate announcements, board meetings, corporate actions."""
    return JSONResponse({
        "ok":    True,
        "items": cache["nse_news"]["data"],
        "count": len(cache["nse_news"]["data"]),
        "age_s": round(time.time() - cache["nse_news"]["ts"]) if cache["nse_news"]["ts"] else None})


@app.get("/api/debug-nse-news")
async def debug_nse_news():
    """
    Test all India stocks news sources live — bypasses cache.
    Shows count from each source: BSE API, Google News, RSS feeds.
    """
    try:
        bse    = await fetch_bse_announcements()
        google = await fetch_google_news_india()
        rss    = await fetch_india_stock_rss()
        merged = await fetch_india_stocks_news()
        return JSONResponse({
            "ok":           True,
            "total":        len(merged),
            "bse_count":    len(bse),
            "google_count": len(google),
            "rss_count":    len(rss),
            "bse_sample":   bse[:3],
            "google_sample":google[:3],
            "rss_sample":   rss[:3]})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)})


@app.get("/api/debug-options")
async def debug_options():
    """Live test of all 5 option chain sources. Check this to diagnose failures."""
    from urllib.parse import quote
    import random, string
    results = {
        "nsepython_available": _HAS_NSEPYTHON,
        "yfinance_available":  _HAS_YFINANCE,
        "scraper_key_set":     bool(SCRAPER_API_KEY)}
    async with httpx.AsyncClient(headers=YAHOO_HEADERS, follow_redirects=True, timeout=40) as client:
        # Test nsepython
        if _HAS_NSEPYTHON:
            try:
                loop = asyncio.get_running_loop()
                raw = await loop.run_in_executor(_thread_pool, _nse_scrapper, "NIFTY")
                results["nsepython_nifty"] = "ok" if (raw and "records" in raw) else "empty"
            except Exception as e:
                results["nsepython_nifty"] = str(e)

        # Test yfinance
        if _HAS_YFINANCE:
            try:
                loop = asyncio.get_running_loop()
                def _yf_test():
                    t = yf.Ticker("^NSEI")
                    return t.options
                dates = await loop.run_in_executor(_thread_pool, _yf_test)
                results["yfinance_nifty_dates"] = list(dates[:3]) if dates else []
            except Exception as e:
                results["yfinance_nifty"] = str(e)

        # Test NSE direct
        try:
            r1 = await client.get("https://www.nseindia.com", headers=_NSE_HTML_HDR, timeout=10)
            results["nse_homepage_status"] = r1.status_code
            results["nse_cookies_received"] = list(dict(r1.cookies).keys())
        except Exception as e:
            results["nse_homepage_error"] = str(e)

        # Test Yahoo crumb
        try:
            crumb, cookies = None, None
            for url in ["https://fc.yahoo.com", "https://query2.finance.yahoo.com"]:
                r = await client.get(url, timeout=8)
                if r.cookies: cookies = dict(r.cookies); break
            if cookies:
                r = await client.get("https://query2.finance.yahoo.com/v1/test/getcrumb",
                                     cookies=cookies, timeout=8)
                results["yahoo_crumb_status"] = r.status_code
                results["yahoo_crumb_ok"] = r.status_code == 200 and bool(r.text.strip())
        except Exception as e:
            results["yahoo_crumb_error"] = str(e)

        # Test scrape.do
        if SCRAPER_API_KEY:
            try:
                sid = "dbg-" + "".join(random.choices(string.ascii_lowercase + string.digits, k=6))
                r1 = await client.get(
                    f"https://api.scrape.do?token={SCRAPER_API_KEY}"
                    f"&url={quote('https://www.nseindia.com/option-chain',safe='')}&sessionId={sid}",
                    timeout=30)
                results["scrapedo_step1"] = r1.status_code
            except Exception as e:
                results["scrapedo_error"] = str(e)

    return JSONResponse(results)


@app.get("/api/health")
def health():
    now = time.time()
    return {
        "status":      "ok",
        "version":     "6.1",
        "finnhub_key":    "set" if FINNHUB_KEY     else "missing",
        "claude_key":     "set" if CLAUDE_KEY      else "not configured",
        "scraper_key":    "set" if SCRAPER_API_KEY else "missing",
        "scrape_do_key":  "no longer needed — using free Moneycontrol API",
        "admin_key":      "set" if ADMIN_KEY       else "NOT SET — admin panel won't work",
        "articles":       len(cache["articles"]["data"]),
        "gift_session":   get_gift_session(),
        "cache": {
            k: {
                "age_s":    round(now - v["ts"]) if v["ts"] else None,
                "has_data": bool(v["data"]),
                "count":    len(v["data"]) if isinstance(v["data"], list) else None}
            for k, v in cache.items()
        }
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=False)