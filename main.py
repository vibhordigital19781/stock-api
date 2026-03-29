"""
Bazaar Watch — Backend API v5.6 (STABLE)
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
    "heatmap":       {"data": [], "ts": 0},
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
        "GC=F":  "gold_usd",       # COMEX Gold $/oz
        "SI=F":  "silver_usd",     # COMEX Silver $/oz
        "HG=F":  "copper_usd",     # COMEX Copper $/lb
        "ALI=F": "aluminium_usd",  # Aluminium $/lb
        "ZNC=F": "zinc_usd",       # Zinc cents/lb
        "NI=F":  "nickel_usd",     # LME Nickel $/t
        "PL=F":  "platinum_usd",   # Platinum $/oz
        "CL=F":  "crude_usd",      # WTI Crude $/bbl
        "NG=F":  "natgas_usd",     # Natural Gas $/mmBtu
    }
    async with httpx.AsyncClient(headers=YAHOO_HEADERS, timeout=15, follow_redirects=True) as client:
        for sym, key in symbol_map.items():
            q = await yahoo_quote(client, sym)
            if q:
                result[key] = q
                log.info(f"✅ {key}: {q['price']}")
            await asyncio.sleep(0.4)

        # Nickel now fetched via NI=F above — Finnhub fallback removed

    return result


async def fetch_us_markets() -> dict:
    """
    US indices via Yahoo Finance — real indices, not ETF proxies.
    ^DJI = Dow Jones Industrial Average
    ^GSPC = S&P 500
    ^IXIC = Nasdaq Composite
    DX-Y.NYB = US Dollar Index (ICE)
    ^VIX = CBOE Volatility Index
    """
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
    """RSS news from multiple Indian financial sources."""
    feeds = [
        # Indian economic & market news — no recommendations
        ("https://economictimes.indiatimes.com/markets/rss.cms",              "ET Markets"),
        ("https://economictimes.indiatimes.com/markets/commodities/rss.cms",  "ET Commodities"),
        ("https://economictimes.indiatimes.com/economy/rss.cms",              "ET Economy"),
        ("https://economictimes.indiatimes.com/markets/stocks/rss.cms",       "ET Stocks"),
        ("https://www.business-standard.com/rss/markets-106.rss",             "Business Standard"),
        ("https://www.business-standard.com/rss/economy-policy-101.rss",      "BS Economy"),
        ("https://www.business-standard.com/rss/finance-100.rss",             "BS Finance"),
        ("https://www.moneycontrol.com/rss/economy.xml",                      "Moneycontrol Economy"),
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



# ── F&O HEATMAP STOCKS ────────────────────────────────────────────────────────
# ~180 NSE/BSE F&O eligible stocks, Yahoo Finance .NS suffix
FNO_STOCKS = [
    # ── BANKING & FINANCE ──
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
    # ── IT ──
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
    # ── ENERGY & OIL ──
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
    # ── AUTO ──
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
    # ── PHARMA ──
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
    # ── METALS & MINING ──
    ("TATASTEEL.NS",  "Tata Steel",     "Metals"),
    ("JSWSTEEL.NS",   "JSW Steel",      "Metals"),
    ("HINDALCO.NS",   "Hindalco",       "Metals"),
    ("VEDL.NS",       "Vedanta",        "Metals"),
    ("COALINDIA.NS",  "Coal India",     "Metals"),
    ("NMDC.NS",       "NMDC",           "Metals"),
    ("SAIL.NS",       "SAIL",           "Metals"),
    ("HINDCOPPER.NS", "Hind Copper",    "Metals"),
    ("NATIONALUM.NS", "NALCO",          "Metals"),
    # ── CONSUMER & FMCG ──
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
    # ── INDUSTRIALS & INFRA ──
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
    # ── TELECOM ──
    ("BHARTIARTL.NS", "Airtel",         "Telecom"),
    ("IDEA.NS",       "Vi",             "Telecom"),
    ("INDUSTOWER.NS", "Indus Towers",   "Telecom"),
    # ── UTILITIES ──
    ("NTPC.NS",       "NTPC",           "Utilities"),
    ("POWERGRID.NS",  "Power Grid",     "Utilities"),
    ("ADANIPOWER.NS", "Adani Power",    "Utilities"),
    ("TORNTPOWER.NS", "Torrent Power",  "Utilities"),
    ("CESC.NS",       "CESC",           "Utilities"),
    # ── REAL ESTATE ──
    ("DLF.NS",        "DLF",            "Real Estate"),
    ("GODREJPROP.NS", "Godrej Prop",    "Real Estate"),
    ("OBEROIRLTY.NS", "Oberoi Realty",  "Real Estate"),
    ("PRESTIGE.NS",   "Prestige",       "Real Estate"),
    ("PHOENIXLTD.NS", "Phoenix",        "Real Estate"),
    ("BRIGADE.NS",    "Brigade",        "Real Estate"),
    # ── MATERIALS & CEMENT ──
    ("ULTRACEMCO.NS", "UltraCem",       "Cement"),
    ("GRASIM.NS",     "Grasim",         "Cement"),
    ("SHREECEM.NS",   "Shree Cem",      "Cement"),
    ("AMBUJACEM.NS",  "Ambuja Cem",     "Cement"),
    ("ACC.NS",        "ACC",            "Cement"),
    ("RAMCOCEM.NS",   "Ramco Cem",      "Cement"),
    # ── CONSUMER DISCRETIONARY ──
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
    # ── HEALTHCARE ──
    ("APOLLOHOSP.NS", "Apollo Hosp",    "Healthcare"),
    ("MAXHEALTH.NS",  "Max Health",     "Healthcare"),
    ("FORTIS.NS",     "Fortis",         "Healthcare"),
    ("METROPOLIS.NS", "Metropolis",     "Healthcare"),
    ("LALPATHLAB.NS", "Dr Lal Path",    "Healthcare"),
    # ── CHEMICALS ──
    ("SRF.NS",        "SRF",            "Chemicals"),
    ("AARTIIND.NS",   "Aarti Ind",      "Chemicals"),
    ("NAVINFLUOR.NS", "Navin Fluor",    "Chemicals"),
    ("DEEPAKNTR.NS",  "Deepak Nitrite", "Chemicals"),
    ("PIIND.NS",      "PI Ind",         "Chemicals"),
    ("UPL.NS",        "UPL",            "Chemicals"),
    # ── EXCHANGE & CAPITAL MARKETS ──
    ("BSE.NS",        "BSE",            "Capital Mkts"),
    ("MCX.NS",        "MCX",            "Capital Mkts"),
    ("CDSL.NS",       "CDSL",           "Capital Mkts"),
    ("ANGELONE.NS",   "Angel One",      "Capital Mkts"),
    ("ICICIPRULI.NS", "ICICI Pru Life", "Capital Mkts"),
    # ── NEW AGE / TECH ──
    ("PAYTM.NS",      "Paytm",          "New Age Tech"),
    ("POLICYBZR.NS",  "PB Fintech",     "New Age Tech"),
    ("DELHIVERY.NS",  "Delhivery",      "New Age Tech"),
    ("IRCTC.NS",      "IRCTC",          "New Age Tech"),
]

async def fetch_heatmap_stocks() -> list:
    """Fetch F&O stock prices for India heatmap. Runs every 5 minutes."""
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
                })
            await asyncio.sleep(0.2)
    log.info(f"✅ Heatmap stocks: {len(result)}/{len(FNO_STOCKS)} fetched")
    return result

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

        # Heatmap stocks every 5 mins (many calls)
        heatmap_age = time.time() - cache["heatmap"]["ts"] if cache["heatmap"]["ts"] else 9999
        if heatmap_age > 300:
            heatmap = await fetch_heatmap_stocks()
            if heatmap:
                cache["heatmap"] = {"data": heatmap, "ts": time.time()}

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
    return {"status": "Bazaar Watch API v5.6", "time": datetime.now().isoformat(), "cache_ages": ages}


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

@app.get("/api/heatmap")
def get_heatmap():
    return JSONResponse(cache["heatmap"]["data"])

@app.get("/api/health")
def health():
    now = time.time()
    return {
        "status":        "ok",
        "version":       "5.6",
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
