/**
 * Bazaar Watch — Service Worker v1.0
 * ─────────────────────────────────────────────────────────────────────────────
 * Strategy:
 *   • Static assets  → Cache-first  (HTML pages, fonts, icons, manifest)
 *   • API calls      → Network-first (fresh market data preferred, stale OK)
 *   • Offline        → Serve cached page or offline fallback
 */

const CACHE_VERSION  = 'bw-v1.0';
const STATIC_CACHE   = `${CACHE_VERSION}-static`;
const API_CACHE      = `${CACHE_VERSION}-api`;
const OFFLINE_URL    = '/offline.html';

// ── Pages to pre-cache on install ────────────────────────────────────────────
const PRECACHE_PAGES = [
  '/',
  '/gift-nifty',
  '/pre-market-cues',
  '/nifty-50-live',
  '/gold-price',
  '/crude-oil-price',
  '/base-metals',
  '/india-vix',
  '/fno-heatmap',
  '/economic-calendar',
  '/us-bond-yields',
  '/global-markets',
  '/crypto',
  '/market-news',
  '/stock-analysis',
  '/site.webmanifest',
  '/favicon.svg',
  OFFLINE_URL,
];

// ── INSTALL — pre-cache all pages ─────────────────────────────────────────────
self.addEventListener('install', event => {
  event.waitUntil(
    caches.open(STATIC_CACHE).then(cache => {
      console.log('[SW] Pre-caching pages...');
      // Cache pages individually so one failure doesn't break the whole install
      return Promise.allSettled(
        PRECACHE_PAGES.map(url =>
          cache.add(url).catch(err => console.warn(`[SW] Pre-cache failed: ${url}`, err))
        )
      );
    }).then(() => self.skipWaiting())
  );
});

// ── ACTIVATE — clean up old caches ────────────────────────────────────────────
self.addEventListener('activate', event => {
  event.waitUntil(
    caches.keys().then(keys =>
      Promise.all(
        keys
          .filter(k => k.startsWith('bw-') && !k.startsWith(CACHE_VERSION))
          .map(k => {
            console.log('[SW] Deleting old cache:', k);
            return caches.delete(k);
          })
      )
    ).then(() => self.clients.claim())
  );
});

// ── FETCH — routing logic ──────────────────────────────────────────────────────
self.addEventListener('fetch', event => {
  const { request } = event;
  const url = new URL(request.url);

  // Only handle same-origin requests
  if (url.origin !== self.location.origin) {
    // For Google Fonts — cache-first
    if (url.hostname === 'fonts.googleapis.com' || url.hostname === 'fonts.gstatic.com') {
      event.respondWith(cacheFirst(request, STATIC_CACHE));
    }
    return;
  }

  // API calls — network-first (fresh data), fall back to cache
  if (url.pathname.startsWith('/api/') ||
      url.pathname === '/quote' ||
      url.pathname === '/quote-bank') {
    event.respondWith(networkFirst(request, API_CACHE, 8000));
    return;
  }

  // Static page navigation — cache-first, fall back to offline page
  if (request.mode === 'navigate') {
    event.respondWith(
      fetch(request)
        .then(response => {
          // Update cache on successful fetch
          if (response.ok) {
            const clone = response.clone();
            caches.open(STATIC_CACHE).then(c => c.put(request, clone));
          }
          return response;
        })
        .catch(() =>
          caches.match(request)
            .then(cached => cached || caches.match(OFFLINE_URL))
        )
    );
    return;
  }

  // All other assets — cache-first
  event.respondWith(cacheFirst(request, STATIC_CACHE));
});

// ── STRATEGIES ────────────────────────────────────────────────────────────────

/** Cache-first: serve from cache, fetch + update if missing */
async function cacheFirst(request, cacheName) {
  const cached = await caches.match(request);
  if (cached) return cached;
  try {
    const response = await fetch(request);
    if (response.ok) {
      const cache = await caches.open(cacheName);
      cache.put(request, response.clone());
    }
    return response;
  } catch {
    return new Response('Offline', { status: 503 });
  }
}

/** Network-first: try network with timeout, fall back to cache */
async function networkFirst(request, cacheName, timeoutMs = 8000) {
  const cache = await caches.open(cacheName);
  try {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeoutMs);
    const response = await fetch(request, { signal: controller.signal });
    clearTimeout(timer);
    if (response.ok) {
      cache.put(request, response.clone());
    }
    return response;
  } catch {
    const cached = await cache.match(request);
    if (cached) return cached;
    // Return empty JSON for API calls so the frontend doesn't crash
    return new Response(JSON.stringify({ ok: false, offline: true }), {
      status: 200,
      headers: { 'Content-Type': 'application/json' }
    });
  }
}
