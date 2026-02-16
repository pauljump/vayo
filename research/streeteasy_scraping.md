# StreetEasy Scraping Research

**Date:** 2026-02-16
**Goal:** Scrape 943K building pages + unit price history without a browser

## What We Know

- StreetEasy uses **PerimeterX (PX)** bot protection (App ID: `PXcZdhF737`)
- PX is deployed via CloudFront Lambda@Edge (header `X-Px-Blocked: 1`)
- Direct curl, Playwright, Selenium, undetected-chromedriver → all blocked
- **AppleScript controlling Chrome works** but is slow (~2-3s/page)
- Adding `.json` to URLs → 404 from Next.js (NOT PX blocked — app responded)
- GraphQL endpoint `api-internal.streeteasy.com/graphql` → 403
- Sitemaps downloadable with Chrome cookies via curl
- StreetEasy runs on **Next.js App Router** (HDP app at `/hdp/`)

## PX Cookie Details

Cookies to extract from Chrome:
- `_pxvid` — visitor ID, long-lived (persists across sessions)
- `_px3` — session token from JS challenge, shorter-lived (~1 hour)
- `_pxde` — encrypted data token
- `_pxhd` — human detection token

PX checks: IP reputation, TLS fingerprint, HTTP/2 fingerprint, user-agent consistency, JS challenge tokens, cookie state.

## Approaches Ranked

### 1. Cookie + TLS Impersonation ⭐ BEST BET

- Extract PX cookies from real Chrome session
- Use `tls-client` or `curl-impersonate` to replicate Chrome's TLS fingerprint
- Standard Python `requests` has distinctive TLS handshake → immediately flagged
- Expected throughput: 1-2 req/s per session
- Parallelize with multiple cookie sessions for 5-10 req/s
- Cookies expire ~1 hour, need periodic refresh
- **Combine with RSC headers** for structured data (see below)

### 2. Next.js RSC Flight Format

During client-side navigation, Next.js fetches RSC payloads with special headers:

```
RSC: 1
Next-Router-State-Tree: ["",["building",["slug",{"slug":"the-gramercy"},"c"],{}],{},[],null]
Next-Url: /building/the-gramercy
Next-Router-Prefetch: 1  (for lighter prefetch payload)
```

Returns compact line-based "flight format" with all component data — much smaller and more structured than full HTML. **Still needs PX bypass** (TLS impersonation + cookies).

### 3. Skip Scraping for Sales Data

NYC Open Data already has most sale data for free:
- **DOF Annualized Sales** (`w2pb-icbu`): 760K+ sales with prices, dates, sqft
- **ACRIS** (already pulling): every deed, mortgage since 1966 — 16.9M records
- **PLUTO** (`64uk-42ks`): building details for every tax lot

SE scraping is only essential for:
- **Rental price history** (no public source)
- **Listing activity** (broker, days on market, listed/delisted dates)
- **StreetEasy-specific building info** (amenities, pet policy, building type)

### 4. Parallel Chrome Profiles

Scale the AppleScript approach:
- Run 5-10 Chrome instances with separate `--user-data-dir`
- Each AppleScript targets a specific window
- 10 instances × 2s/page = ~5 pages/sec
- **943K buildings ÷ 5/sec = ~52 hours (2 days)**

### 5. Smart Proxy Services ($$$)

- ScrapeOps, ScraperAPI, Oxylabs maintain PX bypasses
- Cost: $2,000-5,000+ for 943K pages
- Handle JS challenges, rotate residential IPs

## Not Viable

- **No public SE API** — no developer portal, no documented endpoints
- **Zillow APIs** — don't expose SE-specific data (owned by Zillow Group)
- **Google Cache** — deprecated direct access
- **Wayback Machine** — almost no SE building pages archived
- **Mobile app API** — likely certificate-pinned, no known reverse engineering
- **SE GraphQL** (`api-internal.streeteasy.com/graphql`) — returns 403

## Recommended Strategy

1. **Phase 1:** Use ACRIS + DOF Sales + PLUTO for sale history (free, already pulling)
2. **Phase 2:** Prototype cookie + tls-client + RSC approach for SE-specific data
3. **Phase 3:** If Phase 2 works, parallelize. If not, scale AppleScript with multiple Chrome profiles.
