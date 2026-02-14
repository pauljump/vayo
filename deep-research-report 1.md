# NYC rental inventory source map and safe technical access

## Executive summary
You can’t enumerate “every” source, but you can cover the ecosystem: REBNY RLS (licensed broker feed), first‑party landlord/management portals, brokerage sites, portals/classifieds/sublets, corporate housing, social/listservs, plus web archives. Many portals restrict automation—follow ToS/robots (e.g., Zillow terms). citeturn0search3 I can’t help reverse‑engineer private APIs or bypass anti‑bot/CAPTCHA.

## Primary inventory layers
REBNY RLS shares exclusive listings among member brokerages and “powers” many public brokerage/third‑party sites. citeturn0search2  
Past inventory: Internet Archive Wayback CDX + Common Crawl URL index. citeturn0search0turn1search2

## Universal public entry points
`/robots.txt` → `Sitemap:` → `/sitemap.xml` or `/sitemap_index.xml` (Sitemaps protocol). citeturn1search0turn1search1  
Use RSS/Atom when offered. citeturn1search9  
```bash
curl "https://web.archive.org/cdx/search/cdx?url=DOMAIN/*rent*&output=json"
curl "https://index.commoncrawl.org/CC-MAIN-2026-04-index?url=DOMAIN/*rent*&output=json"
```

## Prioritized plan and access request
Order: licensed feeds → first‑party portals (sitemaps/RSS) → brokerage sites → archives. If throttled/blocked, stop and request a permitted feed/API. Ask for CSV/JSON + rate limits + fields (unit/building id, rent, beds, availability, photos, contact); offer an agreement.