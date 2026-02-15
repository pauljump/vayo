# Vayo - Real Estate Listing Scraping Strategy

**Research Date:** 2026-02-12

---

## Key Finding: Don't Build from Scratch

After researching GitHub, APIs, and data sources, here's the optimal approach:

---

## Tier 1: Leverage Existing Open Source Tools ✅

### 1. **NYCDB - Database of NYC Housing Data**

**What it is:**
- Open-source PostgreSQL database consolidating 40+ NYC housing datasets
- Maintained by Housing Data Coalition
- Licensed under GNU Affero GPL
- Last updated: January 2025

**🔗 Repository:** [github.com/nycdb/nycdb](https://github.com/nycdb/nycdb)

**What it contains (relevant to us):**
- ✅ **PLUTO** - Property characteristics for every lot
- ✅ **ACRIS** - Real property transactions (we already have this)
- ✅ **HPD Registrations** - Rental building data (we have this)
- ✅ **HPD Violations & Complaints** (we have this)
- ✅ **DOB Certificate of Occupancy** - Official unit counts
- ✅ **Property Address Directory (PAD)** - Address normalization
- ✅ **DOF Tax Bills** - Rent stabilization unit counts (via scraping)
- ✅ **Eviction records** (we have this)

**Why this matters:**
- They've already solved data normalization, address matching, and ETL pipelines
- Includes data we DON'T have yet (PLUTO, PAD, DOB COs, tax bill rent-stab counts)
- We can either clone their database or use their Python CLI to load missing datasets

**Next step:**
```bash
# Install NYCDB
pip install nycdb

# Load specific datasets we're missing
nycdb --download pluto
nycdb --download pad
nycdb --download dob_certificate_occupancy
```

---

## Tier 2: NYC Official Open Data APIs ✅

### 2. **NYC Open Data - Socrata API**

**Housing Connect Lottery Data:**
- **Advertised Lotteries by Building** (`nibs-na6y`)
  - Location, BBL, unit counts by income level
  - Active since July 2020
- **Advertised Lotteries by Lottery** (`vy5i-a666`)
  - Lottery status, preferences, tenant selection

**🔗 Source:** [data.cityofnewyork.us](https://data.cityofnewyork.us/)

**API Access:**
```bash
# Example: Get affordable housing lotteries
curl "https://data.cityofnewyork.us/resource/nibs-na6y.json?\$limit=10000"
```

**Contact:** OpenData@hpd.nyc.gov

---

### 3. **HPD Data Feeds (GitHub)**

**Official repository:** [github.com/CityOfNewYork/HPD-Data-Feeds](https://github.com/CityOfNewYork/HPD-Data-Feeds)

- Official data feeds from NYC Department of Housing
- Buildings subject to HPD jurisdiction
- Open violations
- Affordable housing production

---

## Tier 3: Third-Party APIs (Paid but Legal)

### 4. **Reasier - NYC Rental Data API**

**What it offers:**
- 3+ years of historical StreetEasy data
- Historical prices, market trends, amenities
- Thousands of NYC apartments
- Explicitly legal/licensed

**🔗 Website:** [reasier.com/api-access](https://www.reasier.com/api-access)

**Pros:**
- Legitimate, licensed access to StreetEasy data
- No scraping legal risk
- Historical data (not just current)

**Cons:**
- Paid service (pricing unknown)
- Limited to StreetEasy coverage

**Next step:** Contact for pricing and trial access

---

### 5. **RentCast API**

**What it offers:**
- Property data API for rentals and sales
- Coverage across US including NYC
- Property details, valuations, market trends

**🔗 Website:** [rentcast.io/api](https://www.rentcast.io/api)

**Next step:** Evaluate coverage and pricing

---

## Tier 4: REBNY RLS Access (Professional Route)

### 6. **REBNY Residential Listing Service**

**What it is:**
- Official broker-to-broker listing sharing system
- Powers many public sites (StreetEasy, broker sites)
- Migrated from RETS to RESO Web API (modern standard)
- ~90M views/month across 100+ tech providers

**🔗 Info:** [rebny.com/rls](https://www.rebny.com/rls/)

**How to access:**
- Must be REBNY member or approved vendor
- Trestle platform handles data distribution
- RESO-compliant API

**Pros:**
- Most comprehensive NYC broker data
- Legal, official access
- Real-time updates

**Cons:**
- Requires membership/vendor agreement
- Likely expensive
- Business relationship needed

**Next step:**
- Contact REBNY for vendor partnership inquiry
- Email: (check rebny.com/contact)

---

## Tier 5: Compliant Scraping (Sitemap/RSS Method)

### 7. **StreetEasy XML Feed**

**Available:**
- Blog RSS feed: `streeteasy.com/blog/feed`
- XML feed format page: `streeteasy.com/home/feed_format`
- Data Dashboard: `streeteasy.com/blog/data-dashboard/`

**Note:** StreetEasy has blocked direct HTML scraping since 2017 (Distil Networks anti-bot)

**Legal approach:**
- Use public RSS/XML feeds only
- Respect robots.txt
- Contact press@streeteasy.com for data partnership inquiry

---

### 8. **Landlord Direct Sites (Sitemap Method)**

**Target sites:**
- TF Cornerstone: `tfc.com`
- Rockrose: `rockrose.com/availabilities/`
- Durst Organization: `durst.org`
- Two Trees: `twotreesny.com`
- Glenwood: `glenwoodnyc.com`
- Related Rentals: `relatedrentals.com`
- Equity Residential: `equityapartments.com`

**Method:**
1. Check `robots.txt` and `sitemap.xml` for each
2. Use sitemap to discover availability pages
3. Parse structured data (often JSON-LD or microdata)
4. Respect rate limits (1 req/sec default)

**Example:**
```bash
# Check what's allowed
curl https://rockrose.com/robots.txt

# Get sitemap
curl https://rockrose.com/sitemap.xml

# Parse availability feed
curl https://rockrose.com/availabilities/ | jq '.units'
```

**Pros:**
- Compliant (using public sitemaps)
- High-quality data (direct from source)
- Often have APIs or structured feeds

**Cons:**
- Manual setup per site
- Some may require authentication

---

## Tier 6: GitHub Scraper Projects (Study, Don't Use Directly)

### 9. **Existing Scrapers (For Reference Only)**

**StreetEasy scrapers:**
- [purcelba/streeteasy_scrape](https://github.com/purcelba/streeteasy_scrape) - **Archived, blocked since 2017**
- [NicholasMontalbano/apt_finder_streeteasy_scrape](https://github.com/NicholasMontalbano/apt_finder_streeteasy_scrape) - Webscraper with neighborhood filtering
- [andreychernih/streeteasy-parser](https://github.com/andreychernih/streeteasy-parser) - Sales and rental parser

**Zillow scrapers:**
- [scrapehero/zillow_real_estate](https://github.com/scrapehero/zillow_real_estate) - LXML-based scraper by zip code
- [TrkElnIt/zillow-scraper](https://github.com/TrkElnIt/zillow-scraper) - Extract listing and agent data

**Real estate general:**
- [oussafik/Web-Scraping-RealEstate-Beautifulsoup](https://github.com/oussafik/Web-Scraping-RealEstate-Beautifulsoup) - Generic framework
- [mominurr/Real-Estate-Web-Scraping](https://github.com/mominurr/Real-Estate-Web-Scraping) - IP blocking bypass

**⚠️ Important:**
- Most older scrapers are blocked (StreetEasy, Zillow have anti-bot protection)
- Use these for learning techniques, not production
- Focus on sitemap/RSS methods instead

---

## Recommended Implementation Plan

### Phase 1: Low-Hanging Fruit (Week 1)

**1. Install NYCDB datasets we're missing**
```bash
pip install nycdb
nycdb --download pluto
nycdb --download pad
nycdb --download dob_certificate_occupancy
```

**Benefit:** Get PLUTO (comprehensive building data), PAD (address matching), DOB COs (official unit counts)

**2. Query NYC Open Data APIs**
- Housing Connect lottery data
- HPD affordable housing production
- Latest ACRIS updates

**Benefit:** Free, official, legally clear

---

### Phase 2: Commercial API Evaluation (Week 2)

**3. Contact Reasier for trial**
- Evaluate StreetEasy historical data coverage
- Get pricing for API access
- Compare to building our own scraper

**4. Contact REBNY for vendor partnership**
- Inquire about RLS access requirements
- Understand costs and technical requirements
- Evaluate if worth the investment

---

### Phase 3: Compliant Scraping (Weeks 3-4)

**5. Build landlord site scrapers**
- Start with Rockrose (has clean `/availabilities/` feed)
- TF Cornerstone, Durst, Two Trees, etc.
- Use sitemap → structured data → database pipeline

**6. Explore StreetEasy official data**
- Contact press@streeteasy.com for partnership
- Use public RSS/data dashboard in meantime
- Do NOT scrape HTML directly (blocked + ToS violation)

---

### Phase 4: Ongoing Monitoring (Ongoing)

**7. Set up daily/hourly feeds**
- NYC Open Data: Daily updates via Socrata API
- Landlord sites: Hourly checks for new availability
- ACRIS: Daily transaction feed
- NYCDB: Weekly refresh of changing datasets

---

## Legal/Ethical Guidelines

### ✅ DO:
- Use official APIs (NYC Open Data, commercial providers)
- Follow robots.txt and sitemaps
- Respect rate limits (default: 1 req/sec)
- Identify your bot clearly in User-Agent
- Request official partnerships when possible
- Use NYCDB for open-source datasets

### ❌ DON'T:
- Scrape sites that explicitly block it (StreetEasy HTML)
- Bypass CAPTCHAs or anti-bot measures
- Ignore robots.txt directives
- Use scrapers that violate ToS
- Overwhelm small sites with requests
- Misrepresent your scraper as a browser

---

## Tools & Tech Stack

### Data Collection:
- **NYCDB CLI** - For official datasets
- **Python Requests** - For API calls
- **BeautifulSoup/lxml** - For parsing HTML (when allowed)
- **Scrapy** - For sitemap-based structured scraping
- **Playwright** - For JavaScript-heavy sites (if needed)

### Data Processing:
- **PostgreSQL** - Primary database (already have 30GB)
- **SQLite** - For intermediate processing
- **pandas** - Data manipulation
- **dbt** - Data transformations

### Monitoring:
- **Airflow** or **Prefect** - Orchestration
- **Grafana** - Monitoring dashboards
- **Sentry** - Error tracking

---

## Next Actions

**Immediate (Today):**
1. Clone NYCDB repo and review their data loading scripts
2. Contact Reasier for trial/pricing

**This Week:**
1. Install NYCDB and load PLUTO + PAD datasets
2. Set up NYC Open Data API access (free)
3. Build proof-of-concept scraper for Rockrose availabilities

**Next Week:**
1. Evaluate Reasier pricing vs. building in-house
2. Contact REBNY about RLS vendor access
3. Reach out to StreetEasy for official partnership

---

## Sources

### GitHub Projects:
- [NYCDB - Database of NYC Housing Data](https://github.com/nycdb/nycdb)
- [NYC HPD Data Feeds](https://github.com/CityOfNewYork/HPD-Data-Feeds)
- [StreetEasy Scrape (Archived)](https://github.com/purcelba/streeteasy_scrape)
- [Zillow Real Estate Scraper](https://github.com/scrapehero/zillow_real_estate)

### Official Data Sources:
- [NYC Open Data](https://data.cityofnewyork.us/)
- [HPD Open Data](https://www.nyc.gov/site/hpd/about/open-data.page)
- [REBNY RLS](https://www.rebny.com/rls/)
- [StreetEasy XML Feed](https://streeteasy.com/home/feed_format)

### Commercial APIs:
- [Reasier NYC Rental API](https://www.reasier.com/api-access)
- [RentCast Property Data API](https://www.rentcast.io/api)

### Tutorials:
- [Scrape Zillow with Python (Scrapfly)](https://scrapfly.io/blog/posts/how-to-scrape-zillow)
- [Zillow Scraping Guide (ScrapingBee)](https://www.scrapingbee.com/blog/how-to-web-scrape-zillows-real-estate-data-at-scale-with-this-easy-zillow-scraper-in-python/)
- [StreetEasy Analysis (NYC Data Science)](https://nycdatascience.com/blog/student-works/scrape-streeteasy-com-to-analyze-housing-price-in-nyc/)
