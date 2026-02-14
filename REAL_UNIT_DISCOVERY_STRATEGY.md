# Real Unit Discovery Strategy - No Placeholders

**Goal:** Discover the missing ~1.89M real units (with actual unit numbers)

**Current:** 1.85M units | **Target:** 3.74M units | **Gap:** 1.89M units

---

## 🎯 The Core Insight

The missing units fall into three categories:
1. **"Quiet" rentals** (~1M units) - Good buildings, no complaints, never listed
2. **Never-traded condos/co-ops** (~400K units) - Inherited, long-term owners
3. **Recently built** (~400K units) - New construction, not yet in ACRIS

**Where they ARE visible:**
- Government paperwork (permits, certificates, registrations)
- Historical listings (archives)
- Other complaint/violation systems we haven't mined
- Property records beyond ACRIS

---

## 🔥 TIER 1: Mine What We Already Have (Highest ROI)

### 1. Download More NYC Open Data

**We have the infrastructure, just need more datasets:**

**A. 311 Service Requests (Full Dataset)**
- Current: 150K records
- Expected: 30M+ records
- **Unit Coverage:** Apartment numbers in incident addresses
- **Action:** Re-download full 311 dataset from NYC Open Data
```bash
# Download ALL 311 requests, not just recent
curl "https://data.cityofnewyork.us/resource/erm2-nwe9.json?\$limit=1000000&\$offset=0"
```

**B. DOB Certificates of Occupancy**
- Current: 0 records (table exists, empty)
- Expected: Hundreds of thousands
- **Unit Coverage:** Official unit enumeration when buildings are built/converted
- **Download from:** https://data.cityofnewyork.us/Housing-Development/DOB-Certificate-of-Occupancy/bs8b-p36w

**C. Eviction Filings (Housing Court)**
- Current: 0 records
- Expected: Millions of records
- **Unit Coverage:** Every eviction filing has apartment number
- **Download from:** NYC Open Data

**D. OATH Hearings**
- Violations heard at OATH often have unit numbers
- NYC Open Data has this

**E. DOF Property Tax Bills**
- Some bills enumerate units
- Might be scrapable or available via Open Data

**Estimated Add: 300K-500K unique units**

---

### 2. Parse Existing Data We Haven't Fully Mined

**A. DOB Permits (1.6M records) - Extract Units from Descriptions**

We have 1.6M DOB permits. Many have unit numbers in the `job_description` field:
- "REPLACE WINDOWS IN APARTMENT 3A"
- "PLUMBING WORK UNIT 12B"
- "RENOVATION OF APT 5C"

**Action:** Parse all job descriptions with regex to extract unit numbers

**B. ECB Violations (1.7M records) - Mine Violation Descriptions**

Similar to DOB permits, violation descriptions often mention specific units:
- "ILLEGAL WORK IN APARTMENT 2B"
- "VIOLATION IN UNIT 4D"

**Action:** Parse violation_description field

**C. ACRIS Parties - Seller/Buyer Addresses**

The `acris_parties` table has 16M+ records with party addresses. Many sellers/buyers list their unit:
- "123 Main St Apt 5A, New York NY"

**Action:** Parse party addresses to extract unit numbers

**D. Service Requests 311 - Parse Incident Addresses**

The 150K 311 requests we have might include units in addresses:
- "500 WEST 140 STREET APT 3B"

**Action:** Parse incident_address field

**Estimated Add: 200K-400K unique units from text mining**

---

## 🔥 TIER 2: Historical Listing Archives (Massive Coverage)

### 3. Wayback Machine - Historical StreetEasy/Zillow Scraping

**The Strategy:**
- StreetEasy has existed since 2006
- Wayback Machine has thousands of snapshots
- Each snapshot captured different listings (units come and go)
- By scraping historical snapshots, we can discover units that were listed but aren't currently

**Example:**
```bash
# Get all StreetEasy snapshots
curl "http://web.archive.org/cdx/search/cdx?url=streeteasy.com/building/*&output=json"

# For each snapshot, extract listings
curl "http://web.archive.org/web/20200515/streeteasy.com/building/..."
```

**Estimated snapshots:**
- StreetEasy: ~10,000 snapshots over 15+ years
- Zillow NYC: ~5,000 snapshots
- RentHop, Nooklyn, etc.: ~2,000 snapshots each

**Each snapshot might have:**
- 50,000-100,000 active listings
- Many unique units across all snapshots

**Estimated Add: 500K-1M unique units**

**Advantages:**
- Completely legal (Wayback Machine is public)
- No rate limits (Wayback has its own access policies)
- Historical data = units that never appear in current listings

---

### 4. Common Crawl - Web Archive Dataset

**What it is:**
- Non-profit that crawls the entire web monthly
- Petabytes of archived HTML
- Free access via AWS S3

**NYC Real Estate Coverage:**
- Every major listing site gets crawled monthly
- Data going back to 2008
- Can query for specific domains and parse listings

**How to use:**
```python
# Query Common Crawl index for StreetEasy
# Extract all rental/sales listing pages
# Parse unit numbers, addresses, details
```

**Estimated Add: 300K-800K unique units**

---

## 🔥 TIER 3: Download Missing NYCDB Datasets

### 5. NYCDB Has More Datasets We Haven't Loaded

From NYCDB's dataset list, these contain unit-level data:

**A. OCA Housing Court Records**
- Dataset: `oca`
- Contains: Housing court cases with unit numbers
- Coverage: Massive (millions of cases)

**B. HPD Violations (vs Complaints)**
- Dataset: `hpd_violations`
- Different from complaints (we have 26M complaints)
- Violations also have apartment numbers

**C. DOB Certificate of Occupancy**
- Dataset: `dob_certificate_occupancy`
- Official unit counts when buildings get CO
- Sometimes lists individual units

**Action:**
```bash
/Library/Frameworks/Python.framework/Versions/3.12/bin/nycdb \
  --root-dir nycdb_data \
  --download oca

/Library/Frameworks/Python.framework/Versions/3.12/bin/nycdb \
  --root-dir nycdb_data \
  --download hpd_violations
```

Then parse and import to SQLite.

**Estimated Add: 200K-400K unique units**

---

## 🔥 TIER 4: Landlord Website Deep Scraping

### 6. Systematic Landlord Site Scraping

**Target the Top 100 NYC Landlords:**

**Tier 1 (Portfolio > 5,000 units each):**
- Related Companies
- Equity Residential
- AvalonBay
- Glenwood Management
- TF Cornerstone
- Durst Organization
- Rockrose
- Two Trees
- LeFrak
- Stellar Management

**Tier 2 (Portfolio 1,000-5,000 units):**
- Rose Associates
- Normandy Real Estate
- Douglas Elliman Property Management
- Brown Harris Stevens
- L&M Development Partners
- (50+ more)

**Strategy:**
1. Check each site's `/sitemap.xml` and `/robots.txt`
2. Find availability pages or building pages
3. Scrape current + historical availability (some sites have archives)
4. Many sites have JSON APIs powering their search (inspect Network tab)

**Example - Rockrose has clean data:**
```javascript
// Many landlord sites load units via JSON
fetch('https://rockrose.com/api/availabilities')
  .then(r => r.json())
  .then(units => console.log(units))
```

**Estimated Add: 150K-300K unique units (current market)**

---

## 🔥 TIER 5: Alternative Public Records

### 7. USPS Address Data

**HUD USPS Vacancy Data:**
- HUD publishes quarterly USPS vacancy tracking
- Address-level (possibly unit-level) granularity
- Tracks residential delivery points
- **Free, public dataset**

**Download:**
```bash
# HUD USPS Vacancy Data
curl "https://www.huduser.gov/portal/datasets/usps.html"
```

**Might have:** All deliverable addresses including apartment numbers

**Estimated Add: Unknown, could be huge (100K-500K?)**

---

### 8. Voter Registration Data (Public Records)

**NYC Board of Elections:**
- Voter registration records are public
- Include address with apartment number
- Can be purchased or FOIL'd (user said no contacting, but data might be already available)

**Caveat:** Not everyone is registered, but high coverage in NYC

**Estimated Add: 400K-800K units (if accessible)**

---

### 9. Google Street View + OCR (Creative!)

**The Hack:**
- Many buildings have directory buzzers visible in Street View
- Lists all unit numbers: "1A, 1B, 2A, 2B, 3A..."
- Can OCR these from Street View images

**Process:**
1. Get Street View images for all buildings
2. Run OCR on doorway/buzzer area
3. Extract unit directory

**Legal:** Google Street View is public, OCR is fair use

**Estimated Add: 50K-200K units (labor intensive, good for high-value buildings)**

---

## 📊 Estimated Total Impact

| Strategy | Effort | Legality | Units | Confidence |
|----------|--------|----------|-------|------------|
| **Download more NYC Open Data** | Low | ✅ Clear | 300K-500K | High |
| **Parse existing data (DOB, ECB, ACRIS)** | Medium | ✅ Clear | 200K-400K | High |
| **Wayback Machine historical listings** | Medium | ✅ Clear | 500K-1M | Medium-High |
| **Common Crawl** | Medium-High | ✅ Clear | 300K-800K | Medium |
| **NYCDB additional datasets** | Low | ✅ Clear | 200K-400K | High |
| **Landlord site scraping (compliant)** | Medium | ✅ Clear | 150K-300K | Medium |
| **USPS HUD data** | Low | ✅ Clear | 100K-500K | Unknown |
| **Voter registration** | Unknown | ⚠️ Grey | 400K-800K | Low (access?) |
| **Google Street View OCR** | High | ✅ Clear | 50K-200K | Low-Medium |

**Conservative Total: 1.8M - 4.5M additional unique units**

(Overlaps will occur, but even with 50% deduplication, we'd add 900K-2M+ real units)

---

## 🎯 Recommended Implementation Order

### Phase 1 (This Week): Low-Hanging Fruit

**1. Download missing NYC Open Data** (2-3 days)
- Full 311 Service Requests
- DOB Certificates of Occupancy
- Eviction Filings
- OATH Hearings

**Expected: +300K-500K units**

**2. Parse existing data** (1-2 days)
- DOB permit descriptions (1.6M records)
- ECB violation descriptions (1.7M records)
- 311 incident addresses (150K records)

**Expected: +100K-200K units**

**Phase 1 Total: 400K-700K new units in one week**

---

### Phase 2 (Next Week): Historical Archives

**3. Wayback Machine scraper** (3-5 days)
- Build scraper to extract historical StreetEasy snapshots
- Parse listing pages for unit numbers
- Store in database with snapshot dates

**Expected: +500K-1M units**

**4. NYCDB datasets** (1-2 days)
- Download OCA housing court
- Download HPD violations
- Import to our database

**Expected: +200K-400K units**

**Phase 2 Total: 700K-1.4M new units**

---

### Phase 3 (Weeks 3-4): Systematic Scraping

**5. Top 100 landlord sites** (ongoing)
- Build scrapers for each major landlord
- Check for APIs/JSON endpoints
- Respect robots.txt and rate limits

**Expected: +150K-300K units**

**6. Common Crawl** (if needed)
- Query Common Crawl index for real estate domains
- Parse archived listings
- Deduplicate against existing

**Expected: +300K-800K units**

---

## 🛠️ Technical Implementation

### Script 1: Download More NYC Open Data

```python
# Download full 311 dataset
NYC_OPEN_DATA_APIS = {
    "311": "https://data.cityofnewyork.us/resource/erm2-nwe9.json",
    "dob_co": "https://data.cityofnewyork.us/resource/bs8b-p36w.json",
    "evictions": "https://data.cityofnewyork.us/resource/vizv-yxya.json",
    "oath": "https://data.cityofnewyork.us/resource/hbdz-8v3f.json"
}

# Batch download with pagination
for name, url in NYC_OPEN_DATA_APIS.items():
    download_paginated(url, limit=50000, max_records=10_000_000)
```

### Script 2: Text Mining for Units

```python
import re

UNIT_PATTERNS = [
    r'(?:APT|APARTMENT|UNIT|#)\s*([0-9]+[A-Z]?)',
    r'(?:APT|APARTMENT|UNIT)\s+([A-Z]-?[0-9]+)',
    r'\b([0-9]{1,2}[A-Z])\b',  # Like "3A", "12B"
]

def extract_unit_from_text(text):
    for pattern in UNIT_PATTERNS:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            return match.group(1)
    return None

# Apply to DOB permits, ECB violations, 311, ACRIS parties
```

### Script 3: Wayback Machine Scraper

```python
import requests
from bs4 import BeautifulSoup

def get_snapshots(url):
    cdx_url = f"http://web.archive.org/cdx/search/cdx?url={url}&output=json"
    return requests.get(cdx_url).json()

def scrape_snapshot(timestamp, url):
    archive_url = f"http://web.archive.org/web/{timestamp}/{url}"
    html = requests.get(archive_url).text
    # Parse listings from HTML
    return parse_listings(html)

# Example: Get all StreetEasy building pages from history
snapshots = get_snapshots("streeteasy.com/building/*")
for snapshot in snapshots:
    units = scrape_snapshot(snapshot[1], snapshot[2])
    save_units(units)
```

---

## 📈 Success Metrics

**Week 1:**
- Download 4-5 new NYC Open Data datasets
- Parse 3M+ existing records for unit numbers
- **Target: +500K units**

**Week 2:**
- Scrape 100+ Wayback snapshots
- Load NYCDB housing court data
- **Target: +700K units**

**Week 3:**
- Scrape top 20 landlord sites
- Begin Common Crawl queries
- **Target: +300K units**

**Month 1 Total: +1.5M real units discovered**

---

## 🎯 The Bottom Line

You can get to **3M+ real units** (80%+ coverage) within a month by:

1. ✅ Mining existing data harder (text parsing)
2. ✅ Downloading more NYC Open Data
3. ✅ Historical archive scraping (Wayback, Common Crawl)
4. ✅ Systematic landlord scraping (compliant)

**All without placeholders. All with real unit numbers. All legally.**

**Want me to start with Phase 1 (NYC Open Data downloads + text mining)?**
