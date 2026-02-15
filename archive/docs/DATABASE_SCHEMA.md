# Vayo Database Schema

**Database:** `stuytown.db` (30GB)
**Tables:** 36+
**Records:** 50M+

## Core Tables

### buildings
NYC building registry (571,476 records)

| Column | Type | Description |
|--------|------|-------------|
| bin | TEXT PRIMARY KEY | Building Identification Number (unique per building) |
| bbl | TEXT | Borough-Block-Lot (tax lot, may include multiple buildings) |
| address | TEXT | Street address |
| borough | TEXT | MANHATTAN, BROOKLYN, QUEENS, BRONX, STATEN ISLAND |
| year_built | INTEGER | Year of construction |
| num_units | INTEGER | Number of residential units |
| building_class | TEXT | NYC building classification |
| lat, lon | REAL | Geographic coordinates |

**Indexes:**
- PRIMARY KEY (bin)
- INDEX on borough, year_built, num_units

---

### complaints
HPD complaints (26,165,975 records)

| Column | Type | Description |
|--------|------|-------------|
| complaint_id | INTEGER PRIMARY KEY | Unique complaint ID |
| bin | TEXT | Foreign key to buildings.bin |
| unit_number | TEXT | Apartment number (if applicable) |
| complaint_type | TEXT | Category (HEAT, WATER, PEST, etc.) |
| status | TEXT | OPEN, CLOSED |
| date_received | DATE | When complaint was filed |

**Indexes:**
- PRIMARY KEY (complaint_id)
- INDEX on bin (for fast building lookups)

---

### craigslist_listings
Current rental listings (889+ records, growing)

**Note:** This table will be renamed to `listings` to support multiple sources.

| Column | Type | Description |
|--------|------|-------------|
| id | INTEGER PRIMARY KEY | Auto-increment ID |
| post_id | TEXT UNIQUE | Source-prefixed ID (e.g., "reddit_abc") |
| url | TEXT | Listing URL |
| title | TEXT | Listing title/description |
| price | INTEGER | Rent or sale price |
| address | TEXT | Street address |
| neighborhood | TEXT | NYC neighborhood |
| bedrooms | INTEGER | Number of bedrooms |
| bathrooms | REAL | Number of bathrooms (1.5 = 1 full + 1 half) |
| square_feet | INTEGER | Unit size in sqft |
| description | TEXT | Full listing description |
| posted_at | DATETIME | When listing was posted |
| scraped_at | DATETIME | When we scraped it |
| source | TEXT | Original subreddit/site |
| bin | TEXT | Building ID (foreign key to buildings.bin) |

**Migration planned:** Add `data_source` column ('craigslist', 'reddit', 'realtor', 'streeteasy')

**Indexes:**
- UNIQUE (post_id)
- INDEX on bin, address

---

### reddit_testimonials
Building testimonials from Reddit (NEW - Jan 2026)

| Column | Type | Description |
|--------|------|-------------|
| id | INTEGER PRIMARY KEY | Auto-increment ID |
| building_name | TEXT | e.g., "The Dakota" |
| address | TEXT | Building address |
| bin | TEXT | Foreign key to buildings.bin |
| post_id | TEXT UNIQUE | Reddit post ID |
| subreddit | TEXT | r/NYCApartments, r/AskNYC, etc. |
| post_title | TEXT | Post title |
| post_body | TEXT | Post content |
| posted_date | DATE | When post was created |
| scraped_date | DATE | When we found it |
| source_url | TEXT | Full Reddit URL |
| sentiment | TEXT | 'positive', 'negative', 'neutral' |
| tags | TEXT | JSON array: ['rough-quarters', 'testimonial'] |

**Indexes:**
- UNIQUE (post_id)
- INDEX on bin, building_name, sentiment

**Purpose:** Support Rough Quarters' testimonial discovery strategy

---

### current_rents
Rent history by unit

| Column | Type | Description |
|--------|------|-------------|
| id | INTEGER PRIMARY KEY | Auto-increment ID |
| building_id | TEXT | Building identifier |
| unit_number | TEXT | Apartment number |
| bin | TEXT | Foreign key to buildings.bin |
| current_rent | INTEGER | Current monthly rent |
| bedrooms | INTEGER | Unit size |
| last_updated | DATE | When rent was last updated |
| is_stabilized | INTEGER | 1 if rent-stabilized |

**Indexes:**
- UNIQUE (building_id, unit_number)
- INDEX on bin

---

### acris_real_property
NYC property transactions (16M+ records)

| Column | Type | Description |
|--------|------|-------------|
| document_id | TEXT PRIMARY KEY | ACRIS document ID |
| recorded_datetime | DATETIME | When transaction was recorded |
| document_amt | INTEGER | Transaction amount |
| doc_type | TEXT | DEED, MORTGAGE, etc. |
| crfn | TEXT | City Register File Number |
| (many more columns) | | |

---

## Table Relationships

```
buildings (571K)
    ↓ bin
    ├─→ complaints (26M) - HPD complaints by building
    ├─→ craigslist_listings (889) - Current rentals
    ├─→ reddit_testimonials (NEW) - Social proof
    ├─→ current_rents - Rent history
    └─→ acris_real_property (16M) - Property transactions
```

## Usage Examples

**Get building with all related data:**
```sql
-- Building info
SELECT * FROM buildings WHERE bin = '1018055';

-- Complaints
SELECT COUNT(*) FROM complaints WHERE bin = '1018055';

-- Current listings
SELECT * FROM craigslist_listings WHERE bin = '1018055';

-- Testimonials
SELECT * FROM reddit_testimonials WHERE bin = '1018055';
```

**Find buildings with zero complaints:**
```sql
SELECT b.*
FROM buildings b
LEFT JOIN complaints c ON b.bin = c.bin
WHERE b.borough = 'MANHATTAN'
  AND b.num_units >= 20
GROUP BY b.bin
HAVING COUNT(c.complaint_id) = 0;
```

**Calculate building health score:**
```sql
SELECT
    b.bin,
    b.address,
    b.num_units,
    COUNT(c.complaint_id) as complaint_count,
    CAST(COUNT(c.complaint_id) AS FLOAT) / b.num_units as complaints_per_unit,
    CASE
        WHEN CAST(COUNT(c.complaint_id) AS FLOAT) / b.num_units < 0.5 THEN 100
        WHEN CAST(COUNT(c.complaint_id) AS FLOAT) / b.num_units < 1 THEN 95
        WHEN CAST(COUNT(c.complaint_id) AS FLOAT) / b.num_units < 2 THEN 80
        WHEN CAST(COUNT(c.complaint_id) AS FLOAT) / b.num_units < 5 THEN 60
        WHEN CAST(COUNT(c.complaint_id) AS FLOAT) / b.num_units < 10 THEN 40
        ELSE 20
    END as health_score
FROM buildings b
LEFT JOIN complaints c ON b.bin = c.bin
WHERE b.borough = 'MANHATTAN'
GROUP BY b.bin
ORDER BY health_score DESC;
```

---

## Full Table List

Run this command to see all tables:
```bash
sqlite3 stuytown.db ".tables"
```

Expected output:
```
acris_real_property         current_rents
buildings                   dhcr_rent_stabilized
complaints                  craigslist_listings
reddit_testimonials         violations
evictions                   permits
... (30+ more tables)
```

---

**Last updated:** January 18, 2026
**Database size:** 30GB
**Total records:** 50M+
