# Vayo Database - Data Inventory

**Generated:** 2026-01-17
**Database:** `/Users/pjump/Desktop/projects/vayo/stuy-scrape-csv/stuytown.db`
**Size:** 30GB

---

## ✅ AVAILABLE DATA (Good Quality)

### Buildings (571,476 records)
- **BIN** (Building Identification Number) ✅
- **BBL** (Borough-Block-Lot) ✅
- **Address** ✅
- **Borough** ✅
- **Building Class** ✅
- **Number of Units** ✅
- **Year Built** ✅
- **Source:** HPD Registered Buildings

**Sample:**
```
BIN: 1018055
Address: 144 EAST 24 STREET
Borough: MANHATTAN
Year Built: (varies)
Units: (varies)
```

---

### ECB Violations (1,786,096 records)
- **BIN** ✅
- **Violation Number** ✅
- **Status** (ACTIVE/RESOLVE) ✅
- **Severity** (CLASS-1, CLASS-2) ✅
- **Violation Type** (Boilers, Elevators, Construction, etc.) ✅
- **Description** ✅
- **Issue Date** ✅
- **Hearing Date** ✅
- **Penalty Amount** ✅

**Sample for BIN 1018055:**
- 23 ECB violations
- Types: Boilers, Elevators, Construction
- Example: "SAIL SWITCH IS DEFECTIVE-GAS METER AND CONDENSATE PUMP ROOM DOORS..."

**Use Cases:**
- Building health scoring ✅
- Maintenance pattern analysis ✅
- Landlord neglect detection ✅

---

### DOB Complaints (3,018,958 records)
- **BIN** ✅
- **Complaint Number** ✅
- **Complaint Category** ✅
- **Status** (CLOSED/OPEN) ✅
- **Status Date** ✅
- **Date Entered** ✅
- **Apartment** ✅

**Sample for BIN 1018055:**
- 32 DOB complaints
- Categories: BOILR, MAN., ELEVR
- All shown as CLOSED

**Use Cases:**
- Building maintenance tracking ✅
- Complaint volume analysis ✅

---

### HPD Complaints (26,165,975 records)
- **BIN** ✅
- **Unit Number** ✅
- **Block/Lot** ✅
- **Type** (EMERGENCY, IMMEDIATE EMERGENCY) ⚠️ (populated for some)

**⚠️ DATA QUALITY ISSUE:**
Most descriptive fields are NULL:
- major_category: NULL
- minor_category: NULL
- status: NULL
- status_description: NULL
- received_date: NULL

**Sample for BIN 1018055:**
- 243 HPD complaints
- Mostly show "EMERGENCY" or "IMMEDIATE EMERGENCY" type
- Unit-level data available (e.g., "10A", "1A")

**Current Utility:** Limited - can count complaint volume but not categorize by type

---

### ACRIS Real Property (7,250,000 records)
- **BIN** ✅
- **BBL** ✅
- **Document Type** ✅
- **Document Amount** (sale price) ✅
- **Document Date** ✅
- **Executed Date** ✅

**Use Cases:**
- Ownership change detection ✅
- Sale price analysis ✅
- Flip pattern detection ✅
- Deregulation trigger detection ✅

---

### HPD Contacts (723,578 records)
- **BIN** ✅
- **Corporation Name** (owner) ✅
- **Type** (HeadOfficer, etc.) ✅
- **Business Address** ✅

**Use Cases:**
- Landlord identification ✅
- Portfolio analysis (all buildings by same owner) ✅

---

### Evictions (120,618 records)
- **BIN** ✅
- **Executed Date** ✅

**Use Cases:**
- Landlord aggression scoring ✅
- Eviction pattern analysis ✅

---

## ❌ MISSING/EMPTY DATA

### Violations Table (0 records)
- Empty table
- **Workaround:** Use ECB violations + DOB complaints instead

### Rent Stabilization (0 records)
- Empty table
- **Impact:** Cannot determine rent stabilization history
- **Workaround:** Query NYC Open Data API in real-time (as existing intelligence.js does)
- **Alternative:** Need to seed this table from NY State data

### 311 Service Requests (10 records only!)
- Nearly empty table
- **Impact:** Cannot analyze neighborhood quality, heat complaints, rat complaints
- **Workaround:** Query NYC Open Data API in real-time OR re-seed this table

---

## 🔧 DATA WE CAN BUILD THE RENTER REPORT WITH

### ✅ YES - Available Now

1. **Building Health Score**
   - Source: ECB violations (1.7M records) + DOB complaints (3M records)
   - Can calculate: Total violations, open vs closed, severity breakdown
   - Can detect: Neglect patterns, recurring issues

2. **Landlord Analysis**
   - Source: HPD contacts (723K) + Evictions (120K) + ACRIS (7.25M)
   - Can identify: Owner name, portfolio size, eviction rate, flip patterns
   - Can detect: Problem landlords, recent sales (deregulation trigger)

3. **Ownership History**
   - Source: ACRIS (7.25M transactions)
   - Can show: Sale dates, prices, ownership changes

4. **Basic Complaint Volume**
   - Source: HPD complaints (26M) - even without descriptions
   - Can count: Total complaints per building, complaints per unit

### ⚠️ MAYBE - Need to Seed/Fix

5. **Rent Stabilization Risk**
   - **Current:** Empty table
   - **Fix:** Run seed script for NY State rent stabilization data
   - **Alternative:** Use NYC Open Data API (already in intelligence.js)

6. **Neighborhood Quality**
   - **Current:** Only 10 service requests in 311 table
   - **Fix:** Re-seed 311 data (should have millions of records)
   - **Alternative:** Use CityCel data if available

### ❌ NO - Not in Database

7. **Heat Complaint Patterns** (harassment detection)
   - Need: 311 data with complaint types
   - Status: 311 table nearly empty

8. **Detailed Violation Descriptions** for HPD
   - Need: HPD complaints table with major/minor category populated
   - Status: Those fields are NULL for all 26M records

---

## 📊 REALISTIC RENTER REPORT V1 SCOPE

### What We CAN Build Today:

**Page 1: Building Health**
- ✅ Violation count (ECB + DOB)
- ✅ Severity breakdown (CLASS-1 vs CLASS-2)
- ✅ Active vs Resolved status
- ✅ Comparison to area average (calculate median for neighborhood)
- ✅ Grade: A-F based on violation density

**Page 2: Landlord Intelligence**
- ✅ Owner name (HPD contacts)
- ✅ Portfolio size (count buildings with same owner)
- ✅ Eviction count (marshal evictions table)
- ✅ Recent sales (ACRIS - deregulation trigger)

**Page 3: Ownership History**
- ✅ Sale timeline with prices
- ✅ Flip pattern detection (sales frequency)

**Page 4: Complaint Volume**
- ✅ Total HPD complaints
- ✅ Complaint rate per unit
- ✅ Comparison to area average

### What We CANNOT Build (Yet):

**Page X: Rent Stabilization Analysis**
- ❌ Rent stab history (2007 vs 2017) - table empty
- **Workaround:** Use NYC Open Data API (real-time query)

**Page X: Tenant Experience Details**
- ❌ Heat complaint patterns - need 311 data
- ❌ Complaint categorization - HPD complaints missing descriptions
- **Workaround:** Generic "complaint volume" metric only

**Page X: Neighborhood Civic Score**
- ❌ Need 311 service request data
- **Workaround:** Skip this section in V1

---

## 🎯 NEXT STEPS

### Option 1: Build V1 with Available Data
- Use: ECB violations, DOB complaints, HPD contacts, evictions, ACRIS
- Skip: Rent stab history, detailed complaint types, 311 civic scoring
- Timeline: Can start building immediately
- Result: 70% complete renter report

### Option 2: Seed Missing Data First
1. **Rent Stabilization** (Priority: HIGH)
   ```bash
   node seed-rent-stabilization.js
   ```
   Source: https://data.ny.gov/resource/uc5g-f2zf.json

2. **311 Service Requests** (Priority: MEDIUM)
   ```bash
   node seed-311-requests.js
   ```
   Source: https://data.cityofnewyork.us/resource/fhrw-4uyv.json
   Expected: Millions of records (currently only 10)

3. **HPD Complaint Details** (Priority: LOW)
   - Check if seed script is incomplete
   - May need to re-run with full field selection

---

## 💡 RECOMMENDATION

**Build V1 NOW with available data**, then enhance:

1. **Week 1:** Build renter report with:
   - Building health score (ECB + DOB)
   - Landlord analysis (contacts + evictions)
   - Ownership history (ACRIS)
   - Complaint volume (HPD count)

2. **Week 2:** Add real-time API calls for:
   - Rent stabilization (NYC Open Data)
   - Fresh 311 data (NYC Open Data)

3. **Week 3:** Seed missing data for speed:
   - Run rent stab seed script
   - Re-run 311 seed script (should be way more than 10 records!)

This gets you to revenue FAST while improving data quality in parallel.
