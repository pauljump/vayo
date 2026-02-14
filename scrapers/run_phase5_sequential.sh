#!/bin/bash

# Phase 5: Creative FREE data collection
# Running sequentially to avoid database locks

echo "============================================================"
echo "PHASE 5: CREATIVE FREE DATA COLLECTION"
echo "Started: $(date)"
echo "============================================================"
echo ""

cd /Users/pjump/Desktop/projects/vayo

# 1. DOB Complaints (most likely to have explicit unit numbers)
echo "=== Step 1: DOB Complaints ==="
python3 scrapers/15_download_dob_complaints.py
echo ""

# 2. HPD Litigation
echo "=== Step 2: HPD Litigation ==="
python3 scrapers/16_download_hpd_litigation.py
echo ""

# 3. 311 Complete (large dataset, run last)
echo "=== Step 3: 311 Complete Dataset ==="
python3 scrapers/14_download_311_complete.py
echo ""

# 4. Additional datasets
echo "=== Step 4: Additional NYC Open Data ==="
python3 scrapers/18_download_additional_datasets.py
echo ""

# 5. Advanced text mining on all new data
echo "=== Step 5: Advanced Text Mining ==="
python3 scrapers/17_advanced_text_mining.py
echo ""

echo "============================================================"
echo "PHASE 5 COMPLETE"
echo "Finished: $(date)"
echo "============================================================"
