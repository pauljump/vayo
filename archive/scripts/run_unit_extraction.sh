#!/bin/bash

# ============================================================================
# VAYO UNIT EXTRACTION PIPELINE
# ============================================================================
# Extracts all units from existing database sources
# Run from: /Users/pjump/Desktop/projects/vayo
# ============================================================================

DB_PATH="/Users/pjump/Desktop/projects/vayo/stuy-scrape-csv/stuytown.db"
SCRIPTS_DIR="/Users/pjump/Desktop/projects/vayo/scripts"

echo "════════════════════════════════════════════════════════════════"
echo "VAYO UNIT EXTRACTION PIPELINE"
echo "════════════════════════════════════════════════════════════════"
echo ""
echo "Database: $DB_PATH"
echo "Started: $(date)"
echo ""

# Check if database exists
if [ ! -f "$DB_PATH" ]; then
    echo "ERROR: Database not found at $DB_PATH"
    exit 1
fi

# ============================================================================
# STEP 1: Create canonical units table and extract from all sources
# ============================================================================

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "STEP 1: Creating canonical units table and staging tables..."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

sqlite3 "$DB_PATH" < "$SCRIPTS_DIR/01_create_canonical_units.sql"

if [ $? -ne 0 ]; then
    echo "ERROR: Step 1 failed"
    exit 1
fi

echo ""

# ============================================================================
# STEP 2: Merge all sources into canonical units
# ============================================================================

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "STEP 2: Merging units from all sources..."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

sqlite3 "$DB_PATH" < "$SCRIPTS_DIR/02_merge_units.sql"

if [ $? -ne 0 ]; then
    echo "ERROR: Step 2 failed"
    exit 1
fi

echo ""

# ============================================================================
# STEP 3: Generate placeholder units for buildings
# ============================================================================

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "STEP 3: Generating placeholder units for buildings..."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

sqlite3 "$DB_PATH" < "$SCRIPTS_DIR/03_generate_placeholder_units.sql"

if [ $? -ne 0 ]; then
    echo "ERROR: Step 3 failed"
    exit 1
fi

echo ""

# ============================================================================
# FINAL REPORT
# ============================================================================

echo "════════════════════════════════════════════════════════════════"
echo "EXTRACTION COMPLETE"
echo "════════════════════════════════════════════════════════════════"
echo ""
echo "Completed: $(date)"
echo ""
echo "Quick stats:"
sqlite3 "$DB_PATH" "SELECT 'Total canonical units: ' || COUNT(*) FROM canonical_units;"
sqlite3 "$DB_PATH" "SELECT 'Placeholder units: ' || COUNT(*) FROM placeholder_units;"
sqlite3 "$DB_PATH" "SELECT 'Combined total: ' || (SELECT COUNT(*) FROM canonical_units) + (SELECT COUNT(*) FROM placeholder_units);"
echo ""
echo "Next steps:"
echo "  1. Review output: sqlite3 $DB_PATH 'SELECT * FROM canonical_units LIMIT 100;'"
echo "  2. Check stats: sqlite3 $DB_PATH '.read scripts/unit_stats.sql'"
echo "  3. Export sample: sqlite3 $DB_PATH '.mode csv' '.output units_sample.csv' 'SELECT * FROM canonical_units LIMIT 10000;'"
echo ""
