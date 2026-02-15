#!/bin/bash

# Check Phase 5 progress and report status

DB="/Users/pjump/Desktop/projects/vayo/stuy-scrape-csv/stuytown.db"

echo "╔════════════════════════════════════════════════════════════╗"
echo "║           PHASE 5: FREE DATA COLLECTION STATUS            ║"
echo "╚════════════════════════════════════════════════════════════╝"
echo ""
echo "Time: $(date '+%Y-%m-%d %H:%M:%S')"
echo ""

# Check each dataset
echo "📊 DATA DOWNLOADED:"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

dob_count=$(sqlite3 "$DB" "SELECT COUNT(*) FROM dob_complaints" 2>/dev/null || echo "0")
hpd_count=$(sqlite3 "$DB" "SELECT COUNT(*) FROM hpd_litigation" 2>/dev/null || echo "0")
call311_count=$(sqlite3 "$DB" "SELECT COUNT(*) FROM nyc_311_complete" 2>/dev/null || echo "0")

printf "  ✅ DOB Complaints:    %'10d records\n" $dob_count
printf "  ✅ HPD Litigation:    %'10d records\n" $hpd_count
printf "  ⏳ 311 Complete:      %'10d records (downloading...)\n" $call311_count

echo ""
echo "📈 DOWNLOAD RATE:"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# Estimate 311 progress (housing complaints typically ~3-5M records)
if [ "$call311_count" -gt 0 ]; then
    pct_complete=$((call311_count * 100 / 4000000))
    echo "  311 Progress: ~${pct_complete}% of estimated 4M housing complaints"

    # Calculate ETA based on rate
    records_per_min=50000
    remaining=$((4000000 - call311_count))
    eta_min=$((remaining / records_per_min))
    echo "  Estimated completion: ${eta_min} minutes remaining"
fi

echo ""
echo "🔄 PROCESS STATUS:"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

if ps -p 95519 > /dev/null 2>&1; then
    current_step=$(tail -20 /tmp/phase5_complete.log | grep "^===" | tail -1 | sed 's/===//g' | xargs)
    echo "  Status: RUNNING"
    echo "  Current: $current_step"
else
    echo "  Status: COMPLETED ✓"
fi

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
