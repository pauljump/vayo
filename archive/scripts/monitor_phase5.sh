#!/bin/bash

# Monitor Phase 5 progress

DB="/Users/pjump/Desktop/projects/vayo/stuy-scrape-csv/stuytown.db"

echo "=== PHASE 5 PROGRESS MONITOR ==="
echo ""
echo "Data downloaded so far:"
echo ""

# Check each table
for table in dob_complaints hpd_litigation nyc_311_complete; do
    count=$(sqlite3 "$DB" "SELECT COUNT(*) FROM $table" 2>/dev/null || echo "0")
    echo "$table: $(printf '%,d' $count) records"
done

echo ""
echo "Running processes:"
ps aux | grep -E "python.*download|run_phase5" | grep -v grep | awk '{print $2, $11, $12, $13}'

echo ""
echo "Phase 5 log (last 5 lines):"
tail -5 /tmp/phase5_complete.log 2>/dev/null || echo "No log yet"

echo ""
echo "---"
echo "Run: watch -n 5 ./monitor_phase5.sh"
echo "To monitor in real-time"
