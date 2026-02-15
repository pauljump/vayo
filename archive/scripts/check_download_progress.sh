#!/bin/bash
# Check Phase 2 download progress

echo "=================================================="
echo "PHASE 2 DOWNLOAD PROGRESS"
echo "=================================================="
echo ""

# Check if processes are running
echo "📡 Running processes:"
ps aux | grep -E "05_download_full_311|06_download_dob_certificates" | grep -v grep | awk '{print "  - "$11" (PID "$2")"}'
echo ""

# Check database counts
echo "📊 Database counts:"
echo "  - 311 Service Requests: $(sqlite3 stuy-scrape-csv/stuytown.db 'SELECT COUNT(*) FROM service_requests_311')"
echo "  - DOB Certificates: $(sqlite3 stuy-scrape-csv/stuytown.db 'SELECT COUNT(*) FROM certificates_of_occupancy_new')"
echo "  - Canonical Units: $(sqlite3 stuy-scrape-csv/stuytown.db 'SELECT COUNT(*) FROM canonical_units')"
echo ""

# Check log files
echo "📝 Recent log output:"
echo ""
echo "=== 311 Download (last 10 lines) ==="
tail -10 full_311_download.log 2>/dev/null || echo "  (no output yet - buffered)"
echo ""
echo "=== DOB Certificates (last 10 lines) ==="
tail -10 dob_certificates_download.log 2>/dev/null || echo "  (no output yet)"
echo ""

echo "=================================================="
echo "Run this script again to check updated progress"
echo "=================================================="
