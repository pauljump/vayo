#!/bin/bash
# Run wayback fetch in a loop — auto-discovers and queues new units between passes.
# Each pass fetches all pending URLs, then discovered units get queued for the next pass.
# Stops when there's nothing left to fetch.

cd "$(dirname "$0")/.."

PASS=1
while true; do
    echo ""
    echo "============================================"
    echo "  Pass $PASS — $(date)"
    echo "============================================"

    # Retry any errors from previous pass
    python3 scripts/streeteasy_wayback_history.py retry --max-attempts 5

    # Queue any discovered units not yet in queue
    python3 scripts/streeteasy_wayback_history.py discover

    # Check if there's anything pending
    PENDING=$(sqlite3 se_listings.db "SELECT COUNT(*) FROM wb_queue WHERE status='pending';")
    echo "Pending: $PENDING"

    if [ "$PENDING" -eq 0 ]; then
        echo "All done! No more pending URLs."
        python3 scripts/streeteasy_wayback_history.py status
        break
    fi

    # Fetch all pending
    python3 scripts/streeteasy_wayback_history.py fetch --concurrency 20 --rate 15

    PASS=$((PASS + 1))
done

echo ""
echo "Overnight run complete at $(date)"
