#!/usr/bin/env python3
"""
Download COMPLETE NYC 311 Service Requests dataset
Previous phase only sampled - this gets ALL records with apartment mentions
"""

import requests
import sqlite3
import time
from datetime import datetime

DB_PATH = "/Users/pjump/Desktop/projects/vayo/stuy-scrape-csv/stuytown.db"
API_BASE = "https://data.cityofnewyork.us/resource/erm2-nwe9.json"

def download_311_complete():
    """Download all 311 records that mention apartments"""
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Create table for 311 data if not exists
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS nyc_311_complete (
            unique_key TEXT PRIMARY KEY,
            created_date TEXT,
            complaint_type TEXT,
            descriptor TEXT,
            incident_address TEXT,
            bbl TEXT,
            borough TEXT,
            raw_text TEXT
        )
    """)
    
    print("Downloading NYC 311 records with apartment mentions...")
    print(f"Started: {datetime.now()}")
    
    # Query for records that likely mention apartments
    # Focus on housing-related complaint types
    housing_complaints = [
        'HEATING', 'PLUMBING', 'PAINT/PLASTER', 'DOOR/WINDOW',
        'WATER LEAK', 'ELECTRIC', 'ELEVATOR', 'GENERAL CONSTRUCTION',
        'HEAT/HOT WATER', 'UNSANITARY CONDITION'
    ]
    
    offset = 0
    limit = 50000
    total_inserted = 0
    
    while True:
        # Build query for housing complaints with BBL
        where_clause = " OR ".join([f"complaint_type='{ct}'" for ct in housing_complaints])
        
        url = f"{API_BASE}?$where=({where_clause}) AND bbl IS NOT NULL&$limit={limit}&$offset={offset}&$order=created_date DESC"
        
        try:
            response = requests.get(url, timeout=300)
            response.raise_for_status()
            records = response.json()
            
            if not records:
                break
            
            for record in records:
                try:
                    cursor.execute("""
                        INSERT OR IGNORE INTO nyc_311_complete 
                        (unique_key, created_date, complaint_type, descriptor, 
                         incident_address, bbl, borough, raw_text)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        record.get('unique_key'),
                        record.get('created_date'),
                        record.get('complaint_type'),
                        record.get('descriptor'),
                        record.get('incident_address'),
                        record.get('bbl'),
                        record.get('borough'),
                        # Combine all text fields for mining
                        ' '.join([
                            str(record.get('complaint_type', '')),
                            str(record.get('descriptor', '')),
                            str(record.get('incident_address', '')),
                            str(record.get('location_type', '')),
                            str(record.get('resolution_description', ''))
                        ])
                    ))
                except Exception as e:
                    print(f"Error inserting record: {e}")
                    continue
            
            conn.commit()
            total_inserted += len(records)
            print(f"Downloaded {total_inserted:,} records so far...")
            
            offset += limit
            time.sleep(0.5)  # Rate limiting
            
        except Exception as e:
            print(f"Error at offset {offset}: {e}")
            time.sleep(5)
            continue
    
    conn.close()
    print(f"\nComplete! Downloaded {total_inserted:,} 311 records")
    print(f"Finished: {datetime.now()}")

if __name__ == "__main__":
    download_311_complete()
