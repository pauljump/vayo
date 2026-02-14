#!/usr/bin/env python3
"""
Download HPD Litigation dataset
Court cases mention specific apartment numbers
"""

import requests
import sqlite3
import time
from datetime import datetime

DB_PATH = "/Users/pjump/Desktop/projects/vayo/stuy-scrape-csv/stuytown.db"
API_BASE = "https://data.cityofnewyork.us/resource/59kj-x8nc.json"

def download_hpd_litigation():
    """Download HPD litigation records"""
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS hpd_litigation (
            litigationid TEXT PRIMARY KEY,
            buildingid TEXT,
            bbl TEXT,
            casetype TEXT,
            caseopendate TEXT,
            casestatusdate TEXT,
            casestatus TEXT,
            raw_text TEXT
        )
    """)
    
    print("Downloading HPD Litigation records...")
    print(f"Started: {datetime.now()}")
    
    offset = 0
    limit = 50000
    total_inserted = 0
    
    while True:
        url = f"{API_BASE}?$limit={limit}&$offset={offset}"
        
        try:
            response = requests.get(url, timeout=300)
            response.raise_for_status()
            records = response.json()
            
            if not records:
                break
            
            for record in records:
                try:
                    cursor.execute("""
                        INSERT OR IGNORE INTO hpd_litigation 
                        (litigationid, buildingid, bbl, casetype, caseopendate, 
                         casestatusdate, casestatus, raw_text)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        record.get('litigationid'),
                        record.get('buildingid'),
                        record.get('bbl'),
                        record.get('casetype'),
                        record.get('caseopendate'),
                        record.get('casestatusdate'),
                        record.get('casestatus'),
                        str(record.get('casetype', ''))
                    ))
                except Exception as e:
                    print(f"Error inserting record: {e}")
                    continue
            
            conn.commit()
            total_inserted += len(records)
            print(f"Downloaded {total_inserted:,} litigation records so far...")
            
            offset += limit
            time.sleep(0.5)
            
        except Exception as e:
            print(f"Error at offset {offset}: {e}")
            time.sleep(5)
            continue
    
    conn.close()
    print(f"\nComplete! Downloaded {total_inserted:,} HPD litigation records")
    print(f"Finished: {datetime.now()}")

if __name__ == "__main__":
    download_hpd_litigation()
