#!/usr/bin/env python3
"""
Download DOB Complaints dataset
These have apartment numbers in complaint descriptions
"""

import requests
import sqlite3
import time
from datetime import datetime

DB_PATH = "/Users/pjump/Desktop/projects/vayo/stuy-scrape-csv/stuytown.db"
API_BASE = "https://data.cityofnewyork.us/resource/eabe-havv.json"

def download_dob_complaints():
    """Download DOB complaint records"""
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Drop and recreate to ensure schema is correct
    cursor.execute("DROP TABLE IF EXISTS dob_complaints")
    cursor.execute("""
        CREATE TABLE dob_complaints (
            complaint_number TEXT PRIMARY KEY,
            bin TEXT,
            bbl TEXT,
            borough TEXT,
            house_number TEXT,
            street TEXT,
            complaint_category TEXT,
            unit TEXT,
            disposition_code TEXT,
            disposition_date TEXT,
            raw_description TEXT
        )
    """)
    
    print("Downloading DOB Complaints...")
    print(f"Started: {datetime.now()}")
    
    offset = 0
    limit = 50000
    total_inserted = 0
    
    while True:
        url = f"{API_BASE}?$limit={limit}&$offset={offset}&$order=date_entered DESC"
        
        try:
            response = requests.get(url, timeout=300)
            response.raise_for_status()
            records = response.json()
            
            if not records:
                break
            
            for record in records:
                try:
                    cursor.execute("""
                        INSERT OR IGNORE INTO dob_complaints 
                        (complaint_number, bin, bbl, borough, house_number, street,
                         complaint_category, unit, disposition_code, disposition_date, raw_description)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        record.get('complaint_number'),
                        record.get('bin'),
                        record.get('bbl'),
                        record.get('boro'),
                        record.get('house_number'),
                        record.get('street'),
                        record.get('complaint_category'),
                        record.get('unit'),
                        record.get('disposition_code'),
                        record.get('disposition_date'),
                        ' '.join([
                            str(record.get('complaint_category', '')),
                            str(record.get('unit', '')),
                            str(record.get('house_number', '')),
                            str(record.get('street', ''))
                        ])
                    ))
                except Exception as e:
                    print(f"Error inserting record: {e}")
                    continue
            
            conn.commit()
            total_inserted += len(records)
            print(f"Downloaded {total_inserted:,} complaints so far...")
            
            offset += limit
            time.sleep(0.5)
            
        except Exception as e:
            print(f"Error at offset {offset}: {e}")
            time.sleep(5)
            continue
    
    conn.close()
    print(f"\nComplete! Downloaded {total_inserted:,} DOB complaints")
    print(f"Finished: {datetime.now()}")

if __name__ == "__main__":
    download_dob_complaints()
