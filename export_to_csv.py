# Run this locally to export your SQLite data to a CSV for the cloud
import sqlite3
import pandas as pd
from pathlib import Path

db_path = Path("data/smart_money.db")
if not db_path.exists():
    print("ERROR: data/smart_money.db not found")
    exit()

conn = sqlite3.connect(str(db_path))
df = pd.read_sql(
    "SELECT * FROM stocks WHERE scrape_status='done' ORDER BY smart_money_score DESC",
    conn
)
conn.close()

if df.empty:
    print("No scored stocks found in database")
else:
    df.to_csv("data/cloud_data.csv", index=False)
    print(f"Exported {len(df)} stocks to data/cloud_data.csv")
    print(f"Columns: {list(df.columns)}")
