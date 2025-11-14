import sqlite3
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()

db_path = os.getenv('DB_PATH')

def parquet_maker():
    conn = sqlite3.connect(db_path)
    query = "SELECT * FROM raw_data"
    data = pd.read_sql_query(query, conn)
    data.to_parquet('data_parquet/databricks_sync_cpu.parquet', engine='pyarrow', index=False)
    conn.close()

if __name__ == "__main__":
    parquet_maker()