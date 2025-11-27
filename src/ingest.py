import pandas as pd
from sqlalchemy import create_engine
import time
import os

def ingest_data(csv_path, database_url, raw_table_name):

    print("--- 1. INGESTION START ---")
    
    engine = create_engine(database_url)

    try:
        print(f"Reading CSV file from: {csv_path}")
        df_raw = pd.read_csv(csv_path, parse_dates=['Date']) 
        print(f"Data loaded into DataFrame. Shape: {df_raw.shape}")
    except FileNotFoundError:
        print(f"ERROR: CSV file not found at {csv_path}. Please check file path.")
        return False
    except Exception as e:
        print(f"An error occurred while reading CSV: {e}")
        return False

    try:
        start_time = time.time()
        print(f"Loading data into PostgreSQL table: {raw_table_name}...")

        df_raw.to_sql(raw_table_name, engine, if_exists='replace', index=False)
        
        end_time = time.time()
        print(f"Successfully loaded {df_raw.shape[0]} rows into {raw_table_name}.")
        print(f"Time taken: {end_time - start_time:.2f} seconds.")
        print("--- 1. INGESTION COMPLETE ---")
        return True
        
    except Exception as e:
        print(f"ERROR during DB loading: Please ensure your Docker container (postgres_dw) is running.")
        print(f"Details: {e}")
        return False

if __name__ == '__main__':
    print("Please run the pipeline using 'python run_pipeline.py'")