
import pandas as pd
from sqlalchemy import create_engine

def transform_data(database_url, raw_table_name, transformed_table_name):
    
    print("--- 2. TRANSFORMATION START ---")
    
    engine = create_engine(database_url)
    
    
    try:
        sql_query = f"SELECT * FROM {raw_table_name}"
        df_raw = pd.read_sql(sql_query, engine)
        print(f"Successfully retrieved {df_raw.shape[0]} rows from {raw_table_name}.")
    except Exception as e:
        print(f"ERROR: Failed to retrieve data for transformation. Did ingest.py run successfully?")
        print(f"Details: {e}")
        return None

    
    df_raw['Total_Sales'] = df_raw['Price'] * df_raw['Quantity']
    
   
    df_raw['Date'] = pd.to_datetime(df_raw['Date'], dayfirst=True, errors='coerce').dt.date

    
    invalid_dates = df_raw['Date'].isna().sum()
    if invalid_dates > 0:
        print(f"WARNING: Found {invalid_dates} rows with invalid date format. They were set to NaT.")
       
        df_raw = df_raw.dropna(subset=['Date'])


    
    bins = [0, 25, 40, 55, 100]
    labels = ['Youth (<25)', 'Young Adult (25-40)', 'Middle Age (41-55)', 'Senior (>55)']
    df_raw['Age_Group'] = pd.cut(df_raw['Customer_Age'], bins=bins, labels=labels, right=True, ordered=True)

   
    df_transformed = df_raw[[
        'Date', 'Store_Location', 'Bike_Model', 'Customer_Gender', 
        'Customer_Age', 'Age_Group', 'Quantity', 'Price', 'Total_Sales', 
        'Payment_Method'
    ]].copy()
    
    print(f"Data transformed. Final shape: {df_transformed.shape}")

    
    try:
        print(f"Loading transformed data into PostgreSQL table: {transformed_table_name}...")
        
        
        df_transformed.to_sql(transformed_table_name, engine, if_exists='replace', index=False)
        
        print(f"Successfully loaded {df_transformed.shape[0]} rows into {transformed_table_name}.")
        print("--- 2. TRANSFORMATION COMPLETE ---")
        return df_transformed
        
    except Exception as e:
        print(f"ERROR during DB loading of transformed data: {e}")
        return None

if __name__ == '__main__':
    print("Please run the pipeline using 'python run_pipeline.py'")