import os
from ingest import ingest_data
from transform import transform_data
from publish import publish_data


DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "bike_db"
DB_USER = "bike_user"
DB_PASSWORD = "1660902345"

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

RAW_TABLE_NAME = "raw_bike_sales"
TRANSFORMED_TABLE_NAME = "transformed_bike_sales"

CSV_FILE_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'bike_sales_100k.csv')

GSPREAD_CREDENTIALS = "ServiceAccountCredentials.json" 
SPREADSHEET_NAME = "bike_sheet" 
WORKSHEET_NAME = "Transformed_Data" 


def run_pipeline():
  
    print("===========================================")
    print("AUTOMATED BIKE SALES DATA PIPELINE START")
    print("===========================================")

    if not os.path.exists(CSV_FILE_PATH):
        print(f"FATAL ERROR: CSV File not found at {CSV_FILE_PATH}.")
        print("Please ensure 'bike_sales_100k.csv' is in the 'data' folder.")
        return
        
    print("\n[STEP 1/3] Calling Ingestion Process...")
    ingest_success = ingest_data(
        csv_path=CSV_FILE_PATH,
        database_url=DATABASE_URL,
        raw_table_name=RAW_TABLE_NAME
    )
    if not ingest_success:
        print("\n--- PIPELINE FAILED: Ingestion failed. Check PostgreSQL Docker status. ---\n")
        return
    
    print("\n[STEP 2/3] Calling Transformation Process...")
    df_transformed = transform_data(
        database_url=DATABASE_URL,
        raw_table_name=RAW_TABLE_NAME,
        transformed_table_name=TRANSFORMED_TABLE_NAME
    )
    if df_transformed is None:
        print("\n--- PIPELINE FAILED: Transformation failed. ---\n")
        return

    print("\n[STEP 3/3] Calling Publishing Process...")
    publish_success = publish_data(
        database_url=DATABASE_URL,
        transformed_table_name=TRANSFORMED_TABLE_NAME,
        gspread_creds=GSPREAD_CREDENTIALS,
        sheet_name=SPREADSHEET_NAME,
        ws_name=WORKSHEET_NAME,
        df_transformed=df_transformed
    )

    if not publish_success:
        print("\n--- PIPELINE FAILED: Publishing failed. Check Google Sheets API credentials and sharing settings. ---\n")
        return

    print("===========================================")
    print(" PIPELINE COMPLETED SUCCESSFULLY! ")
    print(f"Final data available on Google Sheets: {df_transformed.shape[0]} rows.")
    print("===========================================")

if __name__ == "__main__":
    run_pipeline()