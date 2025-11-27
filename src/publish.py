import pandas as pd
from sqlalchemy import create_engine
import gspread
from oauth2client.service_account import ServiceAccountCredentials

def publish_data(database_url, transformed_table_name, gspread_creds, sheet_name, ws_name, df_transformed=None):
    print("--- 3. PUBLISHING START ---")
    

    if df_transformed is None:
        engine = create_engine(database_url)
        try:
            sql_query = f"SELECT * FROM {transformed_table_name}"
            df_transformed = pd.read_sql(sql_query, engine)
            print(f"Retrieved {df_transformed.shape[0]} rows from {transformed_table_name} for publishing.")
        except Exception as e:
            print(f"ERROR: Failed to retrieve transformed data from DB: {e}")
            return False

    if df_transformed.empty:
        print("No data to publish.")
        return False

    try:
        scope = [
            'https://spreadsheets.google.com/feeds', 
            'https://www.googleapis.com/auth/drive'
        ]
        
        creds = ServiceAccountCredentials.from_json_keyfile_name(gspread_creds, scope)
        client = gspread.authorize(creds)
        print("Successfully authorized with Google Sheets API.")
    except FileNotFoundError:
        print(f"ERROR: {gspread_creds} not found. Please place your Service Account credentials file in the root folder.")
        return False
    except Exception as e:
        print(f"ERROR during Google Sheets API connection: {e}")
        return False

    try:

        try:
            spreadsheet = client.open(sheet_name)
            print(f"Opened existing Spreadsheet: {sheet_name}")
        except gspread.SpreadsheetNotFound:
            print(f"Spreadsheet '{sheet_name}' not found. Creating a new one...")
            spreadsheet = client.create(sheet_name)
            print(f"!!! IMPORTANT: New Spreadsheet created. Please share '{sheet_name}' with your Service Account Email manually.")


        try:
            worksheet = spreadsheet.worksheet(ws_name)
        except gspread.WorksheetNotFound:

            worksheet = spreadsheet.add_worksheet(title=ws_name, rows="1", cols="1") 
            print(f"Created new Worksheet: {ws_name}")


        df_for_upload = df_transformed.astype(str)


        data_to_upload = [df_for_upload.columns.tolist()] + df_for_upload.values.tolist()
        
        worksheet.clear() 
        
        worksheet.update(data_to_upload, range_name='A1')
        
        print(f"Successfully uploaded {len(data_to_upload) - 1} rows to Google Sheet: '{sheet_name}' Worksheet: '{ws_name}'")
        print("--- 3. PUBLISHING COMPLETE ---")
        return True
        
    except Exception as e:
        print(f"ERROR during data upload to Google Sheets: {e}")
        return False

if __name__ == '__main__':
    print("Please run the pipeline using 'python run_pipeline.py'")