import pandas as pd
from sqlalchemy import create_engine
import gspread
from sqlalchemy.exc import SQLAlchemyError
import datetime
import os
from typing import List

# --- 1. CONFIG: PostgreSQL (ใช้ค่าจาก .env) ---
DB_HOST = "localhost"
DB_PORT = "5432"
DB_DB = "processors_db"
DB_USER = "fodopec_user"
DB_PASS = "fodopec"

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_DB}"
engine = create_engine(DATABASE_URL)

# --- 2. CONFIG: Google Sheets ---
# ไฟล์ที่ใช้กำหนดค่า Client ID/Secret
USER_CREDENTIALS_FILE = "gspread_credentials.json"
# ไฟล์ที่ gspread จะสร้างขึ้นเองหลังยืนยันสิทธิ์ (Token)
AUTHORIZED_USER_FILE = "authorized_user.json" 

# ใช้ชื่อ Sheet ใหม่ที่คุณตั้งไว้
GSHEET_NAME = "Bike_Data"  
WORKSHEET_NAME = "Monthly_Sales_Data" 
PRODUCTION_TABLE = "monthly_sales_summary"
PRODUCTION_SCHEMA = "production"

# --- 3. FUNTION: รันหลัก ---
def run_publish():
    print("--- เริ่มต้นขั้นตอน Publication (อัปโหลดขึ้น Google Sheets) ---")

    SQL_QUERY_READ = f"SELECT * FROM {PRODUCTION_SCHEMA}.{PRODUCTION_TABLE} ORDER BY \"Month\""

    try:
        # 3.1 ดึงข้อมูลจาก PostgreSQL 
        df_publish = pd.read_sql(SQL_QUERY_READ, engine)
        
        # 💡 แก้ไข Unicode: ลบ Emoji
        print(f"ดึงข้อมูล {len(df_publish)} แถวจาก {PRODUCTION_SCHEMA}.{PRODUCTION_TABLE} เรียบร้อยแล้ว")

        # 3.2 การเชื่อมต่อ Google Sheets API (User OAuth)
        
        # 🚨 การตรวจสอบไฟล์ Credentials (Client ID JSON)
        if not os.path.exists(USER_CREDENTIALS_FILE):
             print(f"ข้อผิดพลาด: ไม่พบไฟล์ Credentials ที่สำคัญ **{USER_CREDENTIALS_FILE}**")
             print("โปรดวางไฟล์ Client ID JSON ที่ถูกต้องในโฟลเดอร์โครงการ แล้วรันใหม่")
             raise FileNotFoundError(f"Missing credential file: {USER_CREDENTIALS_FILE}")
             
        # เชื่อมต่อ: จะใช้ authorized_user.json ถ้ามี หรือเปิดเบราว์เซอร์ถ้าไม่มี
        # 💡 แก้ไข Unicode: ลบ Emoji
        print("กำลังเชื่อมต่อ Google Sheets API (อาจต้องยืนยันสิทธิ์ในเบราว์เซอร์ครั้งแรก)...")
        client = gspread.oauth(
            credentials_filename=USER_CREDENTIALS_FILE,
            authorized_user_filename=AUTHORIZED_USER_FILE
        )
        # 💡 แก้ไข Unicode: ลบ Emoji
        print("เชื่อมต่อ Sheets API สำเร็จ")

        # 3.3 เปิด Spreadsheet และจัดการ Worksheet
        spreadsheet = client.open(GSHEET_NAME)
        
        try:
            worksheet = spreadsheet.worksheet(WORKSHEET_NAME)
        except gspread.WorksheetNotFound:
            worksheet = spreadsheet.add_worksheet(title=WORKSHEET_NAME, rows="1", cols="1")
            # 💡 แก้ไข Unicode: ลบ Emoji
            print(f"สร้าง Worksheet ใหม่ชื่อ '{WORKSHEET_NAME}' เรียบร้อยแล้ว")
            
        # 3.4 การเขียนข้อมูล
        data_to_write: List[List] = [df_publish.columns.tolist()] + df_publish.values.tolist()
        
        # เคลียร์ข้อมูลเก่าทั้งหมดและอัปเดต
        worksheet.clear() 
        worksheet.update('A1', data_to_write)
        
        # 💡 แก้ไข Unicode: ลบ Emoji
        print(f"อัปโหลดข้อมูลไปยัง Google Sheet '{GSHEET_NAME}' ใน Worksheet '{WORKSHEET_NAME}' สำเร็จแล้ว")
        print(f"เวลาอัปโหลด: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    except FileNotFoundError as e:
        # Catch FileNotFoundError ที่เกิดจากการตรวจสอบด้านบน
        print(f"การดำเนินการล้มเหลว: {e}")
    except gspread.exceptions.SpreadsheetNotFound:
        print(f"ข้อผิดพลาด: ไม่พบ Spreadsheet ชื่อ '{GSHEET_NAME}' โปรดตรวจสอบว่าคุณได้สร้าง Sheet **'Bike_Data'** และแชร์สิทธิ์กับบัญชี Google ที่ใช้ยืนยันสิทธิ์")
    except Exception as e:
        # 💡 แก้ไข Unicode: ลบ Emoji
        print(f"เกิดข้อผิดพลาดที่ไม่คาดคิดใน publish.py: {e}")


if __name__ == "__main__":
    run_publish()