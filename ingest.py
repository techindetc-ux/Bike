import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.schema import CreateSchema 
import os 
# เราไม่จำเป็นต้อง import ProgrammingError อีกต่อไปเมื่อใช้ if_not_exists=True

# --- 1. กำหนดค่าการเชื่อมต่อ (ตามตัวแปรที่คุณให้มา) ---
DB_HOST = "localhost"
DB_PORT = "5432"
DB_DB = "processors_db"
DB_USER = "fodopec_user"
DB_PASS = "fodopec"

# --- 2. สร้าง DATABASE_URL และ Engine ---
DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_DB}"
engine = create_engine(DATABASE_URL)

# --- 3. โหลดไฟล์และกำหนดตาราง ---
FILE_NAME = "bike_sales_100k.csv"
TABLE_NAME = FILE_NAME.replace('.csv', '') 
SCHEMA_NAME = "raw_data"

try:
    # 3.1 ตรวจสอบและสร้าง Schema 'raw_data' หากยังไม่มี (แก้ไข DuplicateSchema)
    with engine.begin() as connection:
        print(f"ตรวจสอบ/สร้าง Schema '{SCHEMA_NAME}'...")
        # 💡 แก้ไข Duplicate Schema: ใช้ if_not_exists=True
        # คำสั่งนี้จะไม่เกิด error แม้ว่า Schema จะมีอยู่แล้ว
        connection.execute(CreateSchema(SCHEMA_NAME, if_not_exists=True))
        print(f"Schema '{SCHEMA_NAME}' พร้อมใช้งาน")

    
    # 3.2 อ่านไฟล์ CSV (ตรวจสอบว่าไฟล์มีอยู่จริง)
    if not os.path.exists(FILE_NAME):
        raise FileNotFoundError(f"ไม่พบไฟล์: {FILE_NAME}")
        
    df = pd.read_csv(FILE_NAME)
    
    # 3.3 โหลด DataFrame เข้าสู่ PostgreSQL
    print(f"กำลังโหลดข้อมูล {len(df)} แถวเข้าตาราง '{SCHEMA_NAME}.{TABLE_NAME}'...")
    df.to_sql(
        name=TABLE_NAME, 
        con=engine, 
        schema=SCHEMA_NAME, 
        if_exists='replace', # สร้างตารางใหม่ทุกครั้ง
        index=False 
    )
    # ลบ Emoji ในส่วนนี้เพื่อป้องกัน UnicodeEncodeError
    print(f"โหลดข้อมูลสำเร็จ: เข้าสู่ตาราง '{SCHEMA_NAME}.{TABLE_NAME}' ใน PostgreSQL เรียบร้อยแล้ว")

except FileNotFoundError as e:
    print(f"เกิดข้อผิดพลาดในการโหลดข้อมูล: {e}")
except Exception as e:
    # ลบ Emoji ในส่วนนี้เพื่อป้องกัน UnicodeEncodeError
    print(f"เกิดข้อผิดพลาดในการโหลดข้อมูล: {e}")