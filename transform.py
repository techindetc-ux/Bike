import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
import datetime

# --- 1. CONFIG: PostgreSQL (ใช้ค่าเดิม) ---
DB_HOST = "localhost"
DB_PORT = "5432"
DB_DB = "processors_db"
DB_USER = "fodopec_user"
DB_PASS = "fodopec"

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_DB}"
engine = create_engine(DATABASE_URL)

# --- 2. CONFIG: ตารางข้อมูล ---
RAW_SCHEMA = "raw_data"
RAW_TABLE = "bike_sales_100k"
PRODUCTION_SCHEMA = "production"
PRODUCTION_TABLE = "monthly_sales_summary" # ตารางสรุปผลรายเดือน

# --- 3. SQL: คำสั่งสร้าง Schema (Idempotent) ---
CREATE_SCHEMA_SQL = text(f"CREATE SCHEMA IF NOT EXISTS {PRODUCTION_SCHEMA}")

# --- 4. SQL: คำสั่ง Transformation (Aggregation) ---
# เราจะคำนวณ Revenue และสรุปยอดขายรายเดือนตาม Bike Model และ Store Location
TRANSFORM_SQL = f"""
WITH cleaned_sales AS (
    SELECT
        "Date" AS sale_date,
        "Bike_Model" AS bike_model,
        "Store_Location" AS store_location,
        "Price" * "Quantity" AS revenue, -- คำนวณรายได้
        "Quantity" AS quantity
    FROM {RAW_SCHEMA}.{RAW_TABLE}
)
SELECT
    -- แปลงวันที่ให้อยู่ในรูปแบบ YYYY-MM-01 เพื่อจัดกลุ่มตามเดือน
    DATE_TRUNC('month', TO_DATE(sale_date, 'DD-MM-YYYY'))::date AS "Month", 
    bike_model AS "Bike Model",
    store_location AS "Store Location",
    COUNT(*) AS "Total Transactions",
    SUM(quantity) AS "Total Quantity Sold",
    ROUND(SUM(revenue)::numeric, 2) AS "Total Revenue"
FROM cleaned_sales
GROUP BY 1, 2, 3
ORDER BY "Month", "Total Revenue" DESC;
"""

def run_transform():
    print("--- เริ่มต้นขั้นตอน Transformation (สร้างตารางสรุปผล) ---")
    
    try:
        # 4.1 ตรวจสอบ/สร้าง Schema 'production'
        with engine.begin() as connection:
            print(f"ตรวจสอบ/สร้าง Schema '{PRODUCTION_SCHEMA}'...")
            connection.execute(CREATE_SCHEMA_SQL)
            print(f"Schema '{PRODUCTION_SCHEMA}' พร้อมใช้งาน")

        # 4.2 ดึงข้อมูลและแปลงข้อมูลด้วย SQL (เร็วและมีประสิทธิภาพ)
        print("กำลังรัน SQL Transformation เพื่อสรุปผลรายเดือน...")
        df_summary = pd.read_sql(TRANSFORM_SQL, engine)
        
        # 💡 แก้ไข: ลบ Emoji '\u2705'
        print(f"สรุปผลได้ {len(df_summary)} แถว (ข้อมูลรายเดือน)") 
        
        # 4.3 โหลดข้อมูลสรุปเข้าสู่ตาราง Production
        print(f"กำลังโหลดข้อมูลสรุปเข้าตาราง '{PRODUCTION_SCHEMA}.{PRODUCTION_TABLE}'...")
        df_summary.to_sql(
            name=PRODUCTION_TABLE, 
            con=engine, 
            schema=PRODUCTION_SCHEMA, 
            if_exists='replace', # สร้างตารางใหม่ทุกครั้ง
            index=False 
        )
        
        # 💡 แก้ไข: ลบ Emoji '\u2705'
        print(f"โหลดข้อมูลสำเร็จ! ตาราง '{PRODUCTION_SCHEMA}.{PRODUCTION_TABLE}' พร้อมใช้งาน")
        
    except SQLAlchemyError as e:
        # 💡 แก้ไข: ลบ Emoji '\u274c'
        print(f"เกิดข้อผิดพลาดใน Database ระหว่าง Transformation: {e}")
        #sys.exit(1)
    except Exception as e:
        # 💡 แก้ไข: ลบ Emoji '\u274c'
        print(f"เกิดข้อผิดพลาดที่ไม่คาดคิดใน transform.py: {e}")
        #sys.exit(1)


if __name__ == "__main__":
    run_transform()