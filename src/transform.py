# src/transform.py

import pandas as pd
from sqlalchemy import create_engine

def transform_data(database_url, raw_table_name, transformed_table_name):
    """
    ขั้นตอนที่ 2: ดึงข้อมูลจาก Raw Table, แปลงข้อมูล, และโหลดเข้า Transformed Table
    """
    print("--- 2. TRANSFORMATION START ---")
    
    engine = create_engine(database_url)
    
    # 1. ดึงข้อมูลจาก Raw Table
    try:
        sql_query = f"SELECT * FROM {raw_table_name}"
        df_raw = pd.read_sql(sql_query, engine)
        print(f"Successfully retrieved {df_raw.shape[0]} rows from {raw_table_name}.")
    except Exception as e:
        print(f"ERROR: Failed to retrieve data for transformation. Did ingest.py run successfully?")
        print(f"Details: {e}")
        return None

    # 2. การแปลงข้อมูล (Transformation Logic)
    
    # 2.1 คำนวณยอดขายรวม (Total Sales)
    df_raw['Total_Sales'] = df_raw['Price'] * df_raw['Quantity']
    
    # ---------------------------------------------------------
    # 🔥 จุดที่แก้ไข (CRITICAL FIX):
    # ระบุ dayfirst=True เพื่อบังคับให้ Pandas อ่านวันที่เป็น วัน-เดือน-ปี
    # และใช้ errors='coerce' เพื่อเปลี่ยนค่าที่อ่านไม่ออกเป็น NaT แทนที่จะทำให้โปรแกรมพัง
    # ---------------------------------------------------------
    df_raw['Date'] = pd.to_datetime(df_raw['Date'], dayfirst=True, errors='coerce').dt.date

    # ตรวจสอบว่ามีข้อมูลที่แปลงไม่ได้หรือไม่
    invalid_dates = df_raw['Date'].isna().sum()
    if invalid_dates > 0:
        print(f"WARNING: Found {invalid_dates} rows with invalid date format. They were set to NaT.")
        # ลบแถวที่วันที่ผิดปกติออก (Optional: ขึ้นอยู่กับการตัดสินใจทางธุรกิจ)
        df_raw = df_raw.dropna(subset=['Date'])


    # 2.3 สร้างคอลัมน์ Age Group
    bins = [0, 25, 40, 55, 100]
    labels = ['Youth (<25)', 'Young Adult (25-40)', 'Middle Age (41-55)', 'Senior (>55)']
    df_raw['Age_Group'] = pd.cut(df_raw['Customer_Age'], bins=bins, labels=labels, right=True, ordered=True)

    # 2.4 เลือกคอลัมน์ที่ต้องการสำหรับ Final Data Model
    df_transformed = df_raw[[
        'Date', 'Store_Location', 'Bike_Model', 'Customer_Gender', 
        'Customer_Age', 'Age_Group', 'Quantity', 'Price', 'Total_Sales', 
        'Payment_Method'
    ]].copy()
    
    print(f"Data transformed. Final shape: {df_transformed.shape}")

    # 3. โหลด DataFrame ที่แปลงแล้วเข้าสู่ Transformed Table
    try:
        print(f"Loading transformed data into PostgreSQL table: {transformed_table_name}...")
        
        # if_exists='replace' เพื่อสร้างตารางใหม่ทุกครั้ง
        df_transformed.to_sql(transformed_table_name, engine, if_exists='replace', index=False)
        
        print(f"Successfully loaded {df_transformed.shape[0]} rows into {transformed_table_name}.")
        print("--- 2. TRANSFORMATION COMPLETE ---")
        return df_transformed
        
    except Exception as e:
        print(f"ERROR during DB loading of transformed data: {e}")
        return None

if __name__ == '__main__':
    print("Please run the pipeline using 'python run_pipeline.py'")