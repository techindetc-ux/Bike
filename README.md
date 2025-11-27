# 🚴 Bike Sales Automated Data Pipeline (Kaggle Data Edition)

## 🎯 ภารกิจ (Project Mission)

โปรเจกต์นี้คือการสร้างไปป์ไลน์ข้อมูลอัตโนมัติแบบครบวงจร (Automated Data Pipeline) สำหรับชุดข้อมูล **Bike Sales 100K Records** ที่ถูกนำมาจาก Kaggle โดยใช้สถาปัตยกรรม **ELT** (Extract, Load, Transform) และส่งผลลัพธ์ไปยัง Google Sheets เพื่อใช้สร้าง Dashboard ใน Looker Studio

## ⚙️ สถาปัตยกรรม (Architecture)


[Image of ETL data pipeline architecture]

* **Source:** `bike_sales_100k.csv`
* **Ingestion & Data Warehouse:** Python (Pandas/SQLAlchemy) โหลดข้อมูลเข้า PostgreSQL (รันบน Docker)
* **Transformation:** Python (Pandas) ดึงข้อมูลจาก Raw Table, แปลง, และโหลดเข้า Transformed Table ใน PostgreSQL
* **Publish:** Python (gspread) ส่งข้อมูลจาก Transformed Table ไปยัง Google Sheets
* **Visualization:** Looker Studio เชื่อมต่อกับ Google Sheets

## 🛠️ เครื่องมือที่ใช้ (Tech Stack)

| ส่วนประกอบ | เครื่องมือ | บทบาท |
| :--- | :--- | :--- |
| **Data Source** | Kaggle / CSV file | ข้อมูลการขายจักรยาน |
| **Data Warehouse** | PostgreSQL | จัดเก็บข้อมูลดิบและข้อมูลที่ผ่านการแปลง |
| **Pipeline Core** | Python 3.10+ | โค้ดสำหรับรันทุกขั้นตอน |
| **Libraries** | `pandas`, `sqlalchemy`, `psycopg2-binary`, `gspread`, `oauth2client` | จัดการข้อมูล, เชื่อมต่อ DB, เชื่อมต่อ Google Sheets |
| **Containerization**| Docker / Docker Compose | รัน PostgreSQL อย่างง่ายดาย |
| **BI Tool** | Looker Studio | สร้าง Dashboard วิเคราะห์ผลลัพธ์ |

## 🚀 วิธีรันโปรเจกต์ (Getting Started)

### 1. การติดตั้ง (Setup)

#### 1.1 ติดตั้ง Python Dependencies:

```bash
pip install pandas sqlalchemy psycopg2-binary gspread oauth2client