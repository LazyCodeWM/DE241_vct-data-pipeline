import os
import json
import boto3
from dotenv import load_dotenv

# 1. โหลดค่า Config จากไฟล์ .env
load_dotenv()

# 2. เชื่อมต่อเข้า MinIO
s3 = boto3.client(
    "s3",
    endpoint_url=os.getenv("MINIO_ENDPOINT", "http://localhost:9000"),
    aws_access_key_id=os.getenv("MINIO_ACCESS_KEY"),
    aws_secret_access_key=os.getenv("MINIO_SECRET_KEY"),
    region_name="us-east-1"  # ใส่ไว้กัน boto3 บ่น (MinIO ใช้ region อะไรก็ได้)
)

BUCKET_NAME = "bronze-vct-data"
PREFIX = "events/raw/"

def check_ingested_events():
    print(f"🔍 กำลังสแกนหาทัวร์นาเมนต์ใน {BUCKET_NAME}/{PREFIX} ...\n")
    
    try:
        # ขอลิสต์รายชื่อไฟล์ทั้งหมดในโฟลเดอร์ events/raw/
        response = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=PREFIX)
        
        if "Contents" not in response:
            print("❌ ยังไม่มีไฟล์ Event ถูก ingest เข้ามาเลยครับ")
            return

        files = response["Contents"]
        events_list = []

        # วนลูปอ่านข้อมูลทีละไฟล์
        for file in files:
            file_key = file["Key"]
            
            # ดึงเนื้อหาไฟล์ JSON ออกมา
            obj = s3.get_object(Bucket=BUCKET_NAME, Key=file_key)
            data = json.loads(obj["Body"].read().decode("utf-8"))
            
            # เก็บข้อมูลชื่อทัวร์นาเมนต์
            event_id = data.get("id", "Unknown ID")
            event_name = data.get("name", "Unknown Name")
            event_date = data.get("start_date", "Unknown Date")
            
            events_list.append((event_id, event_name, event_date))

        # เรียงลำดับตาม ID หรือ Date คล่าวๆ
        events_list.sort(key=lambda x: str(x[0]))

        # ปริ้นต์สรุปผล
        print("-" * 60)
        for e_id, e_name, e_date in events_list:
            print(f"[{e_id}] {e_date} | {e_name}")
        print("-" * 60)
        print(f"✅ พบข้อมูลทัวร์นาเมนต์ทั้งหมด: {len(events_list)} รายการ")

    except Exception as e:
        print(f"❌ เกิดข้อผิดพลาด: {e}")

if __name__ == "__main__":
    check_ingested_events()