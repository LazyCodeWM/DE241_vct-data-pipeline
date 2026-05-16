# VCT Data Pipeline — Presentation Notes
> อ่านก่อนนำเสนอ: อธิบาย design decisions ทุกจุดในแบบที่อาจารย์น่าจะถาม

---

## 1. ภาพรวม Project

**VCT (VALORANT Champions Tour) Data Pipeline** เป็น batch data pipeline ที่ดึงข้อมูลแข่งขัน esports จาก vlr.gg ผ่าน API wrapper ชื่อ `vlrdevapi` แล้วประมวลผลผ่าน 3 ชั้นตาม **Medallion Architecture** จนได้ analytical tables พร้อมให้ Data Analyst นำไปใช้

```
vlr.gg API → Bronze (raw JSON) → Silver (clean tables) → Gold (aggregated) → DA
```

Pipeline รันแบบ **batch on-demand** — ไม่มี schedule อัตโนมัติ trigger เมื่อต้องการ ผ่าน Apache Airflow

---

## 2. Medallion Architecture — ทำไมถึงใช้?

### แนวคิดหลัก
Medallion Architecture แบ่ง data ออกเป็น 3 ชั้นที่มีความน่าเชื่อถือเพิ่มขึ้นเรื่อยๆ (Bronze → Silver → Gold) แทนที่จะทำ transformation ทุกอย่างในครั้งเดียว

### ทำไม Bronze ถึงเก็บ raw JSON?
- **Replayability**: ถ้า transformation logic ผิดพลาด ไม่ต้องไปดึง API ใหม่ แค่ re-run Silver จาก Bronze ที่มีอยู่
- **Source of Truth**: Bronze คือข้อมูลต้นฉบับ ไม่มีการแตะ ไม่มีการลบ ถ้ามี bug ใน Silver/Gold เสมอมี raw data ให้ย้อนกลับมา debug
- **Decoupling**: ถ้า API ของ vlr.gg เปลี่ยน เราแค่แก้ ingestion layer เดียว Silver/Gold ไม่ต้องแก้

### ทำไม Silver ถึง clean และ standardize?
- Bronze มีปัญหาหลายอย่าง เช่น ค่า null ที่ไม่สม่ำเสมอ, type ที่ไม่แน่นอน (บาง record เป็น int บางอันเป็น float), โครงสร้าง nested ที่ต้อง flatten
- Silver แก้ปัญหาพวกนี้ทั้งหมด ทำให้ Gold layer query ได้ง่าย
- มี **PK validation** — ถ้า row ไหน duplicate primary key จะถูกส่งไป quarantine แทนที่จะปนกับ clean data

### ทำไม Gold ถึง aggregate?
- Data Analyst ไม่อยากมานั่ง JOIN 5 ตารางทุกครั้งที่จะดู win rate ของทีม
- Gold pre-aggregate ผลลัพธ์ที่ใช้บ่อยไว้ เช่น `player_performance`, `team_standings`, `agent_meta`
- เป็น principle ของ **"write once, read many"** — ยอมใช้ compute มากตอน pipeline รัน เพื่อให้ query ตอน read เร็วขึ้น

---

## 3. Tech Stack — ทำไมถึงเลือกแต่ละอัน?

### MinIO (Object Storage)
**MinIO คืออะไร**: S3-compatible object storage รันได้บน local/Docker  
**ทำไมไม่ใช้ AWS S3 จริงๆ**: Course project — ไม่อยากเสียค่า cloud แต่ใช้ interface เดียวกับ S3 ทุกประการ ถ้าต้องการ deploy production แค่เปลี่ยน endpoint จาก `localhost:9000` เป็น AWS S3 endpoint โค้ดไม่ต้องแก้เลย

**ทำไมถึงเก็บใน Object Storage แทน Database ตรงๆ**:  
Object storage scale ได้ไม่จำกัด ราคาถูก และเหมาะกับ unstructured/semi-structured data อย่าง JSON raw files ที่ Bronze ต้องการเก็บ

### Apache Iceberg (Table Format)
**Iceberg คืออะไร**: Table format layer ที่วางทับบน object storage ทำให้ไฟล์ Parquet ที่กระจายอยู่ใน MinIO ดูเหมือน "ตาราง" ที่ query ได้ด้วย SQL

**ทำไมถึงใช้ Iceberg แทนแค่ Parquet ธรรมดา**:
- **Schema evolution**: ถ้าเพิ่ม column ใหม่ใน Silver ข้อมูลเก่าไม่พัง Iceberg จัดการ schema versioning ให้
- **ACID transactions**: เขียนพร้อมกันหลาย task ไม่ทำให้ table corrupt
- **Time travel**: ดูข้อมูลย้อนหลังได้ตาม snapshot (ใช้ตอน debug)
- **Partition pruning**: query เร็วขึ้นโดยที่ application code ไม่ต้องรู้ว่า data อยู่ file ไหน

### Project Nessie (Catalog)
**Nessie คืออะไร**: Iceberg catalog — เก็บ metadata ว่าแต่ละ table อยู่ที่ path ไหน, schema เป็นยังไง, มีกี่ snapshot

**ทำไมต้องมี catalog แยกจาก MinIO**:  
MinIO เก็บแค่ไฟล์ Parquet — ไม่รู้ว่าไฟล์ไหนเป็นตารางอะไร Nessie เป็นตัวเก็บ "สารบัญ" ว่า table `silver.fact_player_stats` ประกอบจากไฟล์ไหนบ้าง มี schema อะไร

**ทำไม Nessie ไม่ใช่ Hive Metastore หรือ Glue**:  
Nessie รองรับ **git-like branching** สำหรับ data — สามารถทดลอง transformation บน branch แล้วค่อย merge เข้า main ได้ เหมาะกับการพัฒนา pipeline โดยไม่กระทบ production data

### Polars (In-memory Transform)
**ทำไมใช้ Polars แทน Pandas**:
- Polars เขียนด้วย Rust — เร็วกว่า Pandas 5-10x สำหรับ data ขนาดกลาง
- Lazy evaluation — วางแผน execution plan ก่อน แล้วค่อย execute ทีเดียว
- API cleaner กว่า Pandas สำหรับ data transformation เช่น `cast`, `fill_null`, `struct.field`

**ทำไมใช้ Polars ตอน transform แต่ใช้ PySpark ตอน write Iceberg**:  
PyIceberg เขียน table ตรงจาก PyArrow ได้ แต่ Spark มี native Iceberg connector ที่ stable กว่า รองรับ partitioning และ schema enforcement ดีกว่า — จึงใช้ Polars สำหรับ business logic แล้ว convert เป็น Spark DataFrame แค่ตอนสุดท้ายตอน write

### Apache Airflow (Orchestration)
**ทำไมต้องมี orchestrator**:  
Pipeline มี task หลายอันที่ต้องทำตามลำดับและบางส่วนทำคู่ขนานกันได้ ถ้าไม่มี orchestrator ต้องใช้ shell script ที่ไม่มี retry, ไม่มี logging, ไม่รู้ task ไหน fail

**ทำไม Airflow ไม่ใช่ Prefect หรือ Dagster**:  
Airflow เป็น industry standard ที่ใช้มากที่สุด เอกสารมาก community ใหญ่ และ TaskFlow API (decorator-based) ทำให้เขียน DAG ง่าย

---

## 4. Data Modeling — ทำไมถึง Design แบบนี้?

### Galaxy Schema ไม่ใช่ Star Schema
**Star Schema**: Fact table เดียว ล้อมด้วย dimension tables  
**Galaxy Schema (Fact Constellation)**: หลาย Fact table share dimension tables เดียวกัน

Project นี้ใช้ Galaxy Schema เพราะข้อมูล VCT มี grain (ระดับความละเอียด) ที่แตกต่างกันหลายระดับ:

| Fact Table | Grain | ตัวอย่าง |
|---|---|---|
| `fact_series` | 1 row / match | NRG vs Sentinels วันที่ X |
| `fact_player_stats` | 1 row / player / map | NRG TenZ บน Ascent |
| `fact_map_scores` | 1 row / map | Ascent: 13-7 |
| `fact_round_results` | 1 row / round | Round 14, NRG wins, Eco |
| `fact_player_agent_stats` | 1 row / player / agent | TenZ บน Jett (career) |

ถ้า flatten ทุกอย่างไว้ใน fact เดียวจะมี null column เต็มไปหมด และ query ยากขึ้น

### ทำไมถึงแยก Silver เป็น 3 tasks (dims / facts_match / facts_meta)?
- **Dependency**: `facts_match` ต้อง join กับ `dim_events` เพื่อ resolve `event_id` จาก event_name — เลยต้องรอ dims เสร็จก่อน
- **facts_meta** (standings, placements, transactions) ไม่ขึ้นกับ `facts_match` เลย สามารถรันคู่ขนานกับ dims ได้
- การแยกแบบนี้ทำให้ถ้า `facts_match` fail ไม่ต้อง re-run dims ใหม่ทั้งหมด Airflow retry แค่ task ที่ fail

### ทำไมถึงแยก dim_team_roster ออกจาก dim_teams?
Design ทางเลือก: เก็บ roster เป็น array ใน dim_teams  
เหตุผลที่ไม่ทำ: ถ้า roster เปลี่ยน (ซื้อขายนักกีฬา) แล้ว join กับ fact_player_stats โดย player_id จะยุ่งมาก การแยกเป็นตารางแยกทำให้ JOIN ง่ายและ update เฉพาะ roster ได้โดยไม่กระทบ team info

---

## 5. Idempotency Design — สำคัญแค่ไหน?

**Idempotent pipeline**: รันกี่รอบก็ได้ผลเดิมเสมอ

### ทำไมต้องทำ idempotency?
ใน production pipeline มักเกิดสถานการณ์ที่ต้อง re-run เช่น:
- Task fail กลางทาง แล้ว Airflow retry
- Data source เปลี่ยน ต้องดึงใหม่
- นักพัฒนาแก้ bug แล้วต้อง re-process

ถ้าไม่ทำ idempotency จะได้ข้อมูลซ้ำทุกครั้งที่ re-run

### วิธีที่ใช้ในแต่ละ layer:
- **Bronze**: `_key_exists()` check ด้วย `head_object` ก่อน upload — ถ้า key มีอยู่แล้ว skip ทั้ง API call และ upload
- **Bronze (DAG run)**: Early-exit guard — เช็ค `series/raw/` prefix ตอนต้น ถ้ามีข้อมูลอยู่แล้วข้ามทั้ง task
- **Silver**: `createOrReplace` ใน Iceberg — overwrite table ทั้งใบ ไม่ append
- **Gold**: Drop table เก่าผ่าน PyIceberg แล้วสร้างใหม่ทุกครั้ง

---

## 6. Data Quality — Quarantine Pattern

### ทำไมต้องมี quarantine?
ถ้าข้อมูล Bronze มี primary key ซ้ำ (เช่น match_id เดียวกันขึ้นมาสองครั้ง) จะทำให้ analytical query ได้ผลผิดพลาด (count สองเท่า, join ขยาย rows)

แทนที่จะ fail pipeline หรือเก็บ dirty data ไว้:
- row ที่มี PK ซ้ำจะถูก route ไป `silver-vct-data/quarantine/` เป็น Parquet
- pipeline ยังทำงานต่อด้วย clean data
- สามารถ audit ได้ว่า row ไหน fail ทำไม

### ตัวอย่าง quarantine ที่เห็นใน log:
```
quarantine fact_team_transactions : 308 rows → s3://silver-vct-data/quarantine/
```
แปลว่า transactions 308 rows มี PK ซ้ำ (team_id, player_id, transaction_date เดิม) — อาจเพราะ API ส่งข้อมูลเดิมมาซ้ำกันในหลาย event

---

## 7. Pipeline Orchestration Design

### DAG Task Dependencies
```
start
  └─► bronze_ingestion
        ├─► silver_dims ──────────────────► silver_facts_match
        │                                         │
        └─► silver_facts_meta                     │
                │                                 │
                └──────────┬──────────────────────┘
                           ▼
          ┌────────────────┼─────────────────┐
          ▼                ▼                 ▼                 ▼
    gold_players    gold_agents        gold_teams        gold_matches
          └────────────────┴─────────────────┴─────────────────┘
                           ▼
                          end
```

**ทำไม silver_dims และ silver_facts_meta รันคู่ขนาน**:  
ทั้งสองไม่มี dependency กัน facts_meta (standings, placements, transactions) ดึงจาก Bronze prefix คนละชุดกับ dims รันพร้อมกันได้ ประหยัดเวลา

**ทำไม silver_facts_match ต้องรอ silver_dims**:  
`transform_fact_series` ต้อง join series ข้าม event_name เพื่อ resolve `event_id` — event_id มาจาก `dim_events` ที่ silver_dims สร้าง ถ้า dims ยังไม่เสร็จ lookup จะ fail

**ทำไม Gold tasks ทุกอันรันคู่ขนาน**:  
Gold แต่ละ task เป็น independent aggregation อ่านจาก Silver ซึ่งเขียนเสร็จแล้ว ไม่มี dependency ระหว่างกัน รันพร้อมกัน 4 task ได้เลย

### schedule=None
Pipeline ตั้งเป็น manual trigger เพราะ:
- ข้อมูล VCT ไม่ได้เปลี่ยนทุกวัน (มีแข่งเป็นช่วงๆ)
- Batch on-demand เหมาะกว่า — trigger เมื่อ season จบหรือเมื่อต้องการ refresh
- ถ้าต้องการ incremental ในอนาคตแค่เปลี่ยน `schedule="@weekly"` หรืออะไรก็ได้

---

## 8. คำถามที่น่าจะโดนถาม

**Q: ทำไมไม่ใช้ dbt แทน Python สำหรับ transformation?**  
dbt ดีสำหรับ SQL-native transformation ที่ data อยู่ใน data warehouse แล้ว แต่ pipeline นี้ต้องอ่านจาก object storage (MinIO) ผ่าน API wrapper และทำ complex struct flattening ที่ SQL เดียวทำไม่ได้ Polars เหมาะกว่าสำหรับ data wrangling ระหว่างทาง

**Q: ทำไมไม่ใช้ Spark ตั้งแต่ต้นแทน Polars?**  
ข้อมูลขนาด 5,000-10,000 rows ไม่จำเป็นต้องใช้ distributed compute Spark มี overhead สูง (JVM startup, task scheduling) สำหรับ data ขนาดนี้ Polars ใน single machine เร็วกว่า Spark ถ้าเอา Spark มาใช้ตั้งแต่ต้นจะช้ากว่าแทน — Spark ถูกใช้เฉพาะตอนสุดท้ายที่ต้องเขียน Iceberg เพราะ Spark Iceberg connector มัน mature ที่สุด

**Q: ถ้า API ของ vlr.gg down ระหว่าง backfill จะเกิดอะไร?**  
Bronze ingestion มี retry logic ด้วย exponential wait — RateLimitError, NetworkError, ScrapingError ทุกอันมี retry 3 รอบ ถ้า fail ทั้ง 3 รอบ match นั้นจะถูก skip แล้วทำ match ถัดไปต่อ pipeline ไม่ crash ทั้งหมด

**Q: ถ้าต้องการเพิ่ม event ใหม่ปี 2026 ทำยังไง?**  
แก้แค่ `BACKFILL_END_YEAR = 2026` ใน config แล้ว trigger DAG ใหม่ — `_key_exists` จะ skip ข้อมูลปี 2025 ที่มีอยู่แล้ว แล้วดึงเฉพาะปี 2026 ที่ยังไม่มี

**Q: Gold layer ใช้ข้อมูลอะไรบ้าง?**  
| Gold Table | ใช้สำหรับ |
|---|---|
| `player_performance` | rank นักกีฬาในแต่ละ tournament |
| `player_map_performance` | จุดแข็ง/อ่อนของนักกีฬาบนแต่ละ map |
| `team_standings` | win/loss record ทีม |
| `team_map_performance` | ทีมถนัด map ไหน, attacker/defender side |
| `agent_meta` | pick rate, win rate แยกตาม agent และ map |
| `agent_player_affinity` | นักกีฬาถนัด agent ไหน |
| `match_summary` | ภาพรวม match พร้อม winner |

**Q: Data lineage คืออะไร และ project นี้มีไหม?**  
Data lineage คือการติดตามว่าข้อมูลใน Gold มาจากไหน — ใน project นี้ lineage ชัดเจนผ่าน Airflow DAG graph: Gold มาจาก Silver, Silver มาจาก Bronze, Bronze มาจาก vlr.gg API

**Q: ถ้า Silver table schema เปลี่ยน Gold จะพังไหม?**  
Iceberg รองรับ schema evolution — เพิ่ม column ใหม่โดยไม่ทำ Gold พัง แต่ถ้าลบหรือเปลี่ยนชื่อ column ที่ Gold ใช้อยู่ Spark SQL จะ fail ตอน build Gold ซึ่งเป็น intentional — อยากให้รู้ทันทีถ้า schema เปลี่ยน

---

## 9. สรุปในหนึ่งประโยค

> Pipeline นี้ออกแบบให้เป็น **idempotent batch pipeline** ที่ใช้ Medallion Architecture แยก concern ออกเป็นชัดเจน — Bronze เก็บ raw data ไว้เสมอเพื่อ replayability, Silver ทำ quality และ modeling, Gold pre-aggregate เพื่อ DA — โดยทุก design decision มาจากหลักการ data engineering จริงๆ ไม่ใช่แค่ทำให้มันรันได้
