# 📦 Orders Data Pipeline

> Materi Pendamping Pelatihan Big Data Lab MCI 2026 · End-to-End Modern Data Stack Implementation

Proyek ini mengekstrak data order dari **Orders API**, memprosesnya dengan **Apache Spark**, mengorkestrasinya via **Apache Airflow**, menyimpannya ke **ClickHouse**, dan memvisualisasikannya di **Metabase**. Seluruh komponen dikemas dalam Docker Compose agar bisa dijalankan dengan alur yang sama seperti proyek referensi.

Arsitektur mengadopsi prinsip dari *"Mining of Massive Datasets"* (MMDS) dengan pendekatan **batch pipeline** harian. Data dari API berbentuk JSON bertingkat, lalu diubah menjadi format tabular: satu baris merepresentasikan satu produk dalam satu order.

---

## 🏗️ Arsitektur Sistem

```text
Orders API
     ↓  (100 orders / request)
[Ingestion — Python requests]
     ↓  flatten orders -> products
[Data Lake — folder lokal]
     ↓  simpan .parquet / .csv
[Processing — Apache Spark]
     ↓  cleaning, deduplication, type casting
[Data Warehouse — ClickHouse]
     ↓  JDBC insert ke analytics.orders_raw
[Dashboard — Metabase]

↻  Seluruh siklus diatur oleh Apache Airflow
```

**Metrik yang dianalisis:**

- **Volume Order & User** — jumlah order unik, user unik, dan produk terjual
- **Reorder Behavior** — seberapa sering produk dibeli ulang oleh user
- **Basket Analysis** — rata-rata produk per order dan distribusi ukuran keranjang
- **Product & Category Performance** — produk, aisle, dan department dengan penjualan tertinggi
- **Shopping Pattern** — pola belanja berdasarkan hari, jam, urutan order, dan jarak dari order sebelumnya

---

## 🛠️ Tech Stack

| Komponen | Teknologi |
|----------|-----------|
| Orchestration | Apache Airflow 2.9 |
| Processing | Apache Spark / PySpark 3.5 |
| Data Warehouse | ClickHouse (column-oriented OLAP) |
| BI & Dashboard | Metabase |
| Infrastructure | Docker & Docker Compose |
| Language | Python 3.11 |

---

## 📂 Struktur Proyek

```text
orders-data-pipeline/
├── dags/
│   ├── scripts/
│   │   ├── fetch_orders_stream.py       # Ekstraksi API -> Data Lake
│   │   └── process_orders_spark.py      # PySpark: cleaning & load ke ClickHouse
│   └── orders_pipeline.py               # Definisi DAG Airflow
├── screenshots/
│   └── .gitkeep                         # Folder penyimpanan bukti screenshot
├── sql/
│   ├── ddl_clickhouse.sql               # DDL database dan tabel ClickHouse
│   └── queries_metabase.sql             # Katalog query visualisasi Metabase
├── docker-compose.yml                   # Konfigurasi seluruh service
├── Dockerfile                           # Custom Airflow image (+ Java JRE)
├── requirements.txt                     # Dependensi Python
├── README.md                            # Dokumentasi proyek
└── .gitignore
```

---

## 🚀 Tutorial

### Pastikan

- [Docker Desktop](https://docs.docker.com/get-docker/) sudah terinstal
- Menggunakan Git Bash, PowerShell, atau terminal lain yang mendukung Docker Compose
- Port `8080`, `3000`, `8123`, dan `9000` belum dipakai service lain
- Koneksi internet aktif untuk build image dan menarik dependency Spark JDBC

---

### Step 1 — Buat Struktur Folder

Membuat folder utama:

```bash
mkdir orders-data-pipeline
cd orders-data-pipeline
```

Membuat folder untuk DAG, script, SQL, screenshot, dan data sementara:

```bash
mkdir -p dags/scripts sql screenshots data_lake
```

Membuat file kosong untuk nanti:

```bash
touch docker-compose.yml Dockerfile requirements.txt .gitignore README.md
touch dags/orders_pipeline.py
touch dags/scripts/fetch_orders_stream.py
touch dags/scripts/process_orders_spark.py
touch sql/ddl_clickhouse.sql
touch sql/queries_metabase.sql
touch screenshots/.gitkeep
```

> `dags/` → dibaca otomatis oleh Airflow untuk mendefinisikan jadwal dan alur kerja  
> `dags/scripts/` → logika utama kode ingestion dan processing  
> `data_lake/` → penyimpanan sementara file hasil ingest  
> `sql/` → DDL ClickHouse dan query dashboard Metabase  
> `screenshots/` → tempat menyimpan bukti visual Airflow, ClickHouse, dan Metabase

---

### Step 2 — Isi File Konfigurasi & Kode

Isi masing-masing file dengan mengcopas code dari repo ini:

| File | Fungsi |
|------|--------|
| `requirements.txt` | Library Python yang diinstal otomatis (`requests`, `pandas`, `pyspark`, `pyarrow`, `clickhouse-driver`) |
| `Dockerfile` | Instruksi merakit container Airflow + Java JRE untuk Spark |
| `docker-compose.yml` | Mengatur Postgres, Airflow, ClickHouse, dan Metabase dalam satu environment |
| `fetch_orders_stream.py` | Tarik data dari Orders API, flatten `orders -> products`, lalu simpan ke Data Lake |
| `process_orders_spark.py` | Baca staged file, cleaning, deduplication, dan load ke ClickHouse via JDBC |
| `orders_pipeline.py` | DAG Airflow: jadwal dan urutan task |
| `ddl_clickhouse.sql` | DDL database `analytics`, tabel `orders_raw`, dan view `orders` |
| `queries_metabase.sql` | Katalog 20 query analitik untuk visualisasi Metabase |

> ⚠️ **Perhatikan**  
> Endpoint default yang digunakan adalah `http://96.9.212.102:8000/orders`. Jika endpoint berubah, ubah environment variable `ORDERS_API_URL`.

---

### Step 3 — Jalankan Docker

Build image. Jangan lupa buka Docker Desktop terlebih dahulu.

```bash
docker-compose build
```

Inisialisasi database Airflow dan user admin:

```bash
docker-compose up airflow-init
```

Jalankan seluruh pipeline service:

```bash
docker-compose up -d
```

> Tunggu 1-2 menit lalu buka **http://localhost:8080**

---

### Step 4 — Aktifkan Pipeline di Airflow

1. Buka **http://localhost:8080** → login `admin` / `admin`
2. Temukan DAG **`orders_pipeline`**, geser sakelar untuk mengaktifkan
3. Klik ▶️ **Trigger DAG** untuk memaksanya jalan sekarang

**Yang terjadi di balik layar:**

```text
[Trigger]
    ↓
[Task 1: fetch_orders]
    → Orders API → flatten orders/products → simpan staged file ✅
    ↓
[Task 2: process_orders]
    → Spark baca staged file → cleaning → deduplication → simpan processed parquet ✅
    ↓
[Task 3: load_to_clickhouse]
    → Spark baca processed parquet → JDBC insert ke ClickHouse → cleanup staging ✅
    ↓
[Menunggu schedule harian berikutnya...]
```

Gambar berikut dapat digunakan untuk menunjukkan bahwa file staging sudah terbentuk di folder Data Lake setelah task `fetch_orders` berjalan. Ini menjadi bukti bahwa data order berhasil diambil dari API dan disimpan sebelum diproses Spark.

📸 [Tambahkan screenshot di sini — simpan file PNG ke folder `screenshots/` dan referensikan dengan: `![nama](screenshots/nama.png)`]

![orders-data-lake-file](screenshots/orders-data-lake-file.png)

Gambar berikut dapat digunakan untuk menampilkan tab Graph Airflow. Graph harus menunjukkan tiga task yang saling terhubung: `fetch_orders`, `process_orders`, dan `load_to_clickhouse`. Ketika semua kotak berwarna hijau, artinya dependency antar-task berjalan sukses dari awal sampai akhir.

![airflow-dag-graph](screenshots/airflow-dag-graph.png)

Gambar berikut dapat digunakan untuk menampilkan Grid atau DAG Runs Summary. Bagian ini menunjukkan jumlah run yang berhasil, durasi eksekusi, dan status setiap task.

![airflow-dag-summary](screenshots/airflow-dag-summary.png)

Gambar berikut dapat digunakan untuk menampilkan log task `fetch_orders`. Log yang baik akan menunjukkan pesan pengambilan API dan jumlah baris produk yang berhasil disimpan ke staging.

![airflow-fetch-log](screenshots/airflow-fetch-log.png)

---

### Step 5 — Validasi Data di ClickHouse

Masuk ke database ClickHouse:

```bash
docker-compose exec clickhouse-server clickhouse-client \
  --user admin \
  --password rahasia
```

Masuki database dan jalankan query untuk melihat data:

```sql
SHOW DATABASES;
USE analytics;

DESCRIBE analytics.orders_raw;
SELECT COUNT(*) FROM analytics.orders_raw;
```

Melihat 10 baris order-product terbaru:

```sql
SELECT
    order_id,
    user_id,
    product_id,
    product_name,
    department,
    reordered,
    ingested_at
FROM analytics.orders_raw
ORDER BY ingested_at DESC
LIMIT 10;
```

Melihat produk yang paling sering dibeli:

```sql
SELECT
    product_name,
    COUNT(*) AS total_items_sold
FROM analytics.orders_raw
GROUP BY product_name
ORDER BY total_items_sold DESC
LIMIT 10;
```

Melihat produk dengan reorder rate tertinggi:

```sql
SELECT
    product_name,
    COUNT(*) AS total_items_sold,
    ROUND(SUM(reordered) / COUNT(*) * 100, 2) AS reorder_rate_pct
FROM analytics.orders_raw
GROUP BY product_id, product_name
HAVING total_items_sold >= 5
ORDER BY reorder_rate_pct DESC, total_items_sold DESC
LIMIT 10;
```

Keluar dari ClickHouse:

```sql
exit
```

📸 [Tambahkan screenshot di sini — simpan file PNG ke folder `screenshots/` dan referensikan dengan: `![nama](screenshots/nama.png)`]

![clickhouse-table-verification](screenshots/clickhouse-table-verification.png)

---

## Database & Schema

Database yang digunakan:

```sql
CREATE DATABASE IF NOT EXISTS analytics;
```

### `analytics.orders_raw`

Tabel utama dengan engine `MergeTree()` dan `ORDER BY (order_id, product_id)`.

| Kolom | Tipe ClickHouse | Keterangan |
| --- | --- | --- |
| `order_id` | `UInt32` | ID unik order |
| `user_id` | `UInt32` | ID user yang melakukan order |
| `order_number` | `UInt16` | Urutan order milik user |
| `order_dow` | `UInt8` | Hari order, 0=Minggu sampai 6=Sabtu |
| `order_hour_of_day` | `UInt8` | Jam order, 0 sampai 23 |
| `days_since_prior_order` | `Nullable(UInt16)` | Jarak hari dari order sebelumnya |
| `eval_set` | `String` | Label dataset, misalnya `prior` |
| `product_id` | `UInt32` | ID produk |
| `product_name` | `String` | Nama produk |
| `aisle_id` | `UInt16` | ID aisle |
| `aisle` | `String` | Nama aisle |
| `department_id` | `UInt16` | ID department |
| `department` | `String` | Nama department |
| `add_to_cart_order` | `UInt16` | Urutan produk masuk keranjang |
| `reordered` | `UInt8` | Flag reorder, 0 atau 1 |
| `ingested_at` | `DateTime` | Timestamp saat data dimuat |

### `analytics.orders`

View bersih untuk kebutuhan BI. View ini membaca dari `analytics.orders_raw` dan hanya mengambil baris dengan key valid seperti `order_id`, `user_id`, `product_id`, dan `product_name`.

---

### Step 6 — Visualisasi di Metabase

1. Buka **http://localhost:3000**, isi data diri untuk setup awal Metabase
2. Di halaman **Add your data**, pilih ClickHouse lalu isi koneksi berikut:

| Field | Value |
|-------|-------|
| Database type | ClickHouse |
| Display name | Data Warehouse Orders |
| Host | `clickhouse-server` |
| Port | `8123` |
| Database name | `analytics` |
| Username | `admin` |
| Password | `rahasia` |

3. Klik **+ New → SQL Query**
4. Pilih database **Data Warehouse Orders**
5. Salin query dari `sql/queries_metabase.sql`
6. Klik **Visualize**, pilih chart sesuai kebutuhan, lalu simpan ke dashboard

📸 [Tambahkan screenshot di sini — simpan file PNG ke folder `screenshots/` dan referensikan dengan: `![nama](screenshots/nama.png)`]

### Katalog Query Metabase

| No | Chart Type | Insight |
| --- | --- | --- |
| Q01 | KPI / Number Card | Total unique orders |
| Q02 | KPI / Number Card | Total unique users |
| Q03 | KPI / Number Card | Total products sold |
| Q04 | KPI / Number Card | Overall reorder rate (%) |
| Q05 | KPI / Number Card | Average products per order |
| Q06 | Bar Chart | Top 10 most ordered products |
| Q07 | Bar Chart | Top 10 departments by total items sold |
| Q08 | Bar Chart | Top 10 aisles by total items sold |
| Q09 | Bar Chart | Top 10 products with highest reorder rate |
| Q10 | Distribution Chart | Order count by day of week |
| Q11 | Distribution Chart | Order count by hour of day |
| Q12 | Distribution Chart | Distribution of order size |
| Q13 | Scatter / Bubble | Per-product total orders vs reorder rate |
| Q14 | Scatter / Bubble | Per-user order frequency vs average order size |
| Q15 | Time / Trend | Order volume trend by days since prior order |
| Q16 | Time / Trend | Reorder rate trend across order number |
| Q17 | Pivot / Cohort | Reorder rate by department and day of week |
| Q18 | Pivot / Cohort | Average cart size by hour and day of week |
| Q19 | Funnel / Table | Product table with reorder percentage and department rank |
| Q20 | Funnel / Table | Top 20 users by total orders placed |

Placeholder screenshot chart Metabase:

![q01-total-unique-orders](screenshots/q01-total-unique-orders.png)

![q02-total-unique-users](screenshots/q02-total-unique-users.png)

![q03-total-products-sold](screenshots/q03-total-products-sold.png)

![q04-overall-reorder-rate](screenshots/q04-overall-reorder-rate.png)

![q05-average-products-per-order](screenshots/q05-average-products-per-order.png)

![q06-top-products](screenshots/q06-top-products.png)

![q07-top-departments](screenshots/q07-top-departments.png)

![q08-top-aisles](screenshots/q08-top-aisles.png)

![q09-highest-reorder-products](screenshots/q09-highest-reorder-products.png)

![q10-orders-by-day](screenshots/q10-orders-by-day.png)

![q11-orders-by-hour](screenshots/q11-orders-by-hour.png)

![q12-order-size-distribution](screenshots/q12-order-size-distribution.png)

![q13-product-reorder-bubble](screenshots/q13-product-reorder-bubble.png)

![q14-user-frequency-scatter](screenshots/q14-user-frequency-scatter.png)

![q15-days-since-prior-trend](screenshots/q15-days-since-prior-trend.png)

![q16-loyalty-curve](screenshots/q16-loyalty-curve.png)

![q17-department-day-pivot](screenshots/q17-department-day-pivot.png)

![q18-cart-size-hour-day](screenshots/q18-cart-size-hour-day.png)

![q19-product-rank-table](screenshots/q19-product-rank-table.png)

![q20-top-users-table](screenshots/q20-top-users-table.png)

Placeholder dashboard akhir:

![final-metabase-dashboard](screenshots/final-metabase-dashboard.png)

---

### Step 7 — Matikan Infrastruktur

```bash
docker-compose down
```

Jika ingin menghapus volume dan memulai ulang dari nol:

```bash
docker-compose down -v
```

---

## ⚙️ Konfigurasi

Environment variable yang dapat digunakan oleh script:

| Variable | Default | Keterangan |
| --- | --- | --- |
| `ORDERS_API_URL` | `http://96.9.212.102:8000/orders` | Endpoint sumber data orders |
| `ORDERS_RAW_PATH` | `/opt/airflow/data_lake/orders/raw` | Lokasi staging raw Parquet/CSV |
| `ORDERS_OUTPUT_FORMAT` | `parquet` | Format staging, dapat memakai `parquet` atau `csv` |
| `ORDERS_PROCESSED_PATH` | `/opt/airflow/data_lake/orders/processed/cleaned_orders` | Lokasi hasil proses Spark |
| `CLICKHOUSE_HOST` | `clickhouse-server` | Host ClickHouse di jaringan Docker |
| `CLICKHOUSE_NATIVE_PORT` | `9000` | Port native ClickHouse untuk DDL |
| `CLICKHOUSE_HTTP_PORT` | `8123` | Port HTTP/JDBC ClickHouse |
| `CLICKHOUSE_USER` | `admin` | User ClickHouse |
| `CLICKHOUSE_PASSWORD` | `rahasia` | Password ClickHouse |
| `CLICKHOUSE_DATABASE` | `analytics` | Database target |
| `CLICKHOUSE_TABLE` | `orders_raw` | Tabel target untuk insert |
| `CLICKHOUSE_JDBC_PACKAGE` | `ru.yandex.clickhouse:clickhouse-jdbc:0.3.2` | Driver JDBC untuk Spark |

---

## 🔐 Layanan

| Layanan | URL | Username | Password |
|---------|-----|----------|----------|
| Apache Airflow | http://localhost:8080 | `admin` | `admin` |
| Metabase | http://localhost:3000 | *(buat saat setup)* | — |
| ClickHouse HTTP | http://localhost:8123 | `admin` | `rahasia` |
| ClickHouse TCP | `localhost:9000` | `admin` | `rahasia` |

---

## Anggota Kelompok

[isi sesuai kelompok]

---

## Lisensi

Proyek ini dibuat untuk kebutuhan pembelajaran Data Engineering & Big Data Analytics. Gunakan sesuai ketentuan lisensi dari repositori referensi dan aturan kelas atau institusi terkait.
