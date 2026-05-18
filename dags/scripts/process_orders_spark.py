import argparse
import glob
import os
import shutil
from datetime import datetime

from clickhouse_driver import Client
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


RAW_PATH = os.getenv("ORDERS_RAW_PATH", "/opt/airflow/data_lake/orders/raw")
PROCESSED_PATH = os.getenv(
    "ORDERS_PROCESSED_PATH",
    "/opt/airflow/data_lake/orders/processed/cleaned_orders",
)

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse-server")
CLICKHOUSE_NATIVE_PORT = int(os.getenv("CLICKHOUSE_NATIVE_PORT", "9000"))
CLICKHOUSE_HTTP_PORT = os.getenv("CLICKHOUSE_HTTP_PORT", "8123")
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "admin")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "rahasia")
CLICKHOUSE_DATABASE = os.getenv("CLICKHOUSE_DATABASE", "analytics")
CLICKHOUSE_TABLE = os.getenv("CLICKHOUSE_TABLE", "orders_raw")
CLICKHOUSE_JDBC_URL = os.getenv(
    "CLICKHOUSE_JDBC_URL",
    (
        f"jdbc:clickhouse://{CLICKHOUSE_HOST}:"
        f"{CLICKHOUSE_HTTP_PORT}/{CLICKHOUSE_DATABASE}"
    ),
)
CLICKHOUSE_JDBC_DRIVER = os.getenv(
    "CLICKHOUSE_JDBC_DRIVER",
    "ru.yandex.clickhouse.ClickHouseDriver",
)
CLICKHOUSE_JDBC_PACKAGE = os.getenv(
    "CLICKHOUSE_JDBC_PACKAGE",
    "ru.yandex.clickhouse:clickhouse-jdbc:0.3.2",
)

KEY_COLUMNS = [
    "order_id",
    "user_id",
    "order_number",
    "product_id",
    "product_name",
    "add_to_cart_order",
    "reordered",
]

OUTPUT_COLUMNS = [
    "order_id",
    "user_id",
    "order_number",
    "order_dow",
    "order_hour_of_day",
    "days_since_prior_order",
    "eval_set",
    "product_id",
    "product_name",
    "aisle_id",
    "aisle",
    "department_id",
    "department",
    "add_to_cart_order",
    "reordered",
    "ingested_at",
]

RAW_SCHEMA = T.StructType(
    [
        T.StructField("order_id", T.IntegerType(), True),
        T.StructField("user_id", T.IntegerType(), True),
        T.StructField("order_number", T.ShortType(), True),
        T.StructField("order_dow", T.ByteType(), True),
        T.StructField("order_hour_of_day", T.ByteType(), True),
        T.StructField("days_since_prior_order", T.ShortType(), True),
        T.StructField("eval_set", T.StringType(), True),
        T.StructField("product_id", T.IntegerType(), True),
        T.StructField("product_name", T.StringType(), True),
        T.StructField("aisle_id", T.ShortType(), True),
        T.StructField("aisle", T.StringType(), True),
        T.StructField("department_id", T.ShortType(), True),
        T.StructField("department", T.StringType(), True),
        T.StructField("add_to_cart_order", T.ShortType(), True),
        T.StructField("reordered", T.ByteType(), True),
    ]
)


def build_spark(app_name, include_jdbc=False):
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.driver.memory", "1g")
        .config("spark.sql.session.timeZone", "UTC")
    )

    if include_jdbc:
        builder = builder.config("spark.jars.packages", CLICKHOUSE_JDBC_PACKAGE)

    return builder.getOrCreate()


def clean_orders(df_raw):
    typed_df = df_raw.select(
        F.col("order_id").cast(T.IntegerType()).alias("order_id"),
        F.col("user_id").cast(T.IntegerType()).alias("user_id"),
        F.col("order_number").cast(T.ShortType()).alias("order_number"),
        F.col("order_dow").cast(T.ByteType()).alias("order_dow"),
        F.col("order_hour_of_day").cast(T.ByteType()).alias("order_hour_of_day"),
        F.col("days_since_prior_order")
        .cast(T.ShortType())
        .alias("days_since_prior_order"),
        F.col("eval_set").cast(T.StringType()).alias("eval_set"),
        F.col("product_id").cast(T.IntegerType()).alias("product_id"),
        F.col("product_name").cast(T.StringType()).alias("product_name"),
        F.col("aisle_id").cast(T.ShortType()).alias("aisle_id"),
        F.col("aisle").cast(T.StringType()).alias("aisle"),
        F.col("department_id").cast(T.ShortType()).alias("department_id"),
        F.col("department").cast(T.StringType()).alias("department"),
        F.col("add_to_cart_order")
        .cast(T.ShortType())
        .alias("add_to_cart_order"),
        F.col("reordered").cast(T.ByteType()).alias("reordered"),
    )

    return (
        typed_df.dropna(subset=KEY_COLUMNS)
        .dropDuplicates(["order_id", "product_id", "add_to_cart_order"])
        .withColumn(
            "ingested_at",
            F.date_format(F.current_timestamp(), "yyyy-MM-dd HH:mm:ss"),
        )
        .select(OUTPUT_COLUMNS)
    )


def prepare_clickhouse_df(df_clean):
    return df_clean.select(
        "order_id",
        "user_id",
        "order_number",
        "order_dow",
        "order_hour_of_day",
        "days_since_prior_order",
        "eval_set",
        "product_id",
        "product_name",
        "aisle_id",
        "aisle",
        "department_id",
        "department",
        "add_to_cart_order",
        "reordered",
        F.date_format(
            F.col("ingested_at").cast(T.TimestampType()),
            "yyyy-MM-dd HH:mm:ss",
        ).alias("ingested_at"),
    )


def create_clickhouse_objects():
    client = Client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_NATIVE_PORT,
        user=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
    )

    client.execute(f"CREATE DATABASE IF NOT EXISTS {CLICKHOUSE_DATABASE}")
    client.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {CLICKHOUSE_DATABASE}.orders_raw (
            order_id UInt32,
            user_id UInt32,
            order_number UInt16,
            order_dow UInt8,
            order_hour_of_day UInt8,
            days_since_prior_order Nullable(UInt16),
            eval_set String,
            product_id UInt32,
            product_name String,
            aisle_id UInt16,
            aisle String,
            department_id UInt16,
            department String,
            add_to_cart_order UInt16,
            reordered UInt8,
            ingested_at DateTime DEFAULT now()
        ) ENGINE = MergeTree()
        ORDER BY (order_id, product_id)
        """
    )
    client.execute(
        f"""
        CREATE VIEW IF NOT EXISTS {CLICKHOUSE_DATABASE}.orders AS
        SELECT
            order_id,
            user_id,
            order_number,
            order_dow,
            order_hour_of_day,
            days_since_prior_order,
            eval_set,
            product_id,
            product_name,
            aisle_id,
            aisle,
            department_id,
            department,
            add_to_cart_order,
            reordered,
            ingested_at
        FROM {CLICKHOUSE_DATABASE}.orders_raw
        WHERE order_id > 0
            AND user_id > 0
            AND product_id > 0
            AND product_name != ''
        """
    )


def ensure_raw_files_exist():
    files = glob.glob(os.path.join(RAW_PATH, "*.parquet"))
    files.extend(glob.glob(os.path.join(RAW_PATH, "*.csv")))
    if not files:
        raise FileNotFoundError(f"No staged orders files found in {RAW_PATH}")


def read_staged_orders(spark):
    parquet_files = glob.glob(os.path.join(RAW_PATH, "*.parquet"))
    csv_files = glob.glob(os.path.join(RAW_PATH, "*.csv"))
    frames = []

    if parquet_files:
        parquet_uri = f"file://{RAW_PATH.rstrip('/')}/*.parquet"
        frames.append(spark.read.parquet(parquet_uri))

    if csv_files:
        csv_uri = f"file://{RAW_PATH.rstrip('/')}/*.csv"
        csv_df = (
            spark.read.option("header", "true")
            .option("nullValue", "")
            .schema(RAW_SCHEMA)
            .csv(csv_uri)
        )
        frames.append(csv_df)

    if not frames:
        raise FileNotFoundError(f"No staged orders files found in {RAW_PATH}")

    combined_df = frames[0]
    for staged_df in frames[1:]:
        combined_df = combined_df.unionByName(staged_df)

    return combined_df


def process_orders():
    ensure_raw_files_exist()

    spark = build_spark("Orders_Staging_Processing")
    print("Reading staged orders from Data Lake...")
    df_raw = read_staged_orders(spark)

    print("Cleaning and deduplicating orders data...")
    cleaned_df = clean_orders(df_raw)
    cleaned_count = cleaned_df.count()

    processed_uri = f"file://{PROCESSED_PATH.rstrip('/')}"
    cleaned_df.write.mode("overwrite").parquet(processed_uri)

    spark.stop()
    print(f"Saved {cleaned_count} cleaned rows to {PROCESSED_PATH}")


def load_to_clickhouse():
    if not os.path.exists(PROCESSED_PATH):
        raise FileNotFoundError(
            f"No processed Parquet directory found in {PROCESSED_PATH}"
        )

    create_clickhouse_objects()

    spark = build_spark("Orders_ClickHouse_Load", include_jdbc=True)
    processed_uri = f"file://{PROCESSED_PATH.rstrip('/')}"

    print("Reading cleaned orders for ClickHouse load...")
    df_clean = prepare_clickhouse_df(
        spark.read.parquet(processed_uri).select(OUTPUT_COLUMNS)
    )
    row_count = df_clean.count()

    if row_count > 0:
        print(f"Writing {row_count} rows to ClickHouse using JDBC...")
        (
            df_clean.write.format("jdbc")
            .option("url", CLICKHOUSE_JDBC_URL)
            .option("dbtable", CLICKHOUSE_TABLE)
            .option("user", CLICKHOUSE_USER)
            .option("password", CLICKHOUSE_PASSWORD)
            .option("driver", CLICKHOUSE_JDBC_DRIVER)
            .option("batchsize", "10000")
            .mode("append")
            .save()
        )
    else:
        print("Processed dataset is empty. Nothing to load.")

    spark.stop()
    cleanup_staging()
    print("Orders pipeline finished successfully.")


def cleanup_staging():
    print("Cleaning processed staging files...")

    raw_files = glob.glob(os.path.join(RAW_PATH, "*.parquet"))
    raw_files.extend(glob.glob(os.path.join(RAW_PATH, "*.csv")))

    for file_path in raw_files:
        try:
            os.remove(file_path)
        except OSError as exc:
            print(f"Could not remove {file_path}: {exc}")

    if os.path.exists(PROCESSED_PATH):
        shutil.rmtree(PROCESSED_PATH)


def run_spark_analytics():
    started_at = datetime.now().isoformat(timespec="seconds")
    print(f"Starting orders Spark pipeline at {started_at}")
    process_orders()
    load_to_clickhouse()


def parse_args():
    parser = argparse.ArgumentParser(description="Orders Spark pipeline")
    parser.add_argument(
        "--stage",
        action="store_true",
        help="Clean raw staged files and save processed Parquet.",
    )
    parser.add_argument(
        "--load",
        action="store_true",
        help="Load processed Parquet into ClickHouse through JDBC.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()

    if args.stage and args.load:
        run_spark_analytics()
    elif args.stage:
        process_orders()
    elif args.load:
        load_to_clickhouse()
    else:
        run_spark_analytics()
