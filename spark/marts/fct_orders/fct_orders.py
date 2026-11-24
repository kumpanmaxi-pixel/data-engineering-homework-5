import argparse
import os
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
BUCKET = "de-course"
SOURCE_PATH = f"s3a://{BUCKET}/intermediate/int_orders_with_payments"
RESULT_PATH = f"s3a://{BUCKET}/marts/fct_orders"

spark = (
    SparkSession.builder
    .config("spark.hadoop.fs.s3a.endpoint", os.environ.get("MINIO_ENDPOINT"))
    .config("spark.hadoop.fs.s3a.access.key", os.environ.get("MINIO_ROOT_USER"))
    .config("spark.hadoop.fs.s3a.secret.key", os.environ.get("MINIO_ROOT_PASSWORD"))
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

parser = argparse.ArgumentParser()
parser.add_argument("--load-date", required=True)
args = parser.parse_args()

load_date = datetime.strptime(args.load_date, "%Y-%m-%d").date()
start_date = load_date - timedelta(days=3)
df_orders = (
    spark.read.parquet(SOURCE_PATH)
    .filter(
        (F.col("order_date") >= F.lit(str(start_date))) &
        (F.col("order_date") < F.lit(str(load_date)))
    )
)

df_fct_orders = (
    df_orders
    .filter(~F.col("is_test_order"))
    .select(
        "order_id",
        "customer_id",
        "order_date",
        "ordered_at",
        "order_status",
        "order_amount",
        "paid_amount",
        "refunded_amount",
        "net_paid_amount",
        "successful_payments_count",
        "first_paid_at",
        "is_confirmed",
    )
)
(
    df_fct_orders.write
    .format("parquet")
    .mode("overwrite")
    .partitionBy("order_date")
    .save(RESULT_PATH)
)

print(f"Сохранено: {RESULT_PATH}")

spark.stop()
