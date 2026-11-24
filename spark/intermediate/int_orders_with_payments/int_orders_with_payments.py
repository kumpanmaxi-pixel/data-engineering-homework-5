import argparse
import os
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

BUCKET = "de-course"
ORDERS_PATH = f"s3a://{BUCKET}/staging/stg_orders"
PAYMENTS_PATH = f"s3a://{BUCKET}/staging/stg_payments"
RESULT_PATH = f"s3a://{BUCKET}/intermediate/int_orders_with_payments"

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

orders = (
    spark.read.parquet(ORDERS_PATH)
    .filter(
        (F.col("order_date") >= F.lit(str(start_date))) &
        (F.col("order_date") < F.lit(str(load_date)))
    )
)

payments = (
    spark.read.parquet(PAYMENTS_PATH)
    .filter(
        (
            (F.col("paid_dt") >= F.lit(str(start_date))) &
            (F.col("paid_dt") < F.lit(str(load_date)))
        ) |
        F.col("paid_dt").isNull()
    )
)

success_payment = (
    (F.col("payment_status") == "success") &
    (~F.col("is_refund"))
)

refund = F.col("is_refund")

order_payments = (
    payments
    .groupBy("order_id")
    .agg(
        F.count(
            F.when(success_payment, 1)
        ).alias("successful_payments_count"),

        F.sum(
            F.when(success_payment, F.col("amount"))
        ).alias("paid_amount"),

        (
            -F.sum(F.when(refund, F.col("amount")))
        ).alias("refunded_amount"),

        F.min(
            F.when(success_payment, F.col("paid_at"))
        ).alias("first_paid_at"),

        F.count(
            F.when(F.col("payment_status") == "success", 1)
        ).alias("success_operations_count"),

        F.count(
            F.when(F.col("payment_status") == "failed", 1)
        ).alias("failed_operations_count"),

        F.count(
            F.when(F.col("payment_status") == "pending", 1)
        ).alias("pending_operations_count"),
    )
)

joined = orders.alias("o").join(
    order_payments.alias("p"),
    on="order_id",
    how="left"
)

result = (
    joined
    .select(
        F.col("order_id"),
        F.col("customer_id"),
        F.col("ordered_at"),
        F.col("order_date"),
        F.col("order_status"),
        F.col("order_amount"),
        F.col("is_test_order"),

        F.coalesce(
            F.col("successful_payments_count"), F.lit(0)
        ).alias("successful_payments_count"),

        F.coalesce(
            F.col("paid_amount"), F.lit(0)
        ).alias("paid_amount"),

        F.coalesce(
            F.col("refunded_amount"), F.lit(0)
        ).alias("refunded_amount"),

        (
            F.coalesce(F.col("paid_amount"), F.lit(0)) -
            F.coalesce(F.col("refunded_amount"), F.lit(0))
        ).alias("net_paid_amount"),
        F.col("first_paid_at"),

        (
            (F.col("order_status") == "paid") &
            (F.coalesce(
                F.col("successful_payments_count"), F.lit(0)
            ) > 0)
        ).alias("is_confirmed"),

        F.coalesce(
            F.col("success_operations_count"), F.lit(0)
        ).alias("success_operations_count"),

        F.coalesce(
            F.col("failed_operations_count"), F.lit(0)
        ).alias("failed_operations_count"),

        F.coalesce(
            F.col("pending_operations_count"), F.lit(0)
        ).alias("pending_operations_count"),
    )
)

(
    result.write
    .format("parquet")
    .mode("overwrite")
    .partitionBy("order_date")
    .save(RESULT_PATH)
)

print(f"Сохранено: {RESULT_PATH}")

spark.stop()
