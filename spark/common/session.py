"""Общие настройки: SparkSession с доступом к S3 (MinIO / VK Object Storage) и ClickHouse."""
from __future__ import annotations

import os
import urllib.parse
import urllib.request

from pyspark.sql import SparkSession

S3_ENDPOINT = os.environ.get("S3_ENDPOINT", "http://minio:9000")
S3_ACCESS_KEY = os.environ.get("S3_ACCESS_KEY", "minioadmin")
S3_SECRET_KEY = os.environ.get("S3_SECRET_KEY", "minioadmin")
LAKE = os.environ.get("LAKE_PATH", "s3a://lake")
GTFS_PATH = os.environ.get("GTFS_PATH", "file:///data/gtfs")
RAW_PATH = os.environ.get("RAW_PATH", "file:///data/raw")
KAFKA = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")

CH_HOST = os.environ.get("CLICKHOUSE_HOST", "clickhouse")
CH_HTTP_PORT = os.environ.get("CLICKHOUSE_HTTP_PORT", "8123")
CH_DB = os.environ.get("CLICKHOUSE_DB", "transit")
CH_USER = os.environ.get("CLICKHOUSE_USER", "transit")
CH_PASSWORD = os.environ.get("CLICKHOUSE_PASSWORD", "transit")
CH_JDBC_URL = f"jdbc:clickhouse://{CH_HOST}:{CH_HTTP_PORT}/{CH_DB}"

HORIZON = 3          # прогноз на 3 остановки вперёд
SLOT_SEC = 15 * 60   # слот для признака «что впереди»
TZ = "Europe/Moscow"


def get_spark(app: str, shuffle_partitions: int = 16) -> SparkSession:
    b = (SparkSession.builder.appName(app)
         .config("spark.sql.session.timeZone", TZ)
         .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
         .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
         # партиции date=2026-11-16 читаем как строки, а не как DateType — так проще джойнить
         .config("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
         .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
         .config("spark.hadoop.fs.s3a.access.key", S3_ACCESS_KEY)
         .config("spark.hadoop.fs.s3a.secret.key", S3_SECRET_KEY)
         .config("spark.hadoop.fs.s3a.path.style.access", "true")
         .config("spark.hadoop.fs.s3a.connection.ssl.enabled", str(S3_ENDPOINT.startswith("https")).lower())
         .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
         .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                 "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"))
    master = os.environ.get("SPARK_MASTER")
    if master:
        b = b.master(master)
    spark = b.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ---------- ClickHouse ----------
def ch_jdbc_options(table: str) -> dict:
    return {
        "url": CH_JDBC_URL,
        "driver": "com.clickhouse.jdbc.ClickHouseDriver",
        "dbtable": table,
        "user": CH_USER,
        "password": CH_PASSWORD,
        "batchsize": "50000",
        "isolationLevel": "NONE",
        "socket_timeout": "300000",
    }


DATE_COLUMNS = ("service_date", "date")


def ch_write(df, table: str):
    """Дозапись DataFrame в таблицу ClickHouse. Строковые даты приводим к DateType."""
    from pyspark.sql import functions as F
    from pyspark.sql.types import StringType
    for f in df.schema.fields:
        if f.name in DATE_COLUMNS and isinstance(f.dataType, StringType):
            df = df.withColumn(f.name, F.to_date(F.col(f.name)))
    df.write.format("jdbc").options(**ch_jdbc_options(table)).mode("append").save()


def ch_read(spark, query: str):
    return (spark.read.format("jdbc").options(**ch_jdbc_options(f"({query}) AS q")).load())


def ch_exec(sql: str) -> str:
    """Выполнить DDL/DML в ClickHouse по HTTP (для DROP PARTITION и т.п.)."""
    url = f"http://{CH_HOST}:{CH_HTTP_PORT}/?" + urllib.parse.urlencode(
        {"database": CH_DB, "user": CH_USER, "password": CH_PASSWORD})
    req = urllib.request.Request(url, data=sql.encode("utf-8"), method="POST")
    with urllib.request.urlopen(req, timeout=120) as r:
        return r.read().decode()
