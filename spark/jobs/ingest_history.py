"""Сырые исторические данные (JSONL.gz) -> bronze-слой озера в Parquet + погода в ClickHouse.

  spark-submit jobs/ingest_history.py
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import functions as F  # noqa: E402

from common.schemas import VEHICLE_POSITION  # noqa: E402
from common.session import LAKE, RAW_PATH, ch_exec, ch_write, get_spark  # noqa: E402


def main():
    spark = get_spark("ingest_history", 32)
    pos = (spark.read.schema(VEHICLE_POSITION).json(f"{RAW_PATH}/vehicle_positions/*/*.jsonl.gz")
           .withColumn("date", F.date_format(F.to_date("start_date", "yyyyMMdd"), "yyyy-MM-dd")))
    (pos.repartition("date").write.mode("overwrite").partitionBy("date")
     .parquet(f"{LAKE}/bronze/vehicle_positions"))

    weather = (spark.read.option("header", True).option("inferSchema", True)
               .csv(f"{RAW_PATH}/weather_hourly.csv")
               .select(F.col("date").cast("string"), F.col("hour").cast("int"), F.col("precip_mm").cast("double"),
                       F.col("temp_c").cast("double"), F.col("snow").cast("int")))
    weather.write.mode("overwrite").parquet(f"{LAKE}/bronze/weather_hourly")
    ch_exec("TRUNCATE TABLE weather_hourly")
    ch_write(weather, "weather_hourly")

    stats = (spark.read.parquet(f"{LAKE}/bronze/vehicle_positions").groupBy("date").count().orderBy("date"))
    stats.show(100, truncate=False)


if __name__ == "__main__":
    main()
