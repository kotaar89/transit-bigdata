"""bronze/vehicle_positions -> silver/arrivals (+ загрузка в ClickHouse).

  spark-submit jobs/build_arrivals.py --date 2026-11-17   # один день (так вызывает Airflow)
  spark-submit jobs/build_arrivals.py --all               # вся история
"""
import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import functions as F  # noqa: E402

from common.session import LAKE, ch_exec, ch_write, get_spark  # noqa: E402
from common.transforms import enrich_arrivals, load_static, stop_events  # noqa: E402


def main():
    ap = argparse.ArgumentParser()
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--date")
    g.add_argument("--all", action="store_true")
    a = ap.parse_args()

    spark = get_spark("build_arrivals", 32)
    sched = spark.read.parquet(f"{LAKE}/silver/gtfs/sched")
    pos = spark.read.parquet(f"{LAKE}/bronze/vehicle_positions")
    try:  # отметки, пришедшие через Kafka (пишет stream_processing.py)
        pos = pos.unionByName(spark.read.parquet(f"{LAKE}/bronze/vehicle_positions_stream"))
    except Exception:
        pass
    if a.date:
        pos = pos.filter(F.col("date") == a.date)

    # контроль качества: выбрасываем битые отметки
    pos = pos.filter(F.col("trip_id").isNotNull() & F.col("timestamp").isNotNull()
                     & F.col("latitude").between(54, 57) & F.col("longitude").between(36, 39))
    arrivals = enrich_arrivals(stop_events(pos), sched).filter(F.abs("delay_sec") < 3 * 3600).cache()

    (arrivals.repartition("service_date").write.mode("overwrite").partitionBy("service_date")
     .parquet(f"{LAKE}/silver/arrivals"))

    dates = [r.service_date for r in arrivals.select("service_date").distinct().collect()]
    for d in dates:  # идемпотентность: перезаписываем партиции целиком
        ch_exec(f"ALTER TABLE arrivals DROP PARTITION '{d}'")
    ch_write(arrivals.drop("stop_lat", "stop_lon"), "arrivals")

    arrivals.groupBy("service_date").agg(
        F.count("*").alias("arrivals"),
        F.round(F.avg("delay_sec") / 60, 2).alias("avg_delay_min"),
        F.round(F.avg(F.col("delay_sec").between(-60, 180).cast("int")), 3).alias("on_time_share"),
    ).orderBy("service_date").show(100)


if __name__ == "__main__":
    main()
