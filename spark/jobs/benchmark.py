"""Сравнение: pandas на одной машине vs Spark на кластере на одной и той же задаче.

Задача: из сырых GPS-отметок за N дней найти прибытия и посчитать среднюю задержку
по маршруту и часу. Pandas читает JSONL.gz в память, Spark — параллельно.

  spark-submit jobs/benchmark.py --days 1 3 7
Результат печатается и пишется в ClickHouse transit.benchmark.
"""
import argparse
import glob
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import functions as F  # noqa: E402

from common.schemas import VEHICLE_POSITION  # noqa: E402
from common.session import LAKE, RAW_PATH, ch_write, get_spark  # noqa: E402
from common.transforms import enrich_arrivals, stop_events  # noqa: E402


def pandas_run(files, sched_pdf):
    import pandas as pd
    t0 = time.time()
    try:
        df = pd.concat((pd.read_json(f, lines=True, compression="gzip") for f in files), ignore_index=True)
        st = df[df.current_status == "STOPPED_AT"]
        ev = (st.groupby(["trip_id", "start_date", "current_stop_sequence"], as_index=False)
              .timestamp.min().rename(columns={"current_stop_sequence": "stop_sequence"}))
        ev = ev.merge(sched_pdf, on=["trip_id", "stop_sequence"])
        midnight = pd.to_datetime(ev.start_date.astype(str)).dt.tz_localize("Europe/Moscow").astype("int64") // 10**9
        ev["delay"] = ev.timestamp - midnight - ev.sched_sec
        ev["hour"] = (ev.sched_sec // 3600).clip(upper=23)
        ev.groupby(["route_id", "hour"]).delay.mean()
        return round(time.time() - t0, 1), len(df), "ok"
    except MemoryError:
        return round(time.time() - t0, 1), 0, "MemoryError"


def spark_run(spark, paths, sched):
    t0 = time.time()
    pos = spark.read.schema(VEHICLE_POSITION).json(paths)
    arr = enrich_arrivals(stop_events(pos), sched)
    arr.groupBy("route_id", "hour").agg(F.avg("delay_sec")).collect()
    n = pos.count()
    return round(time.time() - t0, 1), n


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, nargs="+", default=[1, 3, 7])
    ap.add_argument("--skip-pandas-over", type=int, default=7, help="не запускать pandas на объёмах больше N дней")
    a = ap.parse_args()
    spark = get_spark("benchmark", 32)
    sched = spark.read.parquet(f"{LAKE}/silver/gtfs/sched").cache()
    sched_pdf = sched.select("trip_id", "stop_sequence", "route_id", "sched_sec").toPandas()
    local_root = RAW_PATH.replace("file://", "")
    all_files = sorted(glob.glob(f"{local_root}/vehicle_positions/*/*.jsonl.gz"))
    rows = []
    for d in a.days:
        files = all_files[-d:]
        size_mb = sum(os.path.getsize(f) for f in files) / 1e6
        s_t, n = spark_run(spark, [f"file://{f}" for f in files], sched)
        if d <= a.skip_pandas_over:
            p_t, _, status = pandas_run(files, sched_pdf)
        else:
            p_t, status = None, "skipped"
        print(f"{d} дн | {n:,} отметок | {size_mb:.0f} МБ gz | pandas {p_t} c ({status}) | spark {s_t} c", flush=True)
        rows.append((d, int(n), float(round(size_mb, 1)), p_t, s_t, status))
    df = spark.createDataFrame(rows, "days int, pings long, size_mb double, pandas_sec double, spark_sec double, "
                                     "pandas_status string")
    df.show()
    ch_write(df, "benchmark")


if __name__ == "__main__":
    main()
