"""GTFS-справочники -> озеро (silver/gtfs) и ClickHouse (dim_stops, dim_routes)."""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import functions as F  # noqa: E402

from common.session import LAKE, ch_exec, ch_write, get_spark  # noqa: E402
from common.transforms import load_static  # noqa: E402


def main():
    spark = get_spark("ingest_static", 4)
    st = load_static(spark)
    for name in ("stop_times", "stops", "routes", "trips", "sched"):
        st[name].write.mode("overwrite").parquet(f"{LAKE}/silver/gtfs/{name}")
    stops = (st["sched"].select("stop_id", "stop_name", "zone", "stop_lat", "stop_lon", "route_id",
                                "route_short_name", "direction_id", "stop_sequence").distinct())
    ch_exec("TRUNCATE TABLE dim_stops")
    ch_write(stops, "dim_stops")
    ch_exec("TRUNCATE TABLE dim_routes")
    ch_write(st["routes"], "dim_routes")
    print(f"stops={stops.count()} routes={st['routes'].count()} stop_times={st['stop_times'].count()}")


if __name__ == "__main__":
    main()
