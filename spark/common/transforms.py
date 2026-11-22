"""Преобразования, общие для батча и стриминга: GTFS-справочники и расчёт прибытий/задержек."""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .session import GTFS_PATH


def _hms_to_sec(col):
    p = F.split(col, ":")
    return p[0].cast("int") * 3600 + p[1].cast("int") * 60 + p[2].cast("int")


def load_static(spark: SparkSession, gtfs_path: str = GTFS_PATH) -> dict[str, DataFrame]:
    rd = lambda name: spark.read.option("header", True).csv(f"{gtfs_path}/{name}")
    stop_times = (rd("stop_times.txt")
                  .select("trip_id", "stop_id", F.col("stop_sequence").cast("int").alias("stop_sequence"),
                          _hms_to_sec(F.col("arrival_time")).alias("sched_sec")))
    stops = rd("stops.txt").select("stop_id", "stop_name", F.col("stop_lat").cast("double").alias("stop_lat"),
                                   F.col("stop_lon").cast("double").alias("stop_lon"),
                                   F.col("zone_id").alias("zone"))
    routes = rd("routes.txt").select("route_id", "route_short_name", "route_long_name",
                                     F.col("route_type").cast("int").alias("route_type"))
    trips = rd("trips.txt").select("trip_id", "route_id", F.col("direction_id").cast("int").alias("direction_id"))
    sched = (stop_times.join(stops, "stop_id").join(trips, "trip_id").join(routes, "route_id")
             .select("trip_id", "route_id", "route_short_name", "direction_id", "stop_sequence", "stop_id",
                     "stop_name", "zone", "stop_lat", "stop_lon", "sched_sec"))
    return {"stop_times": stop_times, "stops": stops, "routes": routes, "trips": trips, "sched": sched}


def stop_events(positions: DataFrame) -> DataFrame:
    """Первая отметка STOPPED_AT на каждой остановке рейса = фактическое прибытие."""
    return (positions.filter(F.col("current_status") == "STOPPED_AT")
            .groupBy("trip_id", "start_date", F.col("current_stop_sequence").alias("stop_sequence"))
            .agg(F.min("timestamp").alias("actual_epoch"), F.first("vehicle_id").alias("vehicle_id")))


def enrich_arrivals(events: DataFrame, sched: DataFrame, keep: tuple = ()) -> DataFrame:
    """События прибытия + плановое расписание -> задержка. Схема совпадает с таблицей ClickHouse `arrivals`."""
    d = F.to_date(F.col("start_date"), "yyyyMMdd")
    midnight = F.unix_timestamp(F.to_timestamp(d))  # полночь по Europe/Moscow (session timeZone)
    df = events.join(F.broadcast(sched), ["trip_id", "stop_sequence"])
    return df.select(
        F.date_format(d, "yyyy-MM-dd").alias("service_date"),
        "route_id", "route_short_name", "direction_id", "trip_id", "vehicle_id", "stop_id", "stop_name", "zone",
        "stop_sequence", "stop_lat", "stop_lon", "sched_sec",
        (midnight + F.col("sched_sec")).cast("long").alias("scheduled_epoch"),
        F.col("actual_epoch").cast("long").alias("actual_epoch"),
        (F.col("actual_epoch") - midnight - F.col("sched_sec")).cast("int").alias("delay_sec"),
        F.least(F.floor(F.col("sched_sec") / 3600), F.lit(23)).cast("int").alias("hour"),
        ((F.dayofweek(d) + 5) % 7).cast("int").alias("dow"),  # 0 = понедельник
        *keep,
    )
