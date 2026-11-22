"""Признаки для модели прогноза задержки на HORIZON остановок вперёд.

Каждая строка = «ТС только что прибыло на остановку k». Цель = задержка на остановке k+H.
Та же логика продублирована на pandas в scripts/local_validation.py.
"""
from __future__ import annotations

from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.regression import GBTRegressor
from pyspark.sql import DataFrame
from pyspark.sql import Window
from pyspark.sql import functions as F

from .session import HORIZON as H
from .session import SLOT_SEC

FEATURES = ["delay_min", "trend3", "sched_ahead_min", "n_center_ahead", "hour_f", "dow", "is_weekend",
            "precip_mm", "temp_c", "snow", "hist_inc_ahead", "ahead_inc_prev_slot", "has_ahead",
            "route_idx", "stop_sequence", "direction_id"]
HIST_KEYS = ["route_id", "direction_id", "stop_sequence", "hb", "is_weekend"]


def schedule_ahead(sched: DataFrame) -> DataFrame:
    """Что известно заранее из расписания про следующие H остановок (доступно и при обучении, и онлайн)."""
    w = Window.partitionBy("trip_id").orderBy("stop_sequence")
    return (sched
            .withColumn("target_stop_id", F.lead("stop_id", H).over(w))
            .withColumn("target_sched_sec", F.lead("sched_sec", H).over(w))
            .withColumn("n_center_ahead", sum(F.when(F.lead("zone", j).over(w) == "center", 1).otherwise(0)
                                              for j in range(1, H + 1)))
            .filter(F.col("target_sched_sec").isNotNull())
            .select("trip_id", "stop_sequence", "target_stop_id", "target_sched_sec", "n_center_ahead"))


def build_features(arr: DataFrame, sched: DataFrame, weather: DataFrame, hist: DataFrame | None = None,
                   training: bool = True) -> tuple[DataFrame, DataFrame]:
    """arr — прибытия (схема `arrivals`), sched — silver/gtfs/sched, weather — date, hour, precip_mm, temp_c, snow.

    training=True: строки с известной целью (задержкой через H остановок).
    training=False: все строки, для которых есть остановка k+H в расписании (онлайн-прогноз).
    """
    w = Window.partitionBy("service_date", "trip_id").orderBy("stop_sequence")
    lead_ok = F.lead("stop_sequence", H).over(w) == F.col("stop_sequence") + H
    df = (arr.join(F.broadcast(schedule_ahead(sched)), ["trip_id", "stop_sequence"], "left")
          .withColumn("delay_min", F.col("delay_sec") / 60.0)
          .withColumn("actual_sec", F.col("actual_epoch") - (F.col("scheduled_epoch") - F.col("sched_sec")))
          .withColumn("seg_inc", F.col("delay_min") - F.lag("delay_min", 1).over(w))
          .withColumn("trend3", F.coalesce(F.col("delay_min") - F.lag("delay_min", 3).over(w), F.lit(0.0)))
          .withColumn("target", F.when(lead_ok, F.lead("delay_min", H).over(w)))
          .withColumn("target_sched_epoch", F.col("scheduled_epoch") + F.col("target_sched_sec") - F.col("sched_sec"))
          .withColumn("sched_ahead_min", (F.col("target_sched_sec") - F.col("sched_sec")) / 60.0)
          .withColumn("hour_f", F.col("actual_sec") / 3600.0)
          .withColumn("hb", F.when(F.col("hour") < 7, 0).when(F.col("hour") < 10, 1).when(F.col("hour") < 16, 2)
                      .when(F.col("hour") < 20, 3).otherwise(4))
          .withColumn("is_weekend", (F.col("dow") >= 5).cast("int"))
          .withColumn("slot", F.floor(F.col("actual_sec") / SLOT_SEC).cast("int"))
          .withColumn("whour", F.least(F.floor(F.col("actual_sec") / 3600), F.lit(23)).cast("int")))

    # «что происходит впереди»: средний прирост задержки на следующих H перегонах
    # у машин, прошедших их в ПРЕДЫДУЩЕМ слоте (никакой утечки из будущего)
    seg = (df.filter(F.col("seg_inc").isNotNull())
           .groupBy(F.col("service_date").alias("s_date"), F.col("route_id").alias("s_route"),
                    F.col("direction_id").alias("s_dir"), F.col("stop_sequence").alias("s_seq"),
                    F.col("slot").alias("s_slot"))
           .agg(F.avg("seg_inc").alias("seg_inc_mean")))
    keys = ["service_date", "trip_id", "stop_sequence"]
    probe = df.select(*keys, "route_id", "direction_id", "slot",
                      F.explode(F.array(*[F.lit(j) for j in range(1, H + 1)])).alias("j"))
    cond = ((F.col("s_date") == F.col("service_date")) & (F.col("s_route") == F.col("route_id"))
            & (F.col("s_dir") == F.col("direction_id")) & (F.col("s_seq") == F.col("stop_sequence") + F.col("j"))
            & (F.col("s_slot") == F.col("slot") - 1))
    ahead = (probe.join(seg, cond).groupBy(*keys)
             .agg((F.avg("seg_inc_mean") * H).alias("ahead_inc_prev_slot")))
    df = (df.join(ahead, keys, "left")
          .withColumn("has_ahead", F.col("ahead_inc_prev_slot").isNotNull().cast("int"))
          .fillna({"ahead_inc_prev_slot": 0.0}))

    wthr = weather.select(F.col("date").alias("service_date"), F.col("hour").alias("whour"),
                          "precip_mm", "temp_c", "snow")
    df = df.join(F.broadcast(wthr), ["service_date", "whour"], "left").fillna(
        {"precip_mm": 0.0, "temp_c": 0.0, "snow": 0})

    if hist is None:
        hist = (df.filter(F.col("target").isNotNull())
                .groupBy(*HIST_KEYS)
                .agg(F.avg(F.col("target") - F.col("delay_min")).alias("hist_inc_ahead")))
    df = df.join(F.broadcast(hist), HIST_KEYS, "left").fillna({"hist_inc_ahead": 0.0})
    df = df.filter(F.col("target_sched_sec").isNotNull())
    if training:
        df = df.filter(F.col("target").isNotNull())
    return df, hist


def make_pipeline(max_iter: int = 80, max_depth: int = 7) -> Pipeline:
    idx = StringIndexer(inputCol="route_id", outputCol="route_idx", handleInvalid="keep")
    vec = VectorAssembler(inputCols=FEATURES, outputCol="features", handleInvalid="keep")
    gbt = GBTRegressor(featuresCol="features", labelCol="target", predictionCol="prediction",
                       maxIter=max_iter, maxDepth=max_depth, stepSize=0.1, maxBins=64,
                       subsamplingRate=0.8, seed=42)
    return Pipeline(stages=[idx, vec, gbt])
