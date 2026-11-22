"""Потоковая обработка: Kafka -> озеро + ClickHouse + онлайн-прогноз задержек.

Четыре потоковых запроса в одном приложении:
  1. bronze:   все GPS-отметки как есть -> s3a://lake/bronze/vehicle_positions_stream (Parquet)
  2. arrivals: прибытия на остановки (дедупликация, расчёт задержки) -> ClickHouse arrivals,
               там же онлайн-прогноз задержки на 3 остановки вперёд -> ClickHouse predictions
  3. agg5m:    средняя/90-й перцентиль задержки по маршрутам в окнах 5 минут -> ClickHouse route_delay_5min
  4. weather:  погода -> ClickHouse weather_hourly
"""
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.ml import PipelineModel  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from common.features import build_features  # noqa: E402
from common.schemas import VEHICLE_POSITION, WEATHER  # noqa: E402
from common.session import HORIZON, KAFKA, LAKE, ch_read, ch_write, get_spark  # noqa: E402
from common.transforms import enrich_arrivals  # noqa: E402

CKPT = f"{LAKE}/checkpoints"
MODEL_PATH = f"{LAKE}/models/delay_gbt/latest"
TRIGGER = os.environ.get("STREAM_TRIGGER", "20 seconds")

spark = get_spark("stream_processing", 8)
sched = spark.read.parquet(f"{LAKE}/silver/gtfs/sched").cache()
sched.count()


def kafka_stream(topic):
    return (spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", KAFKA)
            .option("subscribe", topic)
            .option("startingOffsets", "earliest")
            .option("failOnDataLoss", "false")
            .option("maxOffsetsPerTrigger", 200_000)
            .load())


pos = (kafka_stream("vehicle_positions")
       .select(F.from_json(F.col("value").cast("string"), VEHICLE_POSITION).alias("m")).select("m.*")
       .filter(F.col("trip_id").isNotNull())
       .withColumn("date", F.date_format(F.to_date("start_date", "yyyyMMdd"), "yyyy-MM-dd"))
       .withColumn("event_time", F.timestamp_seconds("timestamp")))

# ---------- 1. bronze ----------
q_bronze = (pos.drop("event_time").writeStream.format("parquet")
            .option("path", f"{LAKE}/bronze/vehicle_positions_stream")
            .option("checkpointLocation", f"{CKPT}/bronze")
            .partitionBy("date").trigger(processingTime="1 minute").queryName("bronze").start())

# ---------- 2. arrivals + прогноз ----------
events = (pos.filter(F.col("current_status") == "STOPPED_AT")
          .withWatermark("event_time", "10 minutes")
          .dropDuplicatesWithinWatermark(["trip_id", "start_date", "current_stop_sequence"])
          .select("trip_id", "start_date", F.col("current_stop_sequence").alias("stop_sequence"),
                  F.col("timestamp").alias("actual_epoch"), "vehicle_id", "event_time"))
arrivals = enrich_arrivals(events, sched, keep=("event_time",))

_state = {"model": None, "hist": None, "loaded_at": 0.0}


def _load_model():
    """Перечитываем модель раз в 10 минут: Airflow может переобучить её в любой момент."""
    if time.time() - _state["loaded_at"] < 600 and _state["model"] is not None:
        return _state["model"], _state["hist"]
    try:
        _state["model"] = PipelineModel.load(MODEL_PATH)
        _state["hist"] = spark.read.parquet(f"{LAKE}/gold/hist_inc_ahead").cache()
        _state["loaded_at"] = time.time()
        print("[predict] модель загружена", flush=True)
    except Exception as e:  # модели ещё нет — прогноз пропускаем
        print(f"[predict] модели пока нет ({type(e).__name__}), прогноз пропущен", flush=True)
        _state["loaded_at"] = time.time()
    return _state["model"], _state["hist"]


def process_arrivals(batch, batch_id):
    batch = batch.drop("event_time").persist()
    if batch.isEmpty():
        batch.unpersist()
        return
    ch_write(batch.drop("stop_lat", "stop_lon"), "arrivals")
    model, hist = _load_model()
    if model is not None:
        mx = batch.agg(F.max("actual_epoch")).first()[0]
        dates = ",".join(f"'{r.service_date}'" for r in batch.select("service_date").distinct().collect())
        recent = ch_read(spark, f"""
            SELECT toString(service_date) AS service_date, route_id, direction_id, trip_id, stop_id, zone,
                   stop_sequence, sched_sec, scheduled_epoch, actual_epoch, delay_sec, hour, dow
            FROM arrivals WHERE service_date IN ({dates}) AND actual_epoch >= {mx - 5400}""")
        weather = ch_read(spark, f"""SELECT toString(date) AS date, hour, precip_mm, temp_c, snow
                                     FROM weather_hourly FINAL WHERE date IN ({dates})""")
        feats, _ = build_features(recent, sched, weather, hist, training=False)
        keys = batch.select("service_date", "trip_id", "stop_sequence")
        feats = feats.join(keys, ["service_date", "trip_id", "stop_sequence"])
        pred = model.transform(feats).select(
            "service_date", "route_id", "direction_id", "trip_id",
            F.col("actual_epoch").alias("pred_epoch"),
            F.col("stop_sequence").alias("from_stop_sequence"),
            (F.col("stop_sequence") + HORIZON).alias("target_stop_sequence"),
            "target_stop_id",
            F.col("delay_sec").alias("current_delay_sec"),
            F.round(F.col("prediction") * 60).cast("int").alias("predicted_delay_sec"),
            (F.col("target_sched_epoch") + F.round(F.col("prediction") * 60)).cast("long")
            .alias("predicted_arrival_epoch"),
        )
        ch_write(pred, "predictions")
    batch.unpersist()


q_arr = (arrivals.writeStream.foreachBatch(process_arrivals)
         .option("checkpointLocation", f"{CKPT}/arrivals").trigger(processingTime=TRIGGER)
         .queryName("arrivals").start())

# ---------- 3. агрегаты по 5 минут ----------
agg = (arrivals.groupBy(F.window("event_time", "5 minutes"), "route_id", "route_short_name", "direction_id")
       .agg(F.count("*").alias("n_arrivals"),
            (F.avg("delay_sec") / 60).alias("avg_delay_min"),
            (F.percentile_approx("delay_sec", 0.9) / 60).alias("p90_delay_min"),
            F.avg(F.col("delay_sec").between(-60, 180).cast("int")).alias("on_time_share"))
       .select(F.unix_timestamp(F.col("window.start")).alias("window_start_epoch"),
               "route_id", "route_short_name", "direction_id", "n_arrivals", "avg_delay_min",
               "p90_delay_min", "on_time_share"))
q_agg = (agg.writeStream.outputMode("append")
         .foreachBatch(lambda df, _id: ch_write(df, "route_delay_5min"))
         .option("checkpointLocation", f"{CKPT}/agg5m").trigger(processingTime=TRIGGER)
         .queryName("agg5m").start())

# ---------- 4. погода ----------
weather_s = (kafka_stream("weather")
             .select(F.from_json(F.col("value").cast("string"), WEATHER).alias("m")).select("m.*")
             .select("date", "hour", "precip_mm", "temp_c", "snow"))
q_weather = (weather_s.writeStream.foreachBatch(lambda df, _id: ch_write(df, "weather_hourly"))
             .option("checkpointLocation", f"{CKPT}/weather").trigger(processingTime="1 minute")
             .queryName("weather").start())

print("Стриминг запущен:", [q.name for q in spark.streams.active], flush=True)
spark.streams.awaitAnyTermination()
