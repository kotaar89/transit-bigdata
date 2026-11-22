"""Обучение модели прогноза задержки (Spark MLlib GBTRegressor) и сравнение с базовыми подходами.

  spark-submit jobs/train_model.py --days 21 --test-days 3 --sample 0.5
Результаты:
  s3a://lake/models/delay_gbt/latest      — PipelineModel
  s3a://lake/gold/hist_inc_ahead          — историческая «норма» (нужна стриму)
  ClickHouse transit.model_metrics        — метрики каждого запуска
"""
import argparse
import json
import os
import sys
import time
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import functions as F  # noqa: E402

from common.features import FEATURES, build_features, make_pipeline  # noqa: E402
from common.session import HORIZON, LAKE, ch_read, ch_write, get_spark  # noqa: E402


def evaluate(df, pred_col):
    e = F.abs(F.col("target") - F.col(pred_col))
    r = df.select(F.avg(e).alias("mae"), F.sqrt(F.avg(e * e)).alias("rmse"),
                  F.avg((e <= 1).cast("int")).alias("w1"), F.avg((e <= 2).cast("int")).alias("w2")).first()
    return {"mae_min": round(r.mae, 3), "rmse_min": round(r.rmse, 3),
            "within_1min": round(r.w1, 3), "within_2min": round(r.w2, 3)}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=21, help="сколько последних дней брать")
    ap.add_argument("--test-days", type=int, default=3)
    ap.add_argument("--sample", type=float, default=0.5, help="доля обучающей выборки (ускоряет локально)")
    ap.add_argument("--max-iter", type=int, default=80)
    a = ap.parse_args()

    spark = get_spark("train_delay_model", 32)
    arr = spark.read.parquet(f"{LAKE}/silver/arrivals")
    dates = sorted(r.service_date for r in arr.select("service_date").distinct().collect())[-a.days:]
    test_dates, train_dates = dates[-a.test_days:], dates[:-a.test_days]
    print("train:", train_dates[0], "…", train_dates[-1], "| test:", test_dates)
    # погода из ClickHouse: там и история, и дни, пришедшие через стриминг
    weather = ch_read(spark, "SELECT toString(date) AS date, hour, precip_mm, temp_c, snow FROM weather_hourly FINAL")
    sched = spark.read.parquet(f"{LAKE}/silver/gtfs/sched")

    train, hist = build_features(arr.filter(F.col("service_date").isin(train_dates)), sched, weather)
    hist = hist.cache()
    test, _ = build_features(arr.filter(F.col("service_date").isin(test_dates)), sched, weather, hist)
    train = train.sample(a.sample, seed=1) if a.sample < 1 else train
    train, test = train.cache(), test.cache()
    n_train, n_test = train.count(), test.count()
    print(f"train rows={n_train:,} test rows={n_test:,}")

    t0 = time.time()
    model = make_pipeline(a.max_iter).fit(train)
    fit_s = time.time() - t0
    pred = model.transform(test).withColumn("zero", F.lit(0.0)) \
        .withColumn("hist_baseline", F.col("delay_min") + F.col("hist_inc_ahead")).cache()

    results = {
        "schedule": evaluate(pred, "zero"),
        "persistence": evaluate(pred, "delay_min"),
        "persistence_plus_hist": evaluate(pred, "hist_baseline"),
        "gbt": evaluate(pred, "prediction"),
    }
    for k, v in results.items():
        print(f"{k:24s} {v}")

    gbt = model.stages[-1]
    imp = sorted(zip(FEATURES, gbt.featureImportances.toArray().tolist()), key=lambda x: -x[1])
    print("feature importance:", [(f, round(v, 3)) for f, v in imp])

    model.write().overwrite().save(f"{LAKE}/models/delay_gbt/latest")
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    model.write().overwrite().save(f"{LAKE}/models/delay_gbt/{run_id}")
    hist.write.mode("overwrite").parquet(f"{LAKE}/gold/hist_inc_ahead")

    rows = [(run_id, k, v["mae_min"], v["rmse_min"], v["within_1min"], v["within_2min"], n_train, n_test,
             float(round(fit_s, 1)), HORIZON, test_dates[0], test_dates[-1]) for k, v in results.items()]
    cols = ["run_id", "model", "mae_min", "rmse_min", "within_1min", "within_2min", "train_rows", "test_rows",
            "fit_seconds", "horizon_stops", "test_from", "test_to"]
    ch_write(spark.createDataFrame(rows, cols), "model_metrics")
    ch_write(spark.createDataFrame([(run_id, f, float(v)) for f, v in imp], ["run_id", "feature", "importance"]),
             "feature_importance")

    # ошибка по часам — для дашборда
    by_hour = (pred.groupBy(F.least(F.floor("hour_f"), F.lit(23)).cast("int").alias("hour"))
               .agg(F.avg(F.abs(F.col("target") - F.col("prediction"))).alias("mae_gbt"),
                    F.avg(F.abs(F.col("target") - F.col("delay_min"))).alias("mae_persistence"))
               .withColumn("run_id", F.lit(run_id))
               .select("run_id", "hour", "mae_gbt", "mae_persistence"))
    ch_write(by_hour, "model_error_by_hour")
    print(json.dumps({"run_id": run_id, "fit_seconds": round(fit_s, 1), **results}, ensure_ascii=False))


if __name__ == "__main__":
    main()
