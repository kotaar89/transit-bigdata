"""Локальная проверка модели без кластера (pandas + scikit-learn).

Повторяет логику Spark-конвейера (spark/common/features.py) на «чистых»
прибытиях из симулятора, чтобы:
  * убедиться, что признаки несут сигнал, и оценить метрики заранее;
  * получить цифры и агрегаты для отчёта и демо-дашборда.

  python scripts/local_validation.py --days 14 --out docs/validation
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import date, timedelta

import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingRegressor

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "generator"))
from transit_sim.delay_model import DaySimulator  # noqa: E402
from transit_sim.network import Network  # noqa: E402

H = 3          # горизонт прогноза: на сколько остановок вперёд
SLOT = 15 * 60  # размер слота для признака «что происходит впереди»


def simulate(days: list[date], routes: int, seed: int = 42):
    net = Network(routes, seed)
    stops = net.stops
    recs, weather, incidents = [], [], []
    for d in days:
        sim = DaySimulator(net, d, seed)
        weather += sim.weather
        incidents += [dict(date=d.isoformat(), **i.__dict__) for i in sim.incidents]
        for t in net.trips:
            for a in sim.simulate_trip(t):
                recs.append((d, t.route_id, t.direction_id, t.trip_id, a["stop_sequence"], a["stop_id"],
                             a["sched_sec"], a["actual_sec"]))
    df = pd.DataFrame(recs, columns=["service_date", "route_id", "direction_id", "trip_id", "stop_sequence",
                                     "stop_id", "sched_sec", "actual_sec"])
    df["delay_min"] = (df.actual_sec - df.sched_sec) / 60.0
    df["zone"] = df.stop_id.map(lambda s: stops[s].zone)
    df["dow"] = pd.to_datetime(df.service_date).dt.weekday
    return net, df, pd.DataFrame(weather), pd.DataFrame(incidents)


def hour_bucket(h):
    return np.select([h < 7, h < 10, h < 16, h < 20], [0, 1, 2, 3], 4)


def build_features(df: pd.DataFrame, weather: pd.DataFrame, hist: pd.DataFrame | None):
    df = df.sort_values(["service_date", "trip_id", "stop_sequence"]).copy()
    g = df.groupby(["service_date", "trip_id"], sort=False)
    df["seg_inc"] = df.delay_min - g.delay_min.shift(1)
    df["delay_lag3"] = g.delay_min.shift(3)
    df["target"] = g.delay_min.shift(-H)
    df["sched_ahead_min"] = (g.sched_sec.shift(-H) - df.sched_sec) / 60.0
    df["n_center_ahead"] = sum((g.zone.shift(-j) == "center").astype(int) for j in range(1, H + 1))
    df["trend3"] = (df.delay_min - df.delay_lag3).fillna(0.0)
    df["hour_f"] = df.actual_sec / 3600.0
    df["hb"] = hour_bucket(np.minimum(df.sched_sec // 3600, 23))
    df["is_weekend"] = (df.dow >= 5).astype(int)
    df["slot"] = df.actual_sec // SLOT

    # «что происходит впереди»: средний прирост задержки на следующих H перегонах
    # у машин, прошедших их в предыдущем 15-минутном слоте (утечки из будущего нет)
    seg = (df.dropna(subset=["seg_inc"])
           .groupby(["service_date", "route_id", "direction_id", "stop_sequence", "slot"]).seg_inc.mean())
    ahead = np.zeros(len(df)); cnt = np.zeros(len(df))
    for j in range(1, H + 1):
        key = pd.MultiIndex.from_arrays([df.service_date, df.route_id, df.direction_id,
                                         df.stop_sequence + j, df.slot - 1])
        v = seg.reindex(key).to_numpy()
        ok = ~np.isnan(v)
        ahead[ok] += v[ok]; cnt[ok] += 1
    df["ahead_inc_prev_slot"] = np.where(cnt > 0, ahead / np.maximum(cnt, 1) * H, 0.0)
    df["has_ahead"] = (cnt > 0).astype(int)

    # погода
    w = weather.assign(service_date=pd.to_datetime(weather["date"]).dt.date)
    df["whour"] = np.minimum(23, df.actual_sec // 3600)
    df = df.merge(w[["service_date", "hour", "precip_mm", "temp_c", "snow"]].rename(columns={"hour": "whour"}),
                  on=["service_date", "whour"], how="left")

    # историческая «норма» прироста задержки на следующих H перегонах (target encoding)
    df["inc_ahead"] = df.target - df.delay_min
    if hist is None:
        hist = (df.dropna(subset=["inc_ahead"])
                .groupby(["route_id", "direction_id", "stop_sequence", "hb", "is_weekend"])
                .inc_ahead.mean().rename("hist_inc_ahead").reset_index())
    df = df.merge(hist, on=["route_id", "direction_id", "stop_sequence", "hb", "is_weekend"], how="left")
    df["hist_inc_ahead"] = df.hist_inc_ahead.fillna(0.0)
    df = df.dropna(subset=["target"])
    return df, hist


FEATURES = ["delay_min", "trend3", "sched_ahead_min", "n_center_ahead", "hour_f", "dow", "is_weekend",
            "precip_mm", "temp_c", "snow", "hist_inc_ahead", "ahead_inc_prev_slot", "has_ahead", "route_code", "stop_sequence",
            "direction_id"]


def metrics(y, p):
    e = np.abs(y - p)
    return {"mae_min": round(float(e.mean()), 3), "rmse_min": round(float(np.sqrt(((y - p) ** 2).mean())), 3),
            "within_1min": round(float((e <= 1).mean()), 3), "within_2min": round(float((e <= 2).mean()), 3)}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=14)
    ap.add_argument("--test-days", type=int, default=3)
    ap.add_argument("--end", default="2026-11-17")
    ap.add_argument("--routes", type=int, default=40)
    ap.add_argument("--out", default="docs/validation")
    a = ap.parse_args()
    os.makedirs(a.out, exist_ok=True)
    end = date.fromisoformat(a.end)
    days = [end - timedelta(days=i) for i in range(a.days)][::-1]
    t0 = time.time()
    net, arr, weather, incidents = simulate(days, a.routes)
    print(f"симуляция: {len(arr):,} прибытий за {time.time() - t0:.0f} c")

    test_days = set(days[-a.test_days:])
    tr_raw = arr[~arr.service_date.isin(test_days)]
    te_raw = arr[arr.service_date.isin(test_days)]
    train, hist = build_features(tr_raw, weather, None)
    test, _ = build_features(te_raw, weather, hist)
    codes = {r: i for i, r in enumerate(sorted(arr.route_id.unique()))}
    for d in (train, test):
        d["route_code"] = d.route_id.map(codes)
    print(f"train {len(train):,}  test {len(test):,}  ({time.time() - t0:.0f} c)")

    model = HistGradientBoostingRegressor(max_iter=300, learning_rate=0.08, max_leaf_nodes=63,
                                          categorical_features=[FEATURES.index("route_code")], random_state=0)
    t1 = time.time()
    model.fit(train[FEATURES], train.target)
    fit_s = time.time() - t1
    pred = model.predict(test[FEATURES])
    y = test.target.to_numpy()
    res = {
        "Расписание (задержка = 0)": metrics(y, np.zeros_like(y)),
        "Текущая задержка сохранится": metrics(y, test.delay_min.to_numpy()),
        "Текущая + историческая норма": metrics(y, (test.delay_min + test.hist_inc_ahead).to_numpy()),
        "Gradient Boosting (наша модель)": metrics(y, pred),
    }
    for k, v in res.items():
        print(f"{k:35s} {v}")

    # важность признаков (permutation на подвыборке)
    from sklearn.inspection import permutation_importance
    sub = test.sample(min(60_000, len(test)), random_state=1)
    pi = permutation_importance(model, sub[FEATURES], sub.target, n_repeats=3, random_state=0,
                                scoring="neg_mean_absolute_error")
    imp = sorted(zip(FEATURES, pi.importances_mean), key=lambda x: -x[1])

    # ошибка по часам и по условиям
    test = test.assign(pred=pred, err=np.abs(pred - y), err_persist=np.abs(test.delay_min - y),
                       h=(test.actual_sec // 3600).clip(upper=23))
    by_hour = test.groupby("h")[["err", "err_persist"]].mean().round(3).reset_index()

    # агрегаты для дашборда (по всем дням)
    arr["h"] = (arr.sched_sec // 3600).clip(upper=23)
    arr["on_time"] = arr.delay_min.between(-1, 3)
    routes = {r.route_id: r.short_name for r in net.routes}
    stops = net.stops
    daily = arr.groupby("service_date").agg(avg_delay=("delay_min", "mean"), on_time=("on_time", "mean"),
                                            p90=("delay_min", lambda s: s.quantile(0.9))).reset_index()
    hourly = arr.assign(wk=arr.dow >= 5).groupby(["wk", "h"]).delay_min.mean().unstack(0)
    route_hour = arr[arr.dow < 5].groupby(["route_id", "h"]).delay_min.mean().unstack()
    worst_stops = (arr.assign(seg=arr.groupby(["service_date", "trip_id"]).delay_min.diff())
                   .groupby("stop_id").agg(inc=("seg", "mean"), n=("seg", "size")).sort_values("inc", ascending=False)
                   .head(15).reset_index())
    zone = arr.groupby(["zone", "h"]).delay_min.mean().unstack(0)
    wthr = arr.assign(whour=arr.actual_sec // 3600).merge(
        weather.assign(service_date=pd.to_datetime(weather["date"]).dt.date)
        .rename(columns={"hour": "whour"}), on=["service_date", "whour"])
    wthr["cond"] = np.where(wthr.snow == 1, "снег", np.where(wthr.precip_mm > 0, "дождь", "сухо"))
    by_weather = wthr.groupby("cond").delay_min.mean().round(2).to_dict()
    sample_trip = test[test.trip_id == test.trip_id.iloc[len(test) // 2]]

    out = {
        "data": {"days": a.days, "routes": a.routes, "stops": len(net.stops), "trips_per_day": len(net.trips),
                 "arrivals": int(len(arr)), "gps_pings_per_day_approx": 2_050_000,
                 "train_rows": int(len(train)), "test_rows": int(len(test)), "fit_seconds": round(fit_s, 1),
                 "date_from": days[0].isoformat(), "date_to": days[-1].isoformat(),
                 "test_from": min(test_days).isoformat()},
        "metrics": res,
        "importance": [{"feature": f, "mae_increase": round(float(v), 4)} for f, v in imp],
        "error_by_hour": by_hour.to_dict(orient="records"),
        "daily": [{"date": str(r.service_date), "avg_delay": round(r.avg_delay, 2), "on_time": round(r.on_time, 3),
                   "p90": round(r.p90, 2)} for r in daily.itertuples()],
        "hourly": [{"hour": int(h), "weekday": round(float(r[False]), 2), "weekend": round(float(r[True]), 2)}
                   for h, r in hourly.iterrows()],
        "route_hour": {"routes": [routes[r] for r in route_hour.index],
                       "hours": [int(h) for h in route_hour.columns],
                       "values": np.round(route_hour.to_numpy(), 2).tolist()},
        "route_avg": sorted([{"route": routes[r], "avg_delay": round(float(v), 2)}
                             for r, v in arr.groupby("route_id").delay_min.mean().items()],
                            key=lambda x: -x["avg_delay"]),
        "worst_stops": [{"stop": stops[r.stop_id].name, "route": routes[r.stop_id.split("_")[0]],
                         "zone": stops[r.stop_id].zone, "avg_increment_min": round(r.inc, 2)}
                        for r in worst_stops.itertuples()],
        "zone_hour": [{"hour": int(h), **{z: round(float(r[z]), 2) for z in zone.columns}} for h, r in zone.iterrows()],
        "by_weather": by_weather,
        "incidents": incidents.assign(route=incidents.route_id.map(routes))[
            ["date", "route", "kind", "start_sec", "end_sec"]].to_dict(orient="records"),
        "sample_trip": {"trip_id": sample_trip.trip_id.iloc[0],
                        "rows": sample_trip[["stop_sequence", "delay_min", "target", "pred"]].round(2)
                        .to_dict(orient="records")},
    }
    with open(os.path.join(a.out, "results.json"), "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=1, default=str)
    print(json.dumps(out["data"], ensure_ascii=False))
    print("importance:", out["importance"][:8])
    print("by_weather:", by_weather)
    print(f"всё за {time.time() - t0:.0f} c")


if __name__ == "__main__":
    main()
