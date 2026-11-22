"""Автоматическая настройка Superset через REST API: БД ClickHouse, датасеты, графики, дашборд.

Скрипт идемпотентен: существующие объекты не дублируются.
Если какой-то график не создался (API Superset меняется между версиями), его легко
собрать руками — SQL и настройки каждого графика описаны в docs/superset_manual.md.
"""
from __future__ import annotations

import json
import os
import time

import requests

URL = os.environ.get("SUPERSET_INTERNAL_URL", "http://localhost:8088")
CH_USER = os.environ.get("CLICKHOUSE_USER", "transit")
CH_PASSWORD = os.environ.get("CLICKHOUSE_PASSWORD", "transit")
DB_NAME = "ClickHouse transit"
DASH_TITLE = "Городской транспорт: задержки и прогнозы"

s = requests.Session()


def wait_ready():
    for _ in range(60):
        try:
            if s.get(f"{URL}/health", timeout=5).ok:
                return
        except requests.RequestException:
            pass
        time.sleep(5)
    raise SystemExit("Superset не поднялся")


def login():
    r = s.post(f"{URL}/api/v1/security/login",
               json={"username": "admin", "password": "admin", "provider": "db", "refresh": True})
    r.raise_for_status()
    s.headers["Authorization"] = f"Bearer {r.json()['access_token']}"
    csrf = s.get(f"{URL}/api/v1/security/csrf_token/").json()["result"]
    s.headers["X-CSRFToken"] = csrf
    s.headers["Referer"] = URL


def find(kind, col, value):
    q = json.dumps({"filters": [{"col": col, "opr": "eq", "value": value}]})
    r = s.get(f"{URL}/api/v1/{kind}/", params={"q": q})
    r.raise_for_status()
    res = r.json()["result"]
    return res[0]["id"] if res else None


def ensure_db():
    db_id = find("database", "database_name", DB_NAME)
    if db_id:
        return db_id
    r = s.post(f"{URL}/api/v1/database/", json={
        "database_name": DB_NAME,
        "sqlalchemy_uri": f"clickhousedb://{CH_USER}:{CH_PASSWORD}@clickhouse:8123/transit",
        "expose_in_sqllab": True, "allow_run_async": False,
    })
    r.raise_for_status()
    return r.json()["id"]


def ensure_dataset(db_id, name, sql=None):
    ds_id = find("dataset", "table_name", name)
    if ds_id:
        return ds_id
    body = {"database": db_id, "schema": "transit", "table_name": name}
    if sql:
        body["sql"] = sql
    r = s.post(f"{URL}/api/v1/dataset/", json=body)
    if not r.ok:
        print("dataset", name, r.status_code, r.text[:300])
        r.raise_for_status()
    ds_id = r.json()["id"]
    # пометить колонки дат как временные
    cols = s.get(f"{URL}/api/v1/dataset/{ds_id}").json()["result"]["columns"]
    upd = [{"id": c["id"], "column_name": c["column_name"],
            "is_dttm": c["column_name"] in ("service_date", "window_start", "t", "pred_ts", "created_at")}
           for c in cols]
    s.put(f"{URL}/api/v1/dataset/{ds_id}", json={"columns": upd})
    return ds_id


def metric(sql, label):
    return {"expressionType": "SQL", "sqlExpression": sql, "label": label}


def ensure_chart(name, viz, ds_id, params, dash_id):
    if find("chart", "slice_name", name):
        return
    params = {"viz_type": viz, "datasource": f"{ds_id}__table", **params}
    r = s.post(f"{URL}/api/v1/chart/", json={
        "slice_name": name, "viz_type": viz, "datasource_id": ds_id, "datasource_type": "table",
        "params": json.dumps(params), "dashboards": [dash_id]})
    print(("OK  " if r.ok else "ERR ") + name, "" if r.ok else r.text[:200])


def main():
    wait_ready()
    login()
    db = ensure_db()
    ds = {
        "v_daily": ensure_dataset(db, "v_daily"),
        "v_route_hour": ensure_dataset(db, "v_route_hour"),
        "v_bottlenecks": ensure_dataset(db, "v_bottlenecks"),
        "v_weather_effect": ensure_dataset(db, "v_weather_effect"),
        "v_prediction_quality": ensure_dataset(db, "v_prediction_quality"),
        "ds_model_latest": ensure_dataset(db, "ds_model_latest", """
            SELECT multiIf(model = 'schedule', '1. Расписание',
                           model = 'persistence', '2. Текущая задержка',
                           model = 'persistence_plus_hist', '3. Текущая + норма',
                           '4. Gradient Boosting') AS model_name,
                   mae_min, within_1min, run_id
            FROM transit.model_metrics
            WHERE run_id = (SELECT max(run_id) FROM transit.model_metrics)"""),
        "ds_live_5min": ensure_dataset(db, "ds_live_5min", """
            SELECT window_start, route_short_name, n_arrivals, avg_delay_min, p90_delay_min, on_time_share
            FROM transit.route_delay_5min FINAL
            WHERE window_start >= now() - INTERVAL 6 HOUR"""),
    }
    dash_id = find("dashboard", "dashboard_title", DASH_TITLE)
    if not dash_id:
        r = s.post(f"{URL}/api/v1/dashboard/", json={"dashboard_title": DASH_TITLE, "published": True,
                                                       "slug": "transit"})
        r.raise_for_status()
        dash_id = r.json()["id"]

    avg_delay = metric("avg(avg_delay_min)", "Средняя задержка, мин")
    ensure_chart("Средняя задержка, мин", "big_number_total", ds["v_daily"],
                 {"metric": avg_delay, "y_axis_format": ".2f", "subheader": "по всем дням"}, dash_id)
    ensure_chart("Доля рейсов вовремя", "big_number_total", ds["v_daily"],
                 {"metric": metric("avg(on_time_share)", "Вовремя (−1…+3 мин)"), "y_axis_format": ".1%"}, dash_id)
    ensure_chart("Задержка по дням", "echarts_timeseries_line", ds["v_daily"],
                 {"x_axis": "service_date", "time_grain_sqla": "P1D",
                  "metrics": [avg_delay, metric("avg(p90_delay_min)", "90-й перцентиль")],
                  "groupby": [], "row_limit": 1000, "markerEnabled": True}, dash_id)
    ensure_chart("Задержка по часам: будни vs выходные", "echarts_timeseries_line", ds["v_route_hour"],
                 {"x_axis": "hour", "metrics": [metric("avg(avg_delay_min)", "Задержка, мин")],
                  "groupby": ["day_type"], "row_limit": 1000}, dash_id)
    ensure_chart("Тепловая карта: маршрут × час", "heatmap_v2", ds["v_route_hour"],
                 {"x_axis": "hour", "groupby": "route_short_name",
                  "metric": metric("avg(avg_delay_min)", "Задержка, мин"),
                  "linear_color_scheme": "superset_seq_1", "row_limit": 10000, "sort_x_axis": "alpha_asc",
                  "sort_y_axis": "value_desc", "normalize_across": "heatmap", "show_values": False}, dash_id)
    ensure_chart("Узкие места: где задержка растёт сильнее всего", "table", ds["v_bottlenecks"],
                 {"query_mode": "raw",
                  "all_columns": ["route_short_name", "stop_name", "zone", "avg_delay_growth_min", "passes"],
                  "order_by_cols": [json.dumps(["avg_delay_growth_min", False])], "row_limit": 20}, dash_id)
    ensure_chart("Влияние погоды", "echarts_timeseries_bar", ds["v_weather_effect"],
                 {"x_axis": "weather", "metrics": [metric("avg(avg_delay_min)", "Задержка, мин")],
                  "groupby": [], "row_limit": 10}, dash_id)
    ensure_chart("Качество моделей (MAE, мин)", "echarts_timeseries_bar", ds["ds_model_latest"],
                 {"x_axis": "model_name", "metrics": [metric("avg(mae_min)", "MAE, мин")],
                  "groupby": [], "row_limit": 10, "x_axis_sort_asc": True}, dash_id)
    ensure_chart("Онлайн: задержка по маршрутам (5 мин)", "echarts_timeseries_line", ds["ds_live_5min"],
                 {"x_axis": "window_start", "time_grain_sqla": "PT5M",
                  "metrics": [metric("avg(avg_delay_min)", "Задержка, мин")], "groupby": [],
                  "row_limit": 5000}, dash_id)
    ensure_chart("Онлайн: ошибка прогноза vs «текущая задержка»", "echarts_timeseries_line",
                 ds["v_prediction_quality"],
                 {"x_axis": "t", "time_grain_sqla": "PT15M",
                  "metrics": [metric("avg(mae_model_min)", "Модель"),
                              metric("avg(mae_persistence_min)", "Текущая задержка")],
                  "groupby": [], "row_limit": 5000}, dash_id)
    print(f"Дашборд: {URL}/superset/dashboard/transit/")


if __name__ == "__main__":
    main()
