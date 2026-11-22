-- Схема аналитического хранилища (ClickHouse). Выполняется автоматически при первом старте контейнера.
CREATE DATABASE IF NOT EXISTS transit;

-- ===== Справочники =====
CREATE TABLE IF NOT EXISTS transit.dim_routes
(
    route_id         String,
    route_short_name String,
    route_long_name  String,
    route_type       Int32
) ENGINE = MergeTree ORDER BY route_id;

CREATE TABLE IF NOT EXISTS transit.dim_stops
(
    stop_id          String,
    stop_name        String,
    zone             String,
    stop_lat         Float64,
    stop_lon         Float64,
    route_id         String,
    route_short_name String,
    direction_id     Int32,
    stop_sequence    Int32
) ENGINE = MergeTree ORDER BY (route_id, direction_id, stop_sequence);

-- ===== Факты: прибытия на остановки =====
-- Время хранится как epoch (UInt32/Int64) — так надёжнее через JDBC; DateTime вычисляется в ClickHouse.
CREATE TABLE IF NOT EXISTS transit.arrivals
(
    service_date     Date,
    route_id         String,
    route_short_name String,
    direction_id     Int32,
    trip_id          String,
    vehicle_id       String,
    stop_id          String,
    stop_name        String,
    zone             String,
    stop_sequence    Int32,
    sched_sec        Int32,
    scheduled_epoch  Int64,
    actual_epoch     Int64,
    delay_sec        Int32,
    hour             Int32,
    dow              Int32,
    scheduled_ts     DateTime('Europe/Moscow') MATERIALIZED toDateTime(scheduled_epoch, 'Europe/Moscow'),
    actual_ts        DateTime('Europe/Moscow') MATERIALIZED toDateTime(actual_epoch, 'Europe/Moscow'),
    delay_min        Float32 MATERIALIZED delay_sec / 60,
    on_time          UInt8 MATERIALIZED delay_sec BETWEEN -60 AND 180
) ENGINE = ReplacingMergeTree
PARTITION BY service_date
ORDER BY (service_date, route_id, trip_id, stop_sequence);

-- ===== Онлайн-агрегаты из стриминга =====
CREATE TABLE IF NOT EXISTS transit.route_delay_5min
(
    window_start_epoch Int64,
    route_id           String,
    route_short_name   String,
    direction_id       Int32,
    n_arrivals         Int64,
    avg_delay_min      Float64,
    p90_delay_min      Float64,
    on_time_share      Float64,
    window_start       DateTime('Europe/Moscow') MATERIALIZED toDateTime(window_start_epoch, 'Europe/Moscow')
) ENGINE = ReplacingMergeTree
ORDER BY (window_start_epoch, route_id, direction_id);

-- ===== Онлайн-прогнозы =====
CREATE TABLE IF NOT EXISTS transit.predictions
(
    service_date            Date,
    route_id                String,
    direction_id            Int32,
    trip_id                 String,
    pred_epoch              Int64,
    from_stop_sequence      Int32,
    target_stop_sequence    Int32,
    target_stop_id          String,
    current_delay_sec       Int32,
    predicted_delay_sec     Int32,
    predicted_arrival_epoch Int64,
    pred_ts                 DateTime('Europe/Moscow') MATERIALIZED toDateTime(pred_epoch, 'Europe/Moscow')
) ENGINE = ReplacingMergeTree
PARTITION BY service_date
ORDER BY (service_date, trip_id, target_stop_sequence, from_stop_sequence);

-- ===== Погода =====
CREATE TABLE IF NOT EXISTS transit.weather_hourly
(
    date      Date,
    hour      Int32,
    precip_mm Float64,
    temp_c    Float64,
    snow      Int32
) ENGINE = ReplacingMergeTree ORDER BY (date, hour);

-- ===== ML: метрики и важность признаков =====
CREATE TABLE IF NOT EXISTS transit.model_metrics
(
    run_id        String,
    model         String,
    mae_min       Float64,
    rmse_min      Float64,
    within_1min   Float64,
    within_2min   Float64,
    train_rows    Int64,
    test_rows     Int64,
    fit_seconds   Float64,
    horizon_stops Int32,
    test_from     String,
    test_to       String,
    created_at    DateTime DEFAULT now()
) ENGINE = MergeTree ORDER BY (run_id, model);

CREATE TABLE IF NOT EXISTS transit.feature_importance
(
    run_id     String,
    feature    String,
    importance Float64
) ENGINE = MergeTree ORDER BY (run_id, feature);

CREATE TABLE IF NOT EXISTS transit.model_error_by_hour
(
    run_id          String,
    hour            Int32,
    mae_gbt         Float64,
    mae_persistence Float64
) ENGINE = MergeTree ORDER BY (run_id, hour);

CREATE TABLE IF NOT EXISTS transit.benchmark
(
    days          Int32,
    pings         Int64,
    size_mb       Float64,
    pandas_sec    Nullable(Float64),
    spark_sec     Float64,
    pandas_status String,
    created_at    DateTime DEFAULT now()
) ENGINE = MergeTree ORDER BY (created_at, days);

-- ===== Витрины (представления) для дашборда =====
CREATE VIEW IF NOT EXISTS transit.v_daily AS
SELECT service_date,
       count()                       AS arrivals,
       round(avg(delay_min), 2)      AS avg_delay_min,
       round(quantile(0.9)(delay_min), 2) AS p90_delay_min,
       round(avg(on_time), 3)        AS on_time_share
FROM transit.arrivals FINAL
GROUP BY service_date;

CREATE VIEW IF NOT EXISTS transit.v_route_hour AS
SELECT service_date, route_id, route_short_name, hour,
       if(dow >= 5, 'выходной', 'будний') AS day_type,
       count() AS arrivals,
       avg(delay_min) AS avg_delay_min,
       avg(on_time)   AS on_time_share
FROM transit.arrivals FINAL
GROUP BY service_date, route_id, route_short_name, hour, day_type;

-- «Узкие места»: на каких перегонах задержка растёт сильнее всего
CREATE VIEW IF NOT EXISTS transit.v_bottlenecks AS
SELECT route_short_name, stop_name, zone, stop_sequence, direction_id,
       count() AS passes,
       round(avg(inc_min), 2) AS avg_delay_growth_min
FROM
(
    SELECT route_short_name, stop_name, zone, stop_sequence, direction_id,
           (delay_sec - lagInFrame(delay_sec) OVER w) / 60 AS inc_min,
           row_number() OVER w AS rn
    FROM transit.arrivals FINAL
    WINDOW w AS (PARTITION BY service_date, trip_id ORDER BY stop_sequence
                 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
WHERE rn > 1
GROUP BY route_short_name, stop_name, zone, stop_sequence, direction_id
HAVING passes > 50;

-- Качество онлайн-прогнозов: прогноз vs фактическое прибытие
CREATE VIEW IF NOT EXISTS transit.v_prediction_quality AS
SELECT p.service_date, p.route_id, toStartOfFifteenMinutes(p.pred_ts) AS t,
       count() AS n,
       avg(abs(p.predicted_delay_sec - a.delay_sec)) / 60 AS mae_model_min,
       avg(abs(p.current_delay_sec  - a.delay_sec)) / 60 AS mae_persistence_min
FROM transit.predictions AS p FINAL
INNER JOIN transit.arrivals AS a FINAL
    ON a.service_date = p.service_date AND a.trip_id = p.trip_id AND a.stop_sequence = p.target_stop_sequence
GROUP BY p.service_date, p.route_id, t;

CREATE VIEW IF NOT EXISTS transit.v_weather_effect AS
SELECT multiIf(w.snow = 1, 'снег', w.precip_mm > 0, 'дождь', 'сухо') AS weather,
       count() AS arrivals,
       round(avg(a.delay_min), 2) AS avg_delay_min
FROM transit.arrivals AS a FINAL
INNER JOIN transit.weather_hourly AS w FINAL
    ON w.date = a.service_date AND w.hour = least(toHour(a.actual_ts), 23)
GROUP BY weather;
