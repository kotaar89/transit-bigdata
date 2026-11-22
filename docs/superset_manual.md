# Ручная настройка дашборда Superset

`superset/bootstrap.py` создаёт всё автоматически при старте контейнера. Если какой-то график не создался
(API Superset меняется между версиями), соберите его вручную, это займёт несколько минут.

## 1. Подключение к ClickHouse
*Settings → Database Connections → + Database → ClickHouse Connect*
SQLAlchemy URI: `clickhousedb://transit:transit@clickhouse:8123/transit`

## 2. Датасеты
*Datasets → + Dataset*, схема `transit`: `v_daily`, `v_route_hour`, `v_bottlenecks`, `v_weather_effect`,
`v_prediction_quality`. Ещё два виртуальных датасета создаются в *SQL Lab → Save dataset*:

```sql
-- ds_model_latest
SELECT multiIf(model='schedule','1. Расписание', model='persistence','2. Текущая задержка',
               model='persistence_plus_hist','3. Текущая + норма','4. Gradient Boosting') AS model_name,
       mae_min, within_1min, run_id
FROM transit.model_metrics WHERE run_id = (SELECT max(run_id) FROM transit.model_metrics);

-- ds_live_5min
SELECT window_start, route_short_name, n_arrivals, avg_delay_min, p90_delay_min, on_time_share
FROM transit.route_delay_5min FINAL WHERE window_start >= now() - INTERVAL 6 HOUR;
```
В каждом датасете отметьте колонки `service_date`, `window_start`, `t` как *Is temporal*.

## 3. Графики

| Название | Тип | Датасет | Настройки |
|---|---|---|---|
| Средняя задержка, мин | Big Number | v_daily | Metric: `avg(avg_delay_min)` |
| Доля рейсов вовремя | Big Number | v_daily | Metric: `avg(on_time_share)`, формат `.1%` |
| Задержка по дням | Line Chart | v_daily | X: service_date (день), Metrics: `avg(avg_delay_min)`, `avg(p90_delay_min)` |
| Будни vs выходные | Line Chart | v_route_hour | X: hour, Metric: `avg(avg_delay_min)`, Dimension: day_type |
| Маршрут × час | Heatmap | v_route_hour | X: hour, Y: route_short_name, Metric: `avg(avg_delay_min)` |
| Узкие места | Table (raw) | v_bottlenecks | колонки route_short_name, stop_name, zone, avg_delay_growth_min; сортировка по убыванию; 20 строк |
| Влияние погоды | Bar Chart | v_weather_effect | X: weather, Metric: `avg(avg_delay_min)` |
| Качество моделей | Bar Chart | ds_model_latest | X: model_name, Metric: `avg(mae_min)` |
| Онлайн: задержка | Line Chart | ds_live_5min | X: window_start (5 мин), Metric: `avg(avg_delay_min)` |
| Онлайн: ошибка прогноза | Line Chart | v_prediction_quality | X: t, Metrics: `avg(mae_model_min)`, `avg(mae_persistence_min)` |

Добавьте графики на дашборд «Городской транспорт: задержки и прогнозы», включите автообновление
(*… → Set auto-refresh interval → 1 minute*), чтобы на защите было видно живой поток.
