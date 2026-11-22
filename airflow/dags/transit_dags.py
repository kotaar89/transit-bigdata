"""DAG-и проекта.

transit_bootstrap — однократная загрузка: GTFS-справочники + история -> озеро -> ClickHouse -> первое обучение.
transit_daily     — каждую ночь: пересчёт прибытий за вчера, переобучение модели, бенчмарк по воскресеньям.
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.empty import EmptyOperator

JOBS = os.environ.get("SPARK_JOBS", "/opt/transit/spark/jobs")
MASTER = os.environ.get("SPARK_MASTER_URL", "spark://spark-master:7077")

SUBMIT = (
    f"spark-submit --master {MASTER} "
    "--conf spark.driver.host=airflow --conf spark.driver.bindAddress=0.0.0.0 "
    "--conf spark.driver.memory=1g --conf spark.executor.memory=1536m "
    "--conf spark.executor.cores=2 --conf spark.cores.max=4 "
    "--conf 'spark.driver.extraClassPath=/opt/jars/*' "
    f"--py-files /tmp/transit_common.zip "
)
# упаковываем общий код, чтобы он был доступен и драйверу, и исполнителям
PACK = "cd /opt/transit/spark && rm -f /tmp/transit_common.zip && python -m zipfile -c /tmp/transit_common.zip common && "


def spark_task(task_id: str, script: str, args: str = "", **kw) -> BashOperator:
    return BashOperator(task_id=task_id, bash_command=PACK + SUBMIT + f"{JOBS}/{script} {args}", **kw)


default_args = {
    "owner": "transit-team",
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    dag_id="transit_bootstrap",
    description="Однократная загрузка истории и первое обучение модели",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["transit", "bootstrap"],
) as bootstrap:
    static = spark_task("ingest_static_gtfs", "ingest_static.py")
    history = spark_task("ingest_history_to_bronze", "ingest_history.py",
                         execution_timeout=timedelta(hours=2))
    arrivals = spark_task("build_arrivals_all", "build_arrivals.py", "--all")
    train = spark_task("train_model", "train_model.py", "--days 21 --test-days 3 --sample 0.5",
                       execution_timeout=timedelta(hours=2))
    static >> history >> arrivals >> train


def _is_sunday(ds: str, **_):
    return "benchmark" if pendulum.parse(ds).day_of_week == pendulum.SUNDAY else "skip_benchmark"


with DAG(
    dag_id="transit_daily",
    description="Ежедневный пересчёт витрин и переобучение",
    start_date=pendulum.datetime(2026, 11, 1, tz="Europe/Moscow"),
    schedule="30 3 * * *",  # 03:30 — после окончания движения
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["transit", "daily"],
) as daily:
    # {{ ds }} — логическая дата запуска = вчерашний день движения
    rebuild = spark_task("build_arrivals_for_day", "build_arrivals.py", "--date {{ ds }}")
    retrain = spark_task("retrain_model", "train_model.py", "--days 21 --test-days 3 --sample 0.5",
                         execution_timeout=timedelta(hours=2))
    branch = BranchPythonOperator(task_id="is_sunday", python_callable=_is_sunday)
    bench = spark_task("benchmark", "benchmark.py", "--days 1 3 7")
    skip = EmptyOperator(task_id="skip_benchmark")
    rebuild >> retrain >> branch >> [bench, skip]
