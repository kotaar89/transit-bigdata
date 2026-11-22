# Перенос в VK Cloud

Проект запускается локально, но каждый компонент имеет прямой аналог в VK Cloud. Код менять почти не нужно:
все адреса задаются переменными окружения (`spark/common/session.py`).

| Локально | VK Cloud | Что поменять |
|---|---|---|
| MinIO | Object Storage (S3) | `S3_ENDPOINT=https://hb.ru-msk.vkcloud-storage.ru` (адрес вашего региона), ключи доступа сервисного аккаунта, бакет `lake` |
| Kafka | Managed Kafka (Data Platform) или Kafka на ВМ | `KAFKA_BOOTSTRAP` |
| Spark standalone | Spark в Kubernetes (Cloud Containers) или Data Platform | мастер `k8s://…` или кластер ДП; образ `docker/spark.Dockerfile` пушится в Container Registry |
| ClickHouse | Managed ClickHouse (Cloud Databases) | `CLICKHOUSE_HOST`, порт `8443`/HTTPS, пользователь и пароль; выполнить `clickhouse/init.sql` |
| Airflow | Airflow (Data Platform) | положить `airflow/dags` в репозиторий DAG-ов, `SPARK_MASTER_URL` |
| Superset | ВМ с Docker или Data Platform BI | URI подключения к Managed ClickHouse |

Порядок действий:
1. Создать бакет `lake` и сервисный аккаунт с ключами S3.
2. Поднять Managed ClickHouse и прогнать `init.sql`.
3. Собрать образы и запушить в VK Container Registry.
4. Поднять Kafka и запустить продюсер (ВМ или под в Kubernetes).
5. Запустить `stream_processing.py` как долгоживущий Spark-апп, батч-задачи — из Airflow.

Названия сервисов и адреса эндпоинтов уточняйте в консоли VK Cloud: они зависят от региона и
со временем меняются.
