# Удобные команды. Без make: смотрите эквивалентные docker compose-команды в README.
DAYS ?= 14
SUBMIT = docker compose exec spark-master spark-submit --master spark://spark-master:7077 \
	--conf spark.driver.host=spark-master --conf spark.driver.memory=1g \
	--conf spark.executor.memory=1536m --conf spark.executor.cores=2 --conf spark.cores.max=4

.PHONY: up down clean data gtfs history bootstrap bootstrap-airflow stream stream-stop train benchmark validate test ps logs ch

up:                 ## поднять инфраструктуру
	docker compose up -d --build

down:
	docker compose --profile stream down

clean:              ## остановить и удалить все данные (тома, ./data)
	docker compose --profile stream down -v
	rm -rf data/gtfs data/raw

gtfs:               ## сгенерировать GTFS-расписание
	docker compose run --rm generator static --out /data/gtfs

history:            ## сгенерировать историю за DAYS дней (по умолчанию 14)
	docker compose run --rm generator history --out /data/raw --days $(DAYS)

data: gtfs history

bootstrap: data     ## загрузить всё в озеро и ClickHouse и обучить модель (напрямую через Spark)
	$(SUBMIT) jobs/ingest_static.py
	$(SUBMIT) jobs/ingest_history.py
	$(SUBMIT) jobs/build_arrivals.py --all
	$(SUBMIT) jobs/train_model.py --days 21 --test-days 3 --sample 0.5

bootstrap-airflow: data  ## то же самое, но запуском DAG в Airflow (видно в UI)
	docker compose exec airflow airflow dags unpause transit_bootstrap
	docker compose exec airflow airflow dags trigger transit_bootstrap

train:
	$(SUBMIT) jobs/train_model.py --days 21 --test-days 3 --sample 0.5

stream:             ## запустить «живой» поток: генератор -> Kafka -> Spark Streaming
	docker compose --profile stream up -d --build producer spark-streaming

stream-stop:
	docker compose --profile stream stop producer spark-streaming

benchmark:          ## pandas vs Spark
	$(SUBMIT) jobs/benchmark.py --days 1 3 7

validate:           ## локальная проверка модели без Docker (pandas + sklearn)
	python scripts/local_validation.py --days 14 --out docs/validation

test:
	python -m pytest -q tests

ps:
	docker compose ps

logs:
	docker compose logs -f --tail=100 spark-streaming producer

ch:                 ## консоль ClickHouse
	docker compose exec clickhouse clickhouse-client -u transit --password transit -d transit
