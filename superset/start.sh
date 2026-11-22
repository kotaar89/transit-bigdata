#!/usr/bin/env bash
set -e
superset db upgrade
superset fab create-admin --username admin --password admin --firstname Admin --lastname Transit \
  --email admin@example.org || true
superset init
# создаём подключение к ClickHouse, датасеты, графики и дашборд (идемпотентно)
(sleep 25 && python /app/bootstrap/bootstrap.py || echo "bootstrap: см. docs/superset_manual.md") &
exec /usr/bin/run-server.sh
