FROM apache/superset:4.1.1
USER root
RUN pip install --no-cache-dir clickhouse-connect==0.8.11 requests
USER superset
