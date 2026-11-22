#!/usr/bin/env bash
# Скачивает JAR-зависимости Spark: Kafka, S3 (hadoop-aws) и ClickHouse JDBC.
set -euo pipefail
DEST="${1:-/opt/jars}"
mkdir -p "$DEST"
M=https://repo1.maven.org/maven2
JARS=(
  "org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.3/spark-sql-kafka-0-10_2.12-3.5.3.jar"
  "org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.3/spark-token-provider-kafka-0-10_2.12-3.5.3.jar"
  "org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar"
  "org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar"
  "org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar"
  "com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar"
  "com/clickhouse/clickhouse-jdbc/0.6.3/clickhouse-jdbc-0.6.3-all.jar"
)
for j in "${JARS[@]}"; do
  echo "-> $(basename "$j")"
  curl -fsSL --retry 5 -o "$DEST/$(basename "$j")" "$M/$j"
done
