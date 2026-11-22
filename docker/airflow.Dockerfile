FROM apache/airflow:2.10.3-python3.10
USER root
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-17-jre-headless curl \
 && ln -s "$(dirname "$(dirname "$(readlink -f "$(which java)")")")" /usr/lib/jvm/default-java \
 && rm -rf /var/lib/apt/lists/*
COPY docker/download_jars.sh /tmp/download_jars.sh
RUN bash /tmp/download_jars.sh /opt/jars
ENV JAVA_HOME=/usr/lib/jvm/default-java
RUN mkdir -p /opt/airflow/db && chown -R airflow: /opt/airflow/db
USER airflow
RUN pip install --no-cache-dir "apache-airflow==2.10.3" pyspark==3.5.3 numpy==1.26.4 pandas==2.2.3
