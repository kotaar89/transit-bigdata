FROM apache/spark:3.5.3-scala2.12-java17-python3-ubuntu
USER root
RUN apt-get update && apt-get install -y --no-install-recommends python3-pip curl \
 && pip3 install --no-cache-dir numpy==1.26.4 pandas==2.2.3 \
 && rm -rf /var/lib/apt/lists/*
COPY docker/download_jars.sh /tmp/download_jars.sh
RUN bash /tmp/download_jars.sh /opt/spark/jars
ENV PATH="${PATH}:/opt/spark/bin" \
    PYTHONPATH=/opt/transit/spark \
    PYSPARK_PYTHON=python3
WORKDIR /opt/transit/spark
