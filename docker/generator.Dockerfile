FROM python:3.11-slim
RUN pip install --no-cache-dir "kafka-python>=2.0.3"
COPY generator /app
WORKDIR /app
ENTRYPOINT ["python", "-m", "transit_sim.cli"]
