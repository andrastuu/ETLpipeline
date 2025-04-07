FROM apache/airflow:2.7.1

USER root
WORKDIR /opt/airflow

# ✅ Install system dependencies required for ydata-profiling/phik
RUN apt-get update && apt-get install -y \
    build-essential \
    cmake \
    gcc \
    g++ \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY etl /opt/airflow/etl
COPY regions.yml /opt/airflow/regions.yml
