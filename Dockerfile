# =============================================================================
# Data Engineering MVP - Custom Airflow Image
# =============================================================================
# Extends the official Airflow image with OpenJDK 17.
# PySpark requires a JVM at runtime; the base apache/airflow image does not
# include Java, so we install it here.
# =============================================================================

FROM apache/airflow:2.10.4
ARG AIRFLOW_VERSION=2.10.4
ARG PYTHON_VERSION=3.12

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /tmp/requirements.txt
COPY requirements-dbt.txt /tmp/requirements-dbt.txt

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow

# Install Airflow-adjacent Python dependencies under the Airflow constraints.
RUN pip install --no-cache-dir \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt" \
    -r /tmp/requirements.txt

# Install dbt separately, outside the Airflow constraints, to avoid resolver conflicts.
RUN pip install --no-cache-dir -r /tmp/requirements-dbt.txt
