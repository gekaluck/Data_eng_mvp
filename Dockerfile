# =============================================================================
# Data Engineering MVP — Custom Airflow Image
# =============================================================================
# Extends the official Airflow image with OpenJDK 17.
# PySpark requires a JVM at runtime; the base apache/airflow image does not
# include Java, so we install it here.
#
# Why a Dockerfile instead of _PIP_ADDITIONAL_REQUIREMENTS?
# Java is a system-level dependency (not a Python package). pip can install
# pyspark (the Python wrapper), but the JVM itself must come from the OS
# package manager. A custom Dockerfile is the right tool for this.
# =============================================================================

FROM apache/airflow:2.10.4

USER root

# Install OpenJDK 17 JRE (headless = no GUI libs, keeps the image smaller).
# We only need the JRE (not the full JDK) to run Spark.
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# JAVA_HOME must be set for PySpark to locate the JVM.
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow
