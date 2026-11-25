FROM eclipse-temurin:17.0.17_10-jre-noble AS java
FROM python:3.11.14-slim-bookworm

COPY --from=java /opt/java/openjdk /opt/java/openjdk
ENV JAVA_HOME=/opt/java/openjdk \
    PATH=/opt/java/openjdk/bin:$PATH \
    PYTHONPATH=/workspace \
    PYTHONUNBUFFERED=1 \
    SPARK_LOCAL_IP=127.0.0.1 \
    TZ=UTC

WORKDIR /workspace
COPY requirements.txt /tmp/lab-requirements.txt
RUN pip install --no-cache-dir -r /tmp/lab-requirements.txt
