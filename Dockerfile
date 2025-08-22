FROM bitnami/spark:3.5.1

USER root
RUN install_packages python3 python3-venv python3-pip

WORKDIR /app
COPY requirements.txt /app/
RUN pip3 install --no-cache-dir -r requirements.txt

COPY pipeline /app/pipeline
COPY scripts /app/scripts

ENV PYTHONUNBUFFERED=1
ENV SPARK_MODE=client

ENTRYPOINT ["/app/scripts/entrypoint.sh"]
