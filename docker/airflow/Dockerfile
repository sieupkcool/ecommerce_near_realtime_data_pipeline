FROM apache/airflow:3.0.6

# Thêm thư mục code của vào PYTHONPATH
# Điều này cho phép import "from ecommerce.models..."
ENV PYTHONPATH "${PYTHONPATH}:/opt/airflow/code"

USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         build-essential \
         jq \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

USER airflow

RUN pip install --no-cache-dir faker geopy

ENV REDIS_HOST=${REDIS_HOST}
ENV REDIS_PASSWORD=${REDIS_PASSWORD}
ENV REDIS_PORT=${REDIS_PORT}
ENV REDIS_DB=${REDIS_DB}