FROM python:3.8-slim

FROM openjdk:11.0.13-jdk-slim
COPY --from=python:3.8-slim / /

WORKDIR /app


COPY ./requirements.txt /tmp/
RUN pip install -r /tmp/requirements.txt 


COPY ./docker/.env /app/.env

ADD src/stream-csv-read-csv /app/src/stream-csv-read-csv
ADD src/process /app/src/process
ADD src/fill-dm /app/src/fill-dm
ADD src/config.py /app/src/config.py

RUN adduser --disabled-password --gecos '' airflow
USER airflow


ENV PYTHONPATH "${PYTHONPATH}:."

