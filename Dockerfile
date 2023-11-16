FROM python:3.8-slim

RUN mkdir -p /opt/airflow/data
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt