FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY config.py .
COPY scenarios/ scenarios/

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1
