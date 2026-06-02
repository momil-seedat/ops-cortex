FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y wget && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY config.py .
COPY memory/ memory/
COPY ml/ ml/
COPY agents/ agents/

RUN mkdir -p /data

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1
