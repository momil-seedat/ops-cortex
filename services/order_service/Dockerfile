FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y wget && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY config.py .
COPY memory/ memory/
COPY ml/ ml/
COPY services/order_service/app.py services/order_service/app.py

RUN mkdir -p /data

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

EXPOSE 8001

CMD ["python", "-m", "uvicorn", "services.order_service.app:app", "--host", "0.0.0.0", "--port", "8001"]
