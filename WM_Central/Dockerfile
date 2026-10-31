FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libc6-dev \
    librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir confluent-kafka flask

COPY . .

EXPOSE 5000 8080

ENV SOCKET_PORT=5000
ENV BROKER_ADDRESS=broker:29092

ENV PYTHONUNBUFFERED=1

CMD ["sh", "-c", "python wm_central.py $SOCKET_PORT $BROKER_ADDRESS"]