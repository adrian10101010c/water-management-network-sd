FROM python:3.9-slim

WORKDIR /app

COPY . .

EXPOSE 8080

CMD ["python", "wm_central.py", "8080", "localhost:9092"]