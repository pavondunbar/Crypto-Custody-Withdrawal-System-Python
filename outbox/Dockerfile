FROM python:3.13-slim

WORKDIR /app

RUN pip install --no-cache-dir asyncpg aiokafka

COPY outbox-publisher.py .

CMD ["python", "outbox-publisher.py"]
