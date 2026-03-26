FROM python:3.13-slim

WORKDIR /app

RUN pip install --no-cache-dir asyncpg

COPY withdrawal.py .

CMD ["python", "withdrawal.py"]
