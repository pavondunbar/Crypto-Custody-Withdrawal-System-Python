FROM python:3.13-slim

WORKDIR /app

RUN pip install --no-cache-dir aiohttp

COPY node.py .

CMD ["python", "node.py"]
