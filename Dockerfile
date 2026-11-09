FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ ./src/
COPY sql/ ./sql/

RUN mkdir -p /app/logs

CMD ["python", "-m", "src.etl_main"]