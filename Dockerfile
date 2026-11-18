# Base fully qualified: en Podman un nombre corto ("python:3.11-slim") depende
# de short-name-mode (en modo enforcing ni siquiera resuelve) y puede caer en
# otro registro. docker.io/library/... funciona igual en Docker y en Podman.
FROM docker.io/library/python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ ./src/
COPY sql/ ./sql/

RUN mkdir -p /app/logs

CMD ["python", "-m", "src.etl_main"]