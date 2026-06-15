FROM python:3.11-slim

LABEL org.opencontainers.image.title="mesh-rest-bridge"
LABEL org.opencontainers.image.description="Meshtastic BLE to JSON-RPC/REST/WebSocket bridge"

RUN apt-get update && apt-get install -y \
    bluetooth \
    bluez \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

COPY core/ /app/core/
COPY cli/ /app/cli/

EXPOSE 8000

ENTRYPOINT ["python", "-m", "cli.main"]
CMD ["--help"]
