FROM python:3.10.14-slim-bookworm

RUN apt update && \
    apt install -y --no-install-recommends \
    curl \
    jq \
    perl && \
    apt clean && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY . .
RUN python3 -m pip install --no-cache-dir --progress-bar off -U pip && \
    python3 -m pip install --no-cache-dir --progress-bar off -e .[tests]

CMD ["sleep", "infinity"]
