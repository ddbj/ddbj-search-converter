FROM python:3.12-bookworm

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

RUN apt update && \
    apt install -y --no-install-recommends \
    aria2 \
    curl \
    jq \
    pigz && \
    apt clean && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY pyproject.toml uv.lock README.md ./
RUN uv sync --frozen --extra tests

COPY . .

ENTRYPOINT [""]
CMD ["sleep", "infinity"]
