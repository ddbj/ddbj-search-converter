FROM python:3.12-bookworm

LABEL org.opencontainers.image.title="ddbj-search-converter" \
    org.opencontainers.image.description="Data converter for DDBJ Search" \
    org.opencontainers.image.version="0.1.0" \
    org.opencontainers.image.authors="Bioinformatics and DDBJ Center" \
    org.opencontainers.image.url="https://github.com/ddbj/ddbj-search-converter" \
    org.opencontainers.image.source="https://github.com/ddbj/ddbj-search-converter" \
    org.opencontainers.image.documentation="https://github.com/ddbj/ddbj-search-converter/blob/main/README.md" \
    org.opencontainers.image.licenses="Apache-2.0"

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

RUN apt update && \
    apt install -y --no-install-recommends \
    aria2 \
    curl \
    iputils-ping \
    jq \
    less \
    pigz \
    procps \
    tree \
    vim-tiny && \
    apt clean && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY pyproject.toml uv.lock README.md ./
COPY ddbj_search_converter ./ddbj_search_converter

RUN uv sync --frozen --extra tests && \
    chmod -R a+rwX .venv

COPY . .

# Writable home for arbitrary UID
ENV HOME=/home/app
RUN mkdir -p /home/app && chmod 777 /home/app

ENV PATH="/app/.venv/bin:$PATH"

ENTRYPOINT [""]
CMD ["sleep", "infinity"]
