FROM python:3.12-bookworm

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

RUN uv sync --frozen --extra tests

COPY . .

ENV PATH="/app/.venv/bin:$PATH"

ENTRYPOINT [""]
CMD ["sleep", "infinity"]
