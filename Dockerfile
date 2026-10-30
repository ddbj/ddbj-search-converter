ARG VERSION=0.0.0.dev0

FROM python:3.14-slim-trixie AS base

ARG VERSION

LABEL org.opencontainers.image.title="ddbj-search-converter" \
    org.opencontainers.image.description="Data converter for DDBJ Search" \
    org.opencontainers.image.version="${VERSION}" \
    org.opencontainers.image.authors="Bioinformatics and DDBJ Center" \
    org.opencontainers.image.url="https://github.com/ddbj/ddbj-search-converter" \
    org.opencontainers.image.source="https://github.com/ddbj/ddbj-search-converter" \
    org.opencontainers.image.documentation="https://github.com/ddbj/ddbj-search-converter/blob/main/README.md" \
    org.opencontainers.image.licenses="Apache-2.0"

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    aria2 \
    curl \
    jq \
    pigz && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# uv creates /opt/venv without pip, so the pip of the base image is not used.
RUN python -m pip uninstall -y --root-user-action=ignore pip

COPY --from=ghcr.io/inutano/insdc-rdf:0.3.0 /usr/local/bin/insdc-rdf /usr/local/bin/insdc-rdf

WORKDIR /app

ENV UV_PROJECT_ENVIRONMENT=/opt/venv \
    UV_PYTHON_DOWNLOADS=never \
    UV_LINK_MODE=copy \
    PATH="/opt/venv/bin:$PATH" \
    HOME=/home/app

RUN mkdir -p /home/app && chmod 777 /home/app

FROM base AS builder

ARG VERSION

COPY --from=ghcr.io/astral-sh/uv:0.12 /uv /bin/

COPY pyproject.toml uv.lock README.md ./

RUN uv sync --frozen --no-dev --no-install-project --no-cache

COPY . .

RUN SETUPTOOLS_SCM_PRETEND_VERSION=${VERSION} uv sync --frozen --no-dev --no-cache

FROM base AS runtime

COPY --from=builder /opt/venv /opt/venv
COPY --from=builder /app /app

ENTRYPOINT []
CMD ["sleep", "infinity"]

FROM runtime AS dev

ARG VERSION

COPY --from=ghcr.io/astral-sh/uv:0.12 /uv /uvx /bin/

RUN SETUPTOOLS_SCM_PRETEND_VERSION=${VERSION} uv sync --frozen --no-cache
