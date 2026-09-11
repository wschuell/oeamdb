FROM python:3.13-slim AS build
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc python3-dev libpq-dev && rm -rf /var/lib/apt/lists/*
RUN python -m venv /opt/venv
ENV PATH=/opt/venv/bin:$PATH
WORKDIR /app
COPY pyproject.toml README.md ./
COPY src ./src
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install -e ".[web]"



FROM python:3.13-slim

RUN useradd --create-home --uid 10001 app

RUN apt-get update && apt-get install -y --no-install-recommends libpq5 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=build /opt/venv /opt/venv
COPY --from=build /app /app
ENV PATH=/opt/venv/bin:$PATH \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

ARG OEAMDB_WEB_VERSION=dev
ARG OEAMDB_WEB_GIT_SHA=unknown
LABEL org.opencontainers.image.version="${OEAMDB_WEB_VERSION}"\
      org.opencontainers.image.revision="${OEAMDB_WEB_GIT_SHA}"
ENV OEAMDB_WEB_VERSION=${OEAMDB_WEB_VERSION} OEAMDB_WEB_GIT_SHA=${OEAMDB_WEB_GIT_SHA}

USER app
CMD ["uvicorn", "oeamdb.web.main:app", "--host", "0.0.0.0", "--port", "8000"]