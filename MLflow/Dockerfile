FROM ubuntu:24.04

RUN apt-get update -y; \
    apt-get install -y -q \
        locales \
        ca-certificates \
        gcc \
        swig \
        cmake \
        libpq-dev \
        curl \
        vim

ENV PYTHONDONTWRITEBYTECODE=0 \
    PYTHONUNBUFFERED=0 \
    LANG=en_US.UTF-8 \
    LANGUAGE=en_US:en \
    LC_ALL=en_US.UTF-8 \
    WORK_DIR=/app \
    PATH="/root/.local/bin/:$PATH"

RUN locale-gen en_US.UTF-8

RUN dpkg-reconfigure locales && apt-get update -y;

WORKDIR "${WORK_DIR}"

RUN curl -LsSf https://astral.sh/uv/install.sh | sh

COPY --link ./pyproject.toml ./pyproject.toml

RUN uv sync