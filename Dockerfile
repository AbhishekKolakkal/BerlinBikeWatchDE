FROM python:3.12-slim

WORKDIR /app

# Docker CLI (client only, statically linked, no daemon) -- lets the pipeline
# health-check script run `docker ps` against the *host's* Docker daemon via
# a socket mounted in at `docker run` time. Not used by producer/consumer/loader.
RUN apt-get update && apt-get install -y --no-install-recommends curl \
    && curl -fsSL https://download.docker.com/linux/static/stable/x86_64/docker-27.3.1.tgz \
       | tar xz --strip-components=1 -C /usr/local/bin docker/docker \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml uv.lock ./
RUN pip install uv && uv sync --frozen

COPY . .