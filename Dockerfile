# syntax=docker/dockerfile:1

# Multi-stage build: create a venv with dependencies, then copy it into a small runtime image.

FROM python:3.12-alpine AS builder

# Build deps (kept out of final image). Some Python deps may need compilation on musl.
RUN apk add --no-cache build-base libffi-dev openssl-dev

ENV VENV_PATH=/opt/venv
RUN python -m venv "$VENV_PATH"
ENV PATH="$VENV_PATH/bin:$PATH"

WORKDIR /app
COPY requirements.txt ./
RUN pip install --upgrade pip \
  && pip install --no-cache-dir -r requirements.txt

# Copy only the app sources.
COPY *.py ./


FROM python:3.12-alpine AS runtime

ENV VENV_PATH=/opt/venv
ENV PATH="$VENV_PATH/bin:$PATH"
ENV PYTHONUNBUFFERED=1

# Non-root user; the DB path should be a mounted volume or writable working dir.
RUN addgroup -S app \
  && adduser -S -G app app

WORKDIR /app
COPY --from=builder "$VENV_PATH" "$VENV_PATH"
COPY --from=builder /app /app
RUN chown -R app:app /app

USER app

# Set env vars at runtime (WIKI_START_PAGE_TITLE, WIKI_DB_PATH, etc.).
CMD ["python", "walker.py"]
