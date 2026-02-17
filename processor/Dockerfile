FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends build-essential libsnappy-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt ./
COPY requirements-light.txt ./
ARG SENTIMENT_LIGHT_RUNTIME=false
RUN if [ "$SENTIMENT_LIGHT_RUNTIME" = "true" ]; then \
        pip install --no-cache-dir -r requirements-light.txt; \
    else \
        pip install --no-cache-dir -r requirements.txt; \
    fi

COPY src ./src

CMD ["python", "-m", "src.app"]
