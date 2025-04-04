# consumer/Dockerfile
FROM python:3.9-slim

RUN apt-get update && apt-get install -y curl \
    && curl -sSL https://install.python-poetry.org | python3 - \
    && apt-get remove -y python3-numpy python3-numpy-dev || true \
    && rm -rf /var/lib/apt/lists/*

ENV PATH="/root/.local/bin:$PATH"

WORKDIR /app

COPY pyproject.toml pyproject.toml

RUN poetry config virtualenvs.create false \
    && poetry lock --no-interaction \
    && poetry install --no-interaction --no-ansi --no-root

COPY consumer.py .

CMD ["python", "consumer.py"]
