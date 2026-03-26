FROM python:3.11-slim

WORKDIR /app

# uv pour la gestion des dépendances
RUN pip install --no-cache-dir uv

COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev

COPY etl/  ./etl/
COPY api/  ./api/

ENV DATA_DIR=/app/data
EXPOSE 8000

CMD ["uv", "run", "uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000"]
