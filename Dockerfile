FROM python:3.14-slim

WORKDIR /app

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

COPY pyproject.toml .
COPY README.md .
COPY app/ app/
COPY service_workers/ service_workers/
RUN pip install --no-cache-dir -e .

COPY alembic.ini .
COPY migrations/ migrations/

EXPOSE 8000

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
