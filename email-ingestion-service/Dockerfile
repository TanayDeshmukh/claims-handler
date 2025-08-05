FROM python:3.12-slim
LABEL authors="tanay"

COPY --from=ghcr.io/astral-sh/uv:0.7.13 /uv /uvx /bin/

WORKDIR /app

ENV PYTHONPATH=/app

COPY pyproject.toml uv.lock /app/

RUN uv sync

COPY email-ingestion-service/ .
COPY common/ /app/common/

EXPOSE 8000

ENTRYPOINT ["uv", "run"]

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]