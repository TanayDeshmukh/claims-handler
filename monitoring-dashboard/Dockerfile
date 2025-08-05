FROM python:3.12-slim
LABEL authors="tanay"

COPY --from=ghcr.io/astral-sh/uv:0.7.13 /uv /uvx /bin/

WORKDIR /app

COPY pyproject.toml uv.lock /app/

RUN uv sync

COPY monitoring-dashboard/ .

EXPOSE 8080

ENTRYPOINT ["uv", "run"]

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080"]