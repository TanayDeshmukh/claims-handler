FROM python:3.12-slim
LABEL authors="tanay"

COPY --from=ghcr.io/astral-sh/uv:0.7.13 /uv /uvx /bin/

WORKDIR /app

COPY pyproject.toml uv.lock /app/

RUN uv sync

COPY cost-positions-extraction-worker/ .
COPY common/ /app/common/

ENTRYPOINT ["uv", "run"]

CMD ["python", "worker.py"]