FROM python:3.11-slim
COPY --from=ghcr.io/astral-sh/uv:0.9 /uv /bin/uv
WORKDIR /app
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev
COPY gateway ./gateway
COPY ui ./ui
EXPOSE 8000
HEALTHCHECK --interval=15s --timeout=3s --retries=3 \
  CMD ["/app/.venv/bin/python", "-c", "import urllib.request;urllib.request.urlopen('http://127.0.0.1:8000/healthz',timeout=2)"]
# one worker only: agent registry lives in process memory
CMD ["/app/.venv/bin/uvicorn", "gateway.main:app", "--host", "0.0.0.0", "--port", "8000", "--proxy-headers", "--forwarded-allow-ips", "*"]
