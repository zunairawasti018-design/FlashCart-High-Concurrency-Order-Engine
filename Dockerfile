FROM python:3.12-slim AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app
COPY requirements.txt .
RUN python -m pip install --no-cache-dir -r requirements.txt
RUN groupadd --system flashcart && \
    useradd --system --gid flashcart --home-dir /app flashcart && \
    chown flashcart:flashcart /app
USER flashcart

FROM base AS test
COPY app ./app
COPY tests ./tests
COPY scripts ./scripts
COPY static ./static
CMD ["python", "-m", "unittest", "discover", "-s", "tests", "-v"]

FROM base AS runtime
COPY app ./app
COPY static ./static

EXPOSE 8000
CMD ["python", "-m", "uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
