FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    WORKER_ATTACH_TO_API=true \
    WEB_CONCURRENCY=2

WORKDIR /app

# OCR/runtime dependencies for scanned PDFs.
RUN apt-get update && apt-get install -y --no-install-recommends \
    poppler-utils \
    tesseract-ocr \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY src ./src

EXPOSE 3000

CMD ["sh", "-c", "exec uvicorn src.main:app --host 0.0.0.0 --port 3000 --workers ${WEB_CONCURRENCY:-2}"]
