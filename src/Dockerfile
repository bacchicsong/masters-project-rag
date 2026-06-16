FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt /app

# Устанавливаем torch без CUDA (CPU-only) — это ~200MB вместо ~1.5GB
# Важно: делаем это ДО установки sentence-transformers, чтобы он не скачал cuda-версию
RUN pip install torch --index-url https://download.pytorch.org/whl/cpu --no-cache-dir

# Остальные зависимости (sentence-transformers не будет переустанавливать torch)
RUN pip install -r requirements.txt --no-cache-dir

COPY . /app

EXPOSE 8088

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8088", "--no-access-log"]
