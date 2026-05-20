###############################################################################
#  Dockerfile – Chatbot Service (FastAPI port 8004)
#  Build context: source-code/BE_py/book-platform/chatbot-service/
#
#  Chứa:  NLU (SBERT + regex) + Ollama client + OpenSearch agent
#  Port:  8004  (khác với platform_api ở 8000)
###############################################################################

FROM python:3.11-slim

# Thư mục làm việc trong container
WORKDIR /app

# System dependencies (cần cho underthesea, sentence-transformers, torch)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    libgomp1 \
    curl \
    git \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements trước để tận dụng Docker layer cache
COPY requirements.txt .

# Cài Python dependencies
# torch CPU-only để tiết kiệm dung lượng image (SBERT dùng CPU là đủ)
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir torch==2.2.0 --index-url https://download.pytorch.org/whl/cpu \
    && pip install --no-cache-dir -r requirements.txt

# Copy toàn bộ source code chatbot-service
COPY . .

# Expose port chatbot FastAPI
EXPOSE 8004

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD curl -f http://localhost:8004/api/chat/health || exit 1

# Chạy chatbot service (stable mode - không hot-reload)
CMD ["uvicorn", "chatbot_app.main:app", "--host", "0.0.0.0", "--port", "8004", "--timeout-graceful-shutdown", "30"]
