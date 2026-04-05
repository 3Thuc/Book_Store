###############################################################################
#  Dockerfile – FastAPI BE_py (recommend + search + chatbot)
#  Build context: source-code/BE_py/book-platform/
###############################################################################

FROM python:3.11-slim

# Thư mục làm việc trong container
WORKDIR /app

# Cài system dependencies (cần cho underthesea, OpenCV...)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    libgomp1 \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements trước để tận dụng Docker layer cache
COPY requirements.txt .

# Cài Python dependencies
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

# Copy toàn bộ source code
COPY . .

# Expose port FastAPI
EXPOSE 8000

# Chạy FastAPI với uvicorn (hot reload cho dev)
CMD ["uvicorn", "platform_api.main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]
