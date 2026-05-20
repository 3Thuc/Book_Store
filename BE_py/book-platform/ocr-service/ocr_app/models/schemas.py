"""
schemas.py – Pydantic data models cho OCR Service.

Tại sao dùng Pydantic?
- FastAPI tự động validate request/response theo schema
- Tự generate OpenAPI docs (Swagger UI tại /docs)
- Nếu response thiếu field → lỗi rõ ràng thay vì trả về JSON lộn xộn
"""
from pydantic import BaseModel, Field
from typing import Optional, List


# ══════════════════════════════════════════════════════════════════
# SHARED MODELS
# ══════════════════════════════════════════════════════════════════

class BookInfo(BaseModel):
    """
    Thông tin sách được trích xuất từ OCR text.
    Dùng trong cả 3 endpoint (search-by-cover, extract-book-info, scan-receipt).
    """
    title: Optional[str] = Field(
        default=None,
        description="Tiêu đề sách – dòng đầu tiên dài nhất không phải số"
    )
    authors: List[str] = Field(
        default_factory=list,
        description="Danh sách tên tác giả (max 3)"
    )
    isbn: Optional[str] = Field(
        default=None,
        description="ISBN-10 hoặc ISBN-13 nếu tìm thấy trên ảnh"
    )
    publisher: Optional[str] = Field(
        default=None,
        description="Tên nhà xuất bản"
    )
    confidence: float = Field(
        default=0.0,
        ge=0.0, le=1.0,
        description="Độ tin cậy trung bình của OCR engine (0.0 – 1.0)"
    )


class BookResult(BaseModel):
    """
    Một cuốn sách trong kết quả tìm kiếm từ Search Service (:8000).
    """
    book_id:      int            = Field(description="ID sách trong database BookStore")
    title:        str            = Field(description="Tiêu đề sách")
    author_name:  Optional[str]  = Field(default=None)
    price:        Optional[float] = Field(default=None, description="Giá (VNĐ)")
    image_url:    Optional[str]  = Field(default=None, description="URL ảnh bìa")
    score:        Optional[float] = Field(default=None, description="Search relevance score")
    avg_rating:   Optional[float] = Field(default=0.0, description="Điểm đánh giá trung bình (0–5)")
    rating_count: Optional[int]  = Field(default=0, description="Số lượt đánh giá")
    categories:   Optional[list] = Field(default=None, description="Danh sách thể loại")
    stock_quantity: Optional[int] = Field(default=None, description="Số lượng tồn kho")


# ══════════════════════════════════════════════════════════════════
# ENDPOINT RESPONSE MODELS
# ══════════════════════════════════════════════════════════════════

class OCRResponse(BaseModel):
    """
    Response cho 2 endpoint:
    - POST /api/ocr/search-by-cover
    - POST /api/ocr/extract-book-info
    """
    success: bool = Field(description="True nếu OCR thành công")

    processing_time_ms: int = Field(
        description="Tổng thời gian xử lý tính bằng milliseconds"
    )
    extracted_text: str = Field(
        description="Toàn bộ text thô OCR đọc được từ ảnh"
    )
    book_info: BookInfo = Field(
        description="Thông tin sách đã được NLP phân tích từ extracted_text"
    )
    search_results: List[BookResult] = Field(
        default_factory=list,
        description="Danh sách sách tìm được trong hệ thống (search-by-cover)"
    )
    total_results: int = Field(
        default=0,
        description="Tổng số kết quả tìm kiếm"
    )
    engine_used: Optional[str] = Field(
        default=None,
        description="OCR engine đã dùng: 'easyocr' hoặc 'tesseract'"
    )
    match_method: Optional[str] = Field(
        default=None,
        description="Cách tìm thấy kết quả: 'visual_match' | 'ocr_search' | 'not_found'"
    )
    image_quality: Optional[str] = Field(
        default=None,
        description="Chất lượng ảnh: 'good' | 'low' | 'irrelevant'"
    )
    error: Optional[str] = Field(
        default=None,
        description="Thông báo lỗi nếu success=False"
    )


class ReceiptItem(BaseModel):
    """Một dòng sách trong hóa đơn được scan."""
    title:    str            = Field(description="Tên sách")
    quantity: Optional[int]  = Field(default=1, description="Số lượng")
    price:    Optional[float] = Field(default=None, description="Đơn giá (VNĐ)")


class ReceiptResponse(BaseModel):
    """
    Response cho endpoint: POST /api/ocr/scan-receipt
    """
    success:            bool             = Field(description="True nếu scan thành công")
    processing_time_ms: int              = Field(description="Thời gian xử lý (ms)")
    extracted_text:     str              = Field(description="Text thô từ ảnh hóa đơn")
    items:              List[ReceiptItem] = Field(
        default_factory=list,
        description="Danh sách sách tìm thấy trong hóa đơn"
    )
    total_amount: Optional[float] = Field(
        default=None,
        description="Tổng tiền tính được từ hóa đơn (nếu có)"
    )
    error: Optional[str] = Field(default=None)


# ══════════════════════════════════════════════════════════════════
# HEALTH CHECK RESPONSE
# ══════════════════════════════════════════════════════════════════

class HealthResponse(BaseModel):
    """Response cho GET /api/ocr/health"""
    status:        str  = Field(description="'ok' nếu service đang chạy bình thường")
    service:       str  = Field(default="ocr-service")
    version:       str  = Field(default="2.0.0")
    port:          int  = Field(default=8005)
    easyocr_ready: bool = Field(
        description="True nếu EasyOCR model đã được load vào RAM"
    )
    visual_index: Optional[dict] = Field(
        default=None,
        description="Thống kê visual hash index (indexed_books, age, etc.)"
    )
