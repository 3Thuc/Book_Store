/**
 * ocrService.ts – OCR Service API Client
 * =======================================
 * Gọi OCR Service tại port 8005.
 * Dùng multipart/form-data (không phải JSON) vì gửi file ảnh.
 *
 * 3 methods chính:
 *   - searchByCover(file)      → Tìm sách qua ảnh bìa
 *   - extractBookInfo(file)    → Trích xuất metadata sách (Admin auto-fill)
 *   - scanReceipt(file)        → Scan hóa đơn
 */

import axios from 'axios';

// Gateway nginx public (Cloudflare) – route /api/ocr/* → ocr service.
// Local dev cũ: 'http://127.0.0.1:8005'
export const OCR_BASE_URL = 'https://book101.datateam.space';

// Timeout 60s: OCR nặng, CPU có thể mất 5-8s cho ảnh phức tạp
const OCR_TIMEOUT = 60_000;

const ocrClient = axios.create({
  baseURL: OCR_BASE_URL,
  timeout: OCR_TIMEOUT,
});

// ══════════════════════════════════════════════════════════════════
// INTERFACES – Phản ánh Pydantic schemas từ OCR Service
// ══════════════════════════════════════════════════════════════════

export interface OcrBookInfo {
  title: string | null;
  authors: string[];
  isbn: string | null;
  publisher: string | null;
  confidence: number;  // 0.0 – 1.0
}

export interface OcrBookResult {
  book_id: number;
  title: string;
  author_name: string | null;
  price: number | null;
  image_url: string | null;
  score: number | null;
}

export interface OcrResponse {
  success: boolean;
  processing_time_ms: number;
  extracted_text: string;
  book_info: OcrBookInfo;
  search_results: OcrBookResult[];
  total_results: number;
  engine_used: string | null;
  error: string | null;
}

export interface OcrReceiptItem {
  title: string;
  quantity: number | null;
  price: number | null;
  matched_books: OcrBookResult[];
}

export interface OcrReceiptResponse {
  success: boolean;
  processing_time_ms: number;
  extracted_text: string;
  items: OcrReceiptItem[];
  total_amount: number | null;
  error: string | null;
}

export interface OcrBookReviewResponse {
  success: boolean;
  summary: string;
  reasons: string[];
  processing_time_ms: number;
  error: string | null;
}


export interface OcrHealthResponse {
  status: string;
  service: string;
  version: string;
  port: number;
  easyocr_ready: boolean;
}

// ══════════════════════════════════════════════════════════════════
// HELPER – Build FormData
// ══════════════════════════════════════════════════════════════════

function buildFormData(file: File): FormData {
  const formData = new FormData();
  formData.append('file', file);
  return formData;
}

// ══════════════════════════════════════════════════════════════════
// OCR SERVICE CLASS
// ══════════════════════════════════════════════════════════════════

export class OcrService {
  /**
   * Kiểm tra OCR Service đang chạy và EasyOCR đã load chưa.
   * Dùng để hiển thị badge "OCR sẵn sàng" hoặc cảnh báo nếu offline.
   */
  static async checkHealth(): Promise<OcrHealthResponse> {
    const resp = await ocrClient.get<OcrHealthResponse>('/api/ocr/health');
    return resp.data;
  }

  /**
   * Tìm sách qua ảnh bìa.
   * Pipeline: Preprocess → OCR → NLP → Search :8000 → Response
   *
   * @param file  File ảnh (JPEG/PNG/WebP, tối đa 10MB)
   * @returns     OcrResponse với search_results chứa danh sách sách tìm thấy
   *
   * Dùng ở: SearchResultsPage (nút 📷 cạnh search bar)
   */
  static async searchByCover(file: File): Promise<OcrResponse> {
    const resp = await ocrClient.post<OcrResponse>(
      '/api/ocr/search-by-cover',
      buildFormData(file),
      { headers: { 'Content-Type': 'multipart/form-data' } }
    );
    return resp.data;
  }

  /**
   * Trích xuất metadata sách từ ảnh bìa/mặt sau (dùng cho Admin).
   * Chạy 2 mode preprocessing song song và chọn kết quả tốt hơn.
   *
   * @param file  File ảnh bìa hoặc mặt sau sách
   * @returns     OcrResponse với book_info.{title, authors, isbn, publisher}
   *
   * Dùng ở: BookManagement form nhập sách mới (Admin)
   */
  static async extractBookInfo(file: File): Promise<OcrResponse> {
    const resp = await ocrClient.post<OcrResponse>(
      '/api/ocr/extract-book-info',
      buildFormData(file),
      { headers: { 'Content-Type': 'multipart/form-data' } }
    );
    return resp.data;
  }

  /**
   * Scan hóa đơn sách.
   * OCR → Phân tích dòng → Tách tên sách / số lượng / giá.
   *
   * @param file  Ảnh hóa đơn (in máy hoặc viết tay)
   * @returns     OcrReceiptResponse với items[] và total_amount
   *
   * Dùng ở: Modal "Mua lại từ hóa đơn" (future feature)
   */
  static async scanReceipt(file: File): Promise<OcrReceiptResponse> {
    const resp = await ocrClient.post<OcrReceiptResponse>(
      '/api/ocr/scan-receipt',
      buildFormData(file),
      { headers: { 'Content-Type': 'multipart/form-data' } }
    );
    return resp.data;
  }

  /**
   * Tạo tóm tắt và 3 lý do nên đọc sách qua Gemma 4.
   *
   * @param title   Tiêu đề sách
   * @param author  Tác giả (tùy chọn)
   * @returns       OcrBookReviewResponse
   */
  static async getBookReview(title: string, author?: string): Promise<OcrBookReviewResponse> {
    const resp = await ocrClient.post<OcrBookReviewResponse>(
      '/api/ocr/book-review',
      { title, author }
    );
    return resp.data;
  }
}

export default OcrService;
