/**
 * Python Backend API Service
 * Gọi các API từ Python backend (port 8000)
 */

import { pythonApiRequest } from '../lib/python-api-client';
import { PYTHON_API_ENDPOINTS } from '../lib/python-constants';

// ==========================================
// RECOMMENDATION SERVICE
// ==========================================

export interface BookRecommendation {
  book_id: number;
  title: string;
  author_name?: string;
  price?: number;
  image_url?: string;
  score?: number;
  reason?: string;
}

export class PythonRecommendService {
  // ── Home recommendations ──────────────────────────────────────

  /** Sách bán chạy – dùng ở trang chủ (fallback khi chưa đăng nhập) */
  static getPopular(limit: number = 20) {
    return pythonApiRequest.get<BookRecommendation[]>(PYTHON_API_ENDPOINTS.RECOMMEND.POPULAR, {
      params: { limit }
    });
  }

  /** Sách trending (view/add_to_cart gần đây) – dùng trang chủ */
  static getTrending(days: number = 7, limit: number = 10) {
    return pythonApiRequest.get<BookRecommendation[]>(PYTHON_API_ENDPOINTS.RECOMMEND.TRENDING, {
      params: { days, limit }
    });
  }

  /** Top đánh giá cao – dùng trang chủ */
  static getTopRated(limit: number = 10) {
    return pythonApiRequest.get<BookRecommendation[]>(PYTHON_API_ENDPOINTS.RECOMMEND.TOP_RATED, {
      params: { limit }
    });
  }

  // ── Item-based (trang chi tiết sách) ─────────────────────────

  /**
   * Sách tương tự ngữ nghĩa AI (SBERT k-NN + fallback popular).
   * Endpoint: GET /recommend/book/{id}/cb-fallback
   * Dùng ở: BookDetailPage – section "Sách tương tự"
   */
  static getSimilarBooksCB(bookId: number, limit: number = 10) {
    return pythonApiRequest.get<BookRecommendation[]>(PYTHON_API_ENDPOINTS.RECOMMEND.BOOK_CB_FALLBACK(bookId), {
      params: { limit }
    });
  }

  /**
   * Khách hàng mua sách này cũng mua (co-purchase từ đơn hàng thực tế).
   * Endpoint: GET /recommend/book/{id}/also
   * Dùng ở: BookDetailPage – section "Khách hàng cũng mua"
   */
  static getAlsoBought(bookId: number, limit: number = 10) {
    return pythonApiRequest.get<BookRecommendation[]>(PYTHON_API_ENDPOINTS.RECOMMEND.BOOK_ALSO_BOUGHT(bookId), {
      params: { limit }
    });
  }

  // ── Personalized (cá nhân hoá) ────────────────────────────────

  /**
   * Gợi ý "Dành riêng cho bạn" = CF cache + Rule fallback + Popular fallback.
   * Endpoint: GET /recommend/user/{id}/for-you
   * Dùng ở: App.tsx – HomePage section "Dành riêng cho bạn"
   */
  static getForYou(userId: number, limit: number = 20) {
    return pythonApiRequest.get<BookRecommendation[]>(PYTHON_API_ENDPOINTS.RECOMMEND.USER_FOR_YOU(userId), {
      params: { limit }
    });
  }

  // ── Admin / Maintenance ───────────────────────────────────────

  /** Rebuild CF cache cho 1 user cụ thể (Admin use) */
  static rebuildUserCF(userId: number, days: number = 90, topn: number = 50) {
    return pythonApiRequest.post(PYTHON_API_ENDPOINTS.RECOMMEND.USER_CF_REBUILD(userId), null, {
      params: { days, topn }
    });
  }

  /** Xóa cache CB cho 1 sách (Admin use) */
  static clearBookCBCache(bookId: number) {
    return pythonApiRequest.post(PYTHON_API_ENDPOINTS.RECOMMEND.BOOK_CB_CLEAR_CACHE(bookId));
  }
}

// ==========================================
// SEARCH SERVICE
// ==========================================

export interface SearchResult {
  book_id: number;
  title: string;
  author_name?: string;
  price?: number;
  image_url?: string;
  description?: string;
  rating?: number;
  category_name?: string;
  language?: string;
  format?: string;
  in_stock?: boolean;
  _score?: number;
}

export interface SearchResponse {
  results: SearchResult[];
  items: SearchResult[];
  total: number;
  page: number;
  limit: number;
  took_ms: number;
}

export class PythonSearchService {
  static suggest(query: string, limit: number = 10) {
    return pythonApiRequest.get<{ title: string; author_name?: string }[]>(PYTHON_API_ENDPOINTS.SEARCH.SUGGEST, {
      params: { q: query, limit }
    });
  }

  static search(params: {
    q: string;
    page?: number;
    limit?: number;
    in_stock?: boolean;
    category?: string;
    language?: string;
    fmt?: string;
    sort?: 'relevance' | 'price_asc' | 'price_desc' | 'newest' | 'rating_desc';
  }) {
    return pythonApiRequest.get<SearchResponse>(PYTHON_API_ENDPOINTS.SEARCH.SEARCH, { params });
  }

  // Admin
  static indexBook(bookId: number) {
    return pythonApiRequest.post(PYTHON_API_ENDPOINTS.ADMIN.INDEX_BOOK(bookId));
  }
}

export default { PythonRecommendService, PythonSearchService };
